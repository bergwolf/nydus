// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::fs::DirEntry;

use anyhow::{anyhow, Context, Result};
use nydus_utils::{event_tracer, lazy_drop, root_tracer, timing_tracer};

use crate::core::context::{Artifact, NoopArtifactWriter};
use crate::core::prefetch;

use super::core::blob::Blob;
use super::core::context::{
    ArtifactWriter, BlobManager, BootstrapManager, BuildContext, BuildOutput,
};
use super::core::node::Node;
use super::{build_bootstrap, dump_bootstrap, finalize_blob, Builder, Overlay, Tree, TreeNode};

struct FilesystemTreeBuilder {}

impl FilesystemTreeBuilder {
    fn new() -> Self {
        Self {}
    }

    #[allow(clippy::only_used_in_recursion)]
    /// Walk directory to build node tree by DFS
    fn load_children(
        &self,
        ctx: &mut BuildContext,
        parent: &TreeNode,
        layer_idx: u16,
    ) -> Result<(Vec<Tree>, Vec<Tree>)> {
        let mut trees = Vec::new();
        let mut external_trees = Vec::new();
        let parent = parent.borrow();
        if !parent.is_dir() {
            return Ok((trees.clone(), external_trees));
        }

        let children = fs::read_dir(parent.path())
            .with_context(|| format!("failed to read dir {:?}", parent.path()))?;
        let children = children.collect::<Result<Vec<DirEntry>, std::io::Error>>()?;

        event_tracer!("load_from_directory", +children.len());
        for child in children {
            let path = child.path();
            let target = Node::generate_target(&path, &ctx.source_path);
            let mut file_size: u64 = 0;
            if ctx.attributes.is_external(&target) {
                if let Some(value) = ctx.attributes.get_value(&target, "file_size") {
                    file_size = value.parse::<u64>().ok().ok_or_else(|| {
                        anyhow!(
                            "failed to parse file_size for external file {}",
                            &target.display()
                        )
                    })?;
                }
            }

            let mut child = Node::from_fs_object(
                ctx.fs_version,
                ctx.source_path.clone(),
                path.clone(),
                Overlay::UpperAddition,
                ctx.chunk_size,
                file_size,
                parent.info.explicit_uidgid,
                true,
            )
            .with_context(|| format!("failed to create node {:?}", path))?;
            child.layer_idx = layer_idx;

            // as per OCI spec, whiteout file should not be present within final image
            // or filesystem, only existed in layers.
            if layer_idx == 0
                && child.whiteout_type(ctx.whiteout_spec).is_some()
                && !child.is_overlayfs_opaque(ctx.whiteout_spec)
            {
                continue;
            }

            let (mut child, mut external_child) = (Tree::new(child.clone()), Tree::new(child));
            let (child_children, external_children) =
                self.load_children(ctx, &child.node, layer_idx)?;
            child.children = child_children;
            external_child.children = external_children;
            child
                .borrow_mut_node()
                .v5_set_dir_size(ctx.fs_version, &child.children);
            external_child
                .borrow_mut_node()
                .v5_set_dir_size(ctx.fs_version, &external_child.children);

            if ctx.attributes.is_external(&target) {
                external_trees.push(external_child);
            } else {
                // TODO: need to implement type=ignore for nydus attributes,
                // let's ignore the tree for workaround.
                trees.push(child.clone());
                if ctx.attributes.is_prefix_external(target) {
                    external_trees.push(external_child);
                }
            };
        }

        trees.sort_unstable_by(|a, b| a.name().cmp(b.name()));
        external_trees.sort_unstable_by(|a, b| a.name().cmp(b.name()));

        Ok((trees, external_trees))
    }
}

#[derive(Default)]
pub struct DirectoryBuilder {}

impl DirectoryBuilder {
    pub fn new() -> Self {
        Self {}
    }

    /// Build node tree from a filesystem directory
    fn build_tree(&mut self, ctx: &mut BuildContext, layer_idx: u16) -> Result<(Tree, Tree)> {
        let node = Node::from_fs_object(
            ctx.fs_version,
            ctx.source_path.clone(),
            ctx.source_path.clone(),
            Overlay::UpperAddition,
            ctx.chunk_size,
            0,
            ctx.explicit_uidgid,
            true,
        )?;
        let mut tree = Tree::new(node.clone());
        let mut external_tree = Tree::new(node);
        let tree_builder = FilesystemTreeBuilder::new();

        let (tree_children, external_tree_children) = timing_tracer!(
            { tree_builder.load_children(ctx, &tree.node, layer_idx) },
            "load_from_directory"
        )?;
        tree.children = tree_children;
        external_tree.children = external_tree_children;
        tree.borrow_mut_node()
            .v5_set_dir_size(ctx.fs_version, &tree.children);
        external_tree
            .borrow_mut_node()
            .v5_set_dir_size(ctx.fs_version, &external_tree.children);

        Ok((tree, external_tree))
    }

    fn one_build(
        &mut self,
        ctx: &mut BuildContext,
        bootstrap_mgr: &mut BootstrapManager,
        blob_mgr: &mut BlobManager,
        blob_writer: &mut Box<dyn Artifact>,
        tree: Tree,
    ) -> Result<BuildOutput> {
        // Build bootstrap
        let mut bootstrap_ctx = bootstrap_mgr.create_ctx()?;
        let mut bootstrap = timing_tracer!(
            { build_bootstrap(ctx, bootstrap_mgr, &mut bootstrap_ctx, blob_mgr, tree) },
            "build_bootstrap"
        )?;

        // Dump blob file
        timing_tracer!(
            { Blob::dump(ctx, blob_mgr, blob_writer.as_mut()) },
            "dump_blob"
        )?;

        // Dump blob meta information
        if let Some((_, blob_ctx)) = blob_mgr.get_current_blob() {
            Blob::dump_meta_data(ctx, blob_ctx, blob_writer.as_mut())?;
        }

        // Dump RAFS meta/bootstrap and finalize the data blob.
        if ctx.blob_inline_meta {
            timing_tracer!(
                {
                    dump_bootstrap(
                        ctx,
                        bootstrap_mgr,
                        &mut bootstrap_ctx,
                        &mut bootstrap,
                        blob_mgr,
                        blob_writer.as_mut(),
                    )
                },
                "dump_bootstrap"
            )?;
            finalize_blob(ctx, blob_mgr, blob_writer.as_mut())?;
        } else {
            finalize_blob(ctx, blob_mgr, blob_writer.as_mut())?;
            timing_tracer!(
                {
                    dump_bootstrap(
                        ctx,
                        bootstrap_mgr,
                        &mut bootstrap_ctx,
                        &mut bootstrap,
                        blob_mgr,
                        blob_writer.as_mut(),
                    )
                },
                "dump_bootstrap"
            )?;
        }

        lazy_drop(bootstrap_ctx);

        BuildOutput::new(blob_mgr, None, &bootstrap_mgr.bootstrap_storage, &None)
    }
}

impl Builder for DirectoryBuilder {
    fn build(
        &mut self,
        ctx: &mut BuildContext,
        bootstrap_mgr: &mut BootstrapManager,
        blob_mgr: &mut BlobManager,
    ) -> Result<BuildOutput> {
        let layer_idx = u16::from(bootstrap_mgr.f_parent_path.is_some());

        // Scan source directory to build upper layer tree.
        let (tree, external_tree) =
            timing_tracer!({ self.build_tree(ctx, layer_idx) }, "build_tree")?;

        // Build for tree
        let mut blob_writer: Box<dyn Artifact> = if let Some(blob_stor) = ctx.blob_storage.clone() {
            Box::new(ArtifactWriter::new(blob_stor)?)
        } else {
            Box::<NoopArtifactWriter>::default()
        };
        let mut output = self.one_build(ctx, bootstrap_mgr, blob_mgr, &mut blob_writer, tree)?;

        // Build for external tree
        ctx.prefetch = prefetch::Prefetch::new(prefetch::PrefetchPolicy::None)?;
        let mut external_blob_mgr = BlobManager::new(ctx.digester, true);
        let mut external_bootstrap_mgr = bootstrap_mgr.clone();
        if let Some(stor) = external_bootstrap_mgr.bootstrap_storage.as_mut() {
            stor.add_suffix("external")
        }

        let mut external_blob_writer: Box<dyn Artifact> =
            if let Some(blob_stor) = ctx.external_blob_storage.clone() {
                Box::new(ArtifactWriter::new(blob_stor)?)
            } else {
                Box::<NoopArtifactWriter>::default()
            };
        let external_output = self.one_build(
            ctx,
            &mut external_bootstrap_mgr,
            &mut external_blob_mgr,
            &mut external_blob_writer,
            external_tree,
        )?;
        output.external_bootstrap_path = external_output.bootstrap_path;
        output.external_blobs = external_output.blobs;

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_builder_new() {
        let builder = DirectoryBuilder::new();
        // Just verifying construction doesn't panic
        let _ = builder;
    }

    #[test]
    fn test_directory_builder_default() {
        let builder = DirectoryBuilder::default();
        let _ = builder;
    }

    #[test]
    fn test_filesystem_tree_builder_new() {
        let builder = FilesystemTreeBuilder::new();
        let _ = builder;
    }

    #[test]
    fn test_layer_idx_no_parent() {
        let bootstrap_mgr = BootstrapManager::new(None, None);
        let layer_idx = u16::from(bootstrap_mgr.f_parent_path.is_some());
        assert_eq!(layer_idx, 0);
    }

    #[test]
    fn test_layer_idx_with_parent() {
        let bootstrap_mgr = BootstrapManager::new(None, Some("/some/parent".into()));
        let layer_idx = u16::from(bootstrap_mgr.f_parent_path.is_some());
        assert_eq!(layer_idx, 1);
    }

    #[test]
    fn test_tree_sorting_by_name() {
        use crate::core::node::{Node, NodeInfo};
        use nydus_rafs::metadata::inode::InodeWrapper;
        use nydus_rafs::metadata::layout::v5::RafsV5Inode;
        use std::ffi::OsString;

        fn make_node(name: &str) -> Node {
            let mut inode = InodeWrapper::V5(RafsV5Inode::default());
            inode.set_mode(libc::S_IFREG as u32 | 0o644);
            inode.set_name_size(name.len());
            let info = NodeInfo {
                target_vec: vec![OsString::from("/"), OsString::from(name)],
                ..NodeInfo::default()
            };
            Node::new(inode, info, 0)
        }

        // Replicate the sorting done in load_children
        let mut trees = vec![
            Tree::new(make_node("zebra")),
            Tree::new(make_node("apple")),
            Tree::new(make_node("mango")),
        ];

        trees.sort_unstable_by(|a, b| a.name().cmp(b.name()));

        assert_eq!(trees[0].name(), b"apple");
        assert_eq!(trees[1].name(), b"mango");
        assert_eq!(trees[2].name(), b"zebra");
    }

    #[test]
    fn test_tree_sorting_empty() {
        let mut trees: Vec<Tree> = Vec::new();
        trees.sort_unstable_by(|a, b| a.name().cmp(b.name()));
        assert!(trees.is_empty());
    }

    #[test]
    fn test_tree_sorting_single_element() {
        use crate::core::node::{Node, NodeInfo};
        use nydus_rafs::metadata::inode::InodeWrapper;
        use nydus_rafs::metadata::layout::v5::RafsV5Inode;
        use std::ffi::OsString;

        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFREG as u32 | 0o644);
        inode.set_name_size("only".len());
        let info = NodeInfo {
            target_vec: vec![OsString::from("/"), OsString::from("only")],
            ..NodeInfo::default()
        };
        let node = Node::new(inode, info, 0);

        let mut trees = vec![Tree::new(node)];
        trees.sort_unstable_by(|a, b| a.name().cmp(b.name()));
        assert_eq!(trees.len(), 1);
        assert_eq!(trees[0].name(), b"only");
    }

    #[test]
    fn test_build_tree_with_nonexistent_source() {
        use std::path::PathBuf;
        let mut ctx = BuildContext::default();
        ctx.source_path = PathBuf::from("/nonexistent_directory_12345");
        let mut builder = DirectoryBuilder::new();
        let result = builder.build_tree(&mut ctx, 0);
        assert!(result.is_err());
    }
}
