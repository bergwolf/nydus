// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2023 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Error, Result};
use nydus_utils::digest::{self, RafsDigest};
use std::ops::Deref;

use nydus_rafs::metadata::layout::{RafsBlobTable, RAFS_V5_ROOT_INODE};
use nydus_rafs::metadata::{RafsSuper, RafsSuperConfig, RafsSuperFlags};

use crate::{ArtifactStorage, BlobManager, BootstrapContext, BootstrapManager, BuildContext, Tree};

/// RAFS bootstrap/meta builder.
pub struct Bootstrap {
    pub(crate) tree: Tree,
}

impl Bootstrap {
    /// Create a new instance of [Bootstrap].
    pub fn new(tree: Tree) -> Result<Self> {
        Ok(Self { tree })
    }

    /// Build the final view of the RAFS filesystem meta from the hierarchy `tree`.
    pub fn build(
        &mut self,
        ctx: &mut BuildContext,
        bootstrap_ctx: &mut BootstrapContext,
    ) -> Result<()> {
        // Special handling of the root inode
        let mut root_node = self.tree.borrow_mut_node();
        assert!(root_node.is_dir());
        let index = bootstrap_ctx.generate_next_ino();
        // 0 is reserved and 1 also matches RAFS_V5_ROOT_INODE.
        assert_eq!(index, RAFS_V5_ROOT_INODE);
        root_node.index = index;
        root_node.inode.set_ino(index);
        ctx.prefetch.insert(&self.tree.node, root_node.deref());
        bootstrap_ctx.inode_map.insert(
            (
                root_node.layer_idx,
                root_node.info.src_ino,
                root_node.info.src_dev,
            ),
            vec![self.tree.node.clone()],
        );
        drop(root_node);

        Self::build_rafs(ctx, bootstrap_ctx, &mut self.tree)?;
        if ctx.fs_version.is_v6() {
            let root_offset = self.tree.node.borrow().v6_offset;
            Self::v6_update_dirents(&self.tree, root_offset);
        }

        Ok(())
    }

    /// Dump the RAFS filesystem meta information to meta blob.
    pub fn dump(
        &mut self,
        ctx: &mut BuildContext,
        bootstrap_storage: &mut Option<ArtifactStorage>,
        bootstrap_ctx: &mut BootstrapContext,
        blob_table: &RafsBlobTable,
    ) -> Result<()> {
        match blob_table {
            RafsBlobTable::V5(table) => self.v5_dump(ctx, bootstrap_ctx, table)?,
            RafsBlobTable::V6(table) => self.v6_dump(ctx, bootstrap_ctx, table)?,
        }

        if let Some(ArtifactStorage::FileDir(p)) = bootstrap_storage {
            let bootstrap_data = bootstrap_ctx.writer.as_bytes()?;
            let digest = RafsDigest::from_buf(&bootstrap_data, digest::Algorithm::Sha256);
            let name = digest.to_string();
            bootstrap_ctx.writer.finalize(Some(name.clone()))?;
            let mut path = p.0.join(name);
            path.set_extension(&p.1);
            *bootstrap_storage = Some(ArtifactStorage::SingleFile(path));
            Ok(())
        } else {
            bootstrap_ctx.writer.finalize(Some(String::default()))
        }
    }

    /// Traverse node tree, set inode index, ino, child_index and child_count etc according to the
    /// RAFS metadata format, then store to nodes collection.
    fn build_rafs(
        ctx: &mut BuildContext,
        bootstrap_ctx: &mut BootstrapContext,
        tree: &mut Tree,
    ) -> Result<()> {
        let parent_node = tree.node.clone();
        let mut parent_node = parent_node.borrow_mut();
        let parent_ino = parent_node.inode.ino();
        let block_size = ctx.v6_block_size();

        // In case of multi-layer building, it's possible that the parent node is not a directory.
        if parent_node.is_dir() {
            parent_node
                .inode
                .set_child_count(tree.children.len() as u32);
            if ctx.fs_version.is_v5() {
                parent_node
                    .inode
                    .set_child_index(bootstrap_ctx.get_next_ino() as u32);
            } else if ctx.fs_version.is_v6() {
                // Layout directory entries for v6.
                let d_size = parent_node.v6_dirent_size(ctx, tree)?;
                parent_node.v6_set_dir_offset(bootstrap_ctx, d_size, block_size)?;
            }
        }

        let mut dirs: Vec<&mut Tree> = Vec::new();
        for child in tree.children.iter_mut() {
            let child_node = child.node.clone();
            let mut child_node = child_node.borrow_mut();
            let index = bootstrap_ctx.generate_next_ino();
            child_node.index = index;
            if ctx.fs_version.is_v5() {
                child_node.inode.set_parent(parent_ino);
            }

            // Handle hardlink.
            // All hardlink nodes' ino and nlink should be the same.
            // We need to find hardlink node index list in the layer where the node is located
            // because the real_ino may be different among different layers,
            let mut v6_hardlink_offset: Option<u64> = None;
            let key = (
                child_node.layer_idx,
                child_node.info.src_ino,
                child_node.info.src_dev,
            );
            if let Some(indexes) = bootstrap_ctx.inode_map.get_mut(&key) {
                let nlink = indexes.len() as u32 + 1;
                // Update nlink for previous hardlink inodes
                for n in indexes.iter() {
                    n.borrow_mut().inode.set_nlink(nlink);
                }

                let (first_ino, first_offset) = {
                    let first_node = indexes[0].borrow_mut();
                    (first_node.inode.ino(), first_node.v6_offset)
                };
                // set offset for rafs v6 hardlinks
                v6_hardlink_offset = Some(first_offset);
                child_node.inode.set_nlink(nlink);
                child_node.inode.set_ino(first_ino);
                indexes.push(child.node.clone());
            } else {
                child_node.inode.set_ino(index);
                child_node.inode.set_nlink(1);
                // Store inode real ino
                bootstrap_ctx
                    .inode_map
                    .insert(key, vec![child.node.clone()]);
            }

            // update bootstrap_ctx.offset for rafs v6 non-dir nodes.
            if !child_node.is_dir() && ctx.fs_version.is_v6() {
                child_node.v6_set_offset(bootstrap_ctx, v6_hardlink_offset, block_size)?;
            }
            ctx.prefetch.insert(&child.node, child_node.deref());
            if child_node.is_dir() {
                dirs.push(child);
            }
        }

        // According to filesystem semantics, a parent directory should have nlink equal to
        // the number of its child directories plus 2.
        if parent_node.is_dir() {
            parent_node.inode.set_nlink((2 + dirs.len()) as u32);
        }
        for dir in dirs {
            Self::build_rafs(ctx, bootstrap_ctx, dir)?;
        }

        Ok(())
    }

    /// Load a parent RAFS bootstrap and return the `Tree` object representing the filesystem.
    pub fn load_parent_bootstrap(
        ctx: &mut BuildContext,
        bootstrap_mgr: &mut BootstrapManager,
        blob_mgr: &mut BlobManager,
    ) -> Result<Tree> {
        let rs = if let Some(path) = bootstrap_mgr.f_parent_path.as_ref() {
            RafsSuper::load_from_file(path, ctx.configuration.clone(), false).map(|(rs, _)| rs)?
        } else {
            return Err(Error::msg("bootstrap context's parent bootstrap is null"));
        };

        let config = RafsSuperConfig {
            compressor: ctx.compressor,
            digester: ctx.digester,
            chunk_size: ctx.chunk_size,
            batch_size: ctx.batch_size,
            explicit_uidgid: ctx.explicit_uidgid,
            version: ctx.fs_version,
            is_tarfs_mode: rs.meta.flags.contains(RafsSuperFlags::TARTFS_MODE),
        };
        config.check_compatibility(&rs.meta)?;

        // Reuse lower layer blob table,
        // we need to append the blob entry of upper layer to the table
        blob_mgr.extend_from_blob_table(ctx, rs.superblock.get_blob_infos())?;

        // Build node tree of lower layer from a bootstrap file, and add chunks
        // of lower node to layered_chunk_dict for chunk deduplication on next.
        Tree::from_bootstrap(&rs, &mut blob_mgr.layered_chunk_dict)
            .context("failed to build tree from bootstrap")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::node::{Node, NodeInfo};
    use nydus_rafs::metadata::inode::InodeWrapper;
    use nydus_rafs::metadata::layout::v5::RafsV5Inode;
    use std::ffi::OsString;
    use std::path::PathBuf;

    fn make_dir_node(name: &str) -> Node {
        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFDIR as u32 | 0o755);
        inode.set_name_size(name.len());
        let info = NodeInfo {
            explicit_uidgid: true,
            src_dev: 0,
            src_ino: 0,
            rdev: 0,
            source: PathBuf::from("/"),
            path: PathBuf::from("/").join(name),
            target: PathBuf::from("/").join(name),
            target_vec: vec![OsString::from("/"), OsString::from(name)],
            symlink: None,
            xattrs: Default::default(),
            v6_force_extended_inode: false,
        };
        Node::new(inode, info, 0)
    }

    fn make_file_node(name: &str, src_ino: u64) -> Node {
        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFREG as u32 | 0o644);
        inode.set_name_size(name.len());
        inode.set_size(1024);
        let info = NodeInfo {
            explicit_uidgid: true,
            src_dev: 0,
            src_ino,
            rdev: 0,
            source: PathBuf::from("/"),
            path: PathBuf::from("/").join(name),
            target: PathBuf::from("/").join(name),
            target_vec: vec![OsString::from("/"), OsString::from(name)],
            symlink: None,
            xattrs: Default::default(),
            v6_force_extended_inode: false,
        };
        Node::new(inode, info, 0)
    }

    fn make_root_node() -> Node {
        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFDIR as u32 | 0o755);
        inode.set_name_size("/".len());
        let info = NodeInfo {
            explicit_uidgid: true,
            src_dev: 0,
            src_ino: 0,
            rdev: 0,
            source: PathBuf::from("/"),
            path: PathBuf::from("/"),
            target: PathBuf::from("/"),
            target_vec: vec![OsString::from("/")],
            symlink: None,
            xattrs: Default::default(),
            v6_force_extended_inode: false,
        };
        Node::new(inode, info, 0)
    }

    #[test]
    fn test_bootstrap_new() {
        let root = make_root_node();
        let tree = Tree::new(root);
        let bootstrap = Bootstrap::new(tree);
        assert!(bootstrap.is_ok());
        let bootstrap = bootstrap.unwrap();
        assert!(bootstrap.tree.children.is_empty());
    }

    #[test]
    fn test_bootstrap_new_with_children() {
        let root = make_root_node();
        let mut tree = Tree::new(root);

        let child1 = make_file_node("file1.txt", 100);
        let child2 = make_dir_node("subdir");
        tree.children.push(Tree::new(child1));
        tree.children.push(Tree::new(child2));

        let bootstrap = Bootstrap::new(tree).unwrap();
        assert_eq!(bootstrap.tree.children.len(), 2);
    }

    #[test]
    fn test_bootstrap_tree_structure_preserved() {
        let root = make_root_node();
        let mut tree = Tree::new(root);

        let child = make_file_node("test.txt", 42);
        tree.children.push(Tree::new(child));

        let bootstrap = Bootstrap::new(tree).unwrap();
        let child_node = bootstrap.tree.children[0].borrow_mut_node();
        assert!(child_node.is_reg());
        assert_eq!(child_node.inode.size(), 1024);
    }

    #[test]
    fn test_bootstrap_root_is_dir() {
        let root = make_root_node();
        let tree = Tree::new(root);
        let bootstrap = Bootstrap::new(tree).unwrap();
        assert!(bootstrap.tree.borrow_mut_node().is_dir());
    }

    #[test]
    fn test_bootstrap_nested_tree() {
        let root = make_root_node();
        let mut tree = Tree::new(root);

        let subdir = make_dir_node("subdir");
        let mut subdir_tree = Tree::new(subdir);

        let file = make_file_node("nested.txt", 200);
        subdir_tree.children.push(Tree::new(file));

        tree.children.push(subdir_tree);

        let bootstrap = Bootstrap::new(tree).unwrap();
        assert_eq!(bootstrap.tree.children.len(), 1);
        assert_eq!(bootstrap.tree.children[0].children.len(), 1);
        assert!(bootstrap.tree.children[0].children[0]
            .borrow_mut_node()
            .is_reg());
    }

    #[test]
    fn test_bootstrap_load_parent_no_parent_path() {
        let mut ctx = BuildContext::default();
        let mut bootstrap_mgr = BootstrapManager::new(None, None);
        let mut blob_mgr = BlobManager::new(nydus_utils::digest::Algorithm::Sha256, false);

        let result =
            Bootstrap::load_parent_bootstrap(&mut ctx, &mut bootstrap_mgr, &mut blob_mgr);
        assert!(result.is_err());
        let err_msg = format!("{}", result.err().unwrap());
        assert!(err_msg.contains("parent bootstrap is null"));
    }

    #[test]
    fn test_bootstrap_load_parent_invalid_path() {
        let mut ctx = BuildContext::default();
        let mut bootstrap_mgr =
            BootstrapManager::new(None, Some("/nonexistent/path/bootstrap".into()));
        let mut blob_mgr = BlobManager::new(nydus_utils::digest::Algorithm::Sha256, false);

        let result =
            Bootstrap::load_parent_bootstrap(&mut ctx, &mut bootstrap_mgr, &mut blob_mgr);
        assert!(result.is_err());
    }
}
