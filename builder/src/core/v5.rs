// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::convert::TryFrom;
use std::mem::size_of;

use anyhow::{bail, Context, Result};
use nydus_rafs::metadata::inode::InodeWrapper;
use nydus_rafs::metadata::layout::v5::{
    RafsV5BlobTable, RafsV5ChunkInfo, RafsV5InodeTable, RafsV5InodeWrapper, RafsV5SuperBlock,
    RafsV5XAttrsTable,
};
use nydus_rafs::metadata::{RafsStore, RafsVersion};
use nydus_rafs::RafsIoWrite;
use nydus_utils::digest::{DigestHasher, RafsDigest};
use nydus_utils::{div_round_up, root_tracer, timing_tracer, try_round_up_4k};

use super::node::Node;
use crate::{Bootstrap, BootstrapContext, BuildContext, Tree};

// Filesystem may have different algorithms to calculate `i_size` for directory entries,
// which may break "repeatable build". To support repeatable build, instead of reuse the value
// provided by the source filesystem, we use our own algorithm to calculate `i_size` for directory
// entries for stable `i_size`.
//
// Rafs v6 already has its own algorithm to calculate `i_size` for directory entries, but we don't
// have directory entries for Rafs v5. So let's generate a pseudo `i_size` for Rafs v5 directory
// inode.
const RAFS_V5_VIRTUAL_ENTRY_SIZE: u64 = 8;

impl Node {
    /// Dump RAFS v5 inode metadata to meta blob.
    pub fn dump_bootstrap_v5(
        &self,
        ctx: &mut BuildContext,
        f_bootstrap: &mut dyn RafsIoWrite,
    ) -> Result<()> {
        trace!("[{}]\t{}", self.overlay, self);
        if let InodeWrapper::V5(raw_inode) = &self.inode {
            // Dump inode info
            let name = self.name();
            let inode = RafsV5InodeWrapper {
                name,
                symlink: self.info.symlink.as_deref(),
                inode: raw_inode,
            };
            inode
                .store(f_bootstrap)
                .context("failed to dump inode to bootstrap")?;

            // Dump inode xattr
            if !self.info.xattrs.is_empty() {
                self.info
                    .xattrs
                    .store_v5(f_bootstrap)
                    .context("failed to dump xattr to bootstrap")?;
                ctx.has_xattr = true;
            }

            // Dump chunk info
            if self.is_reg() && self.inode.child_count() as usize != self.chunks.len() {
                bail!("invalid chunk count {}: {}", self.chunks.len(), self);
            }
            for chunk in &self.chunks {
                chunk
                    .inner
                    .store(f_bootstrap)
                    .context("failed to dump chunk info to bootstrap")?;
                trace!("\t\tchunk: {} compressor {}", chunk, ctx.compressor,);
            }

            Ok(())
        } else {
            bail!("dump_bootstrap_v5() encounters non-v5-inode");
        }
    }

    // Filesystem may have different algorithms to calculate `i_size` for directory entries,
    // which may break "repeatable build". To support repeatable build, instead of reuse the value
    // provided by the source filesystem, we use our own algorithm to calculate `i_size` for
    // directory entries for stable `i_size`.
    //
    // Rafs v6 already has its own algorithm to calculate `i_size` for directory entries, but we
    // don't have directory entries for Rafs v5. So let's generate a pseudo `i_size` for Rafs v5
    // directory inode.
    pub fn v5_set_dir_size(&mut self, fs_version: RafsVersion, children: &[Tree]) {
        if !self.is_dir() || !fs_version.is_v5() {
            return;
        }

        let mut d_size = 0u64;
        for child in children.iter() {
            d_size += child.borrow_mut_node().inode.name_size() as u64 + RAFS_V5_VIRTUAL_ENTRY_SIZE;
        }
        if d_size == 0 {
            self.inode.set_size(4096);
        } else {
            // Safe to unwrap() because we have u32 for child count.
            self.inode.set_size(try_round_up_4k(d_size).unwrap());
        }
        self.v5_set_inode_blocks();
    }

    /// Calculate and set `i_blocks` for inode.
    ///
    /// In order to support repeatable build, we can't reuse `i_blocks` from source filesystems,
    /// so let's calculate it by ourself for stable `i_block`.
    ///
    /// Normal filesystem includes the space occupied by Xattr into the directory size,
    /// let's follow the normal behavior.
    pub fn v5_set_inode_blocks(&mut self) {
        // Set inode blocks for RAFS v5 inode, v6 will calculate it at runtime.
        if let InodeWrapper::V5(_) = self.inode {
            self.inode.set_blocks(div_round_up(
                self.inode.size() + self.info.xattrs.aligned_size_v5() as u64,
                512,
            ));
        }
    }
}

impl Bootstrap {
    /// Calculate inode digest for directory.
    fn v5_digest_node(&self, ctx: &mut BuildContext, tree: &Tree) {
        let mut node = tree.borrow_mut_node();

        // We have set digest for non-directory inode in the previous dump_blob workflow.
        if node.is_dir() {
            let mut inode_hasher = RafsDigest::hasher(ctx.digester);
            for child in tree.children.iter() {
                let child = child.borrow_mut_node();
                inode_hasher.digest_update(child.inode.digest().as_ref());
            }
            node.inode.set_digest(inode_hasher.digest_finalize());
        }
    }

    /// Dump bootstrap and blob file, return (Vec<blob_id>, blob_size)
    pub(crate) fn v5_dump(
        &mut self,
        ctx: &mut BuildContext,
        bootstrap_ctx: &mut BootstrapContext,
        blob_table: &RafsV5BlobTable,
    ) -> Result<()> {
        // Set inode digest, use reverse iteration order to reduce repeated digest calculations.
        self.tree.walk_dfs_post(&mut |t| {
            self.v5_digest_node(ctx, t);
            Ok(())
        })?;

        // Set inode table
        let super_block_size = size_of::<RafsV5SuperBlock>();
        let inode_table_entries = bootstrap_ctx.get_next_ino() as u32 - 1;
        let mut inode_table = RafsV5InodeTable::new(inode_table_entries as usize);
        let inode_table_size = inode_table.size();

        // Set prefetch table
        let (prefetch_table_size, prefetch_table_entries) =
            if let Some(prefetch_table) = ctx.prefetch.get_v5_prefetch_table() {
                (prefetch_table.size(), prefetch_table.len() as u32)
            } else {
                (0, 0u32)
            };

        // Set blob table, use sha256 string (length 64) as blob id if not specified
        let prefetch_table_offset = super_block_size + inode_table_size;
        let blob_table_offset = prefetch_table_offset + prefetch_table_size;
        let blob_table_size = blob_table.size();
        let extended_blob_table_offset = blob_table_offset + blob_table_size;
        let extended_blob_table_size = blob_table.extended.size();
        let extended_blob_table_entries = blob_table.extended.entries();

        // Set super block
        let mut super_block = RafsV5SuperBlock::new();
        let inodes_count = bootstrap_ctx.inode_map.len() as u64;
        super_block.set_inodes_count(inodes_count);
        super_block.set_inode_table_offset(super_block_size as u64);
        super_block.set_inode_table_entries(inode_table_entries);
        super_block.set_blob_table_offset(blob_table_offset as u64);
        super_block.set_blob_table_size(blob_table_size as u32);
        super_block.set_extended_blob_table_offset(extended_blob_table_offset as u64);
        super_block.set_extended_blob_table_entries(u32::try_from(extended_blob_table_entries)?);
        super_block.set_prefetch_table_offset(prefetch_table_offset as u64);
        super_block.set_prefetch_table_entries(prefetch_table_entries);
        super_block.set_compressor(ctx.compressor);
        super_block.set_digester(ctx.digester);
        super_block.set_chunk_size(ctx.chunk_size);
        if ctx.explicit_uidgid {
            super_block.set_explicit_uidgid();
        }

        // Set inodes and chunks
        let mut inode_offset = (super_block_size
            + inode_table_size
            + prefetch_table_size
            + blob_table_size
            + extended_blob_table_size) as u32;

        let mut has_xattr = false;
        self.tree.walk_dfs_pre(&mut |t| {
            let node = t.borrow_mut_node();
            inode_table.set(node.index, inode_offset)?;
            // Add inode size
            inode_offset += node.inode.inode_size() as u32;
            if node.inode.has_xattr() {
                has_xattr = true;
                if !node.info.xattrs.is_empty() {
                    inode_offset += (size_of::<RafsV5XAttrsTable>()
                        + node.info.xattrs.aligned_size_v5())
                        as u32;
                }
            }
            // Add chunks size
            if node.is_reg() {
                inode_offset += node.inode.child_count() * size_of::<RafsV5ChunkInfo>() as u32;
            }
            Ok(())
        })?;
        if has_xattr {
            super_block.set_has_xattr();
        }

        // Dump super block
        super_block
            .store(bootstrap_ctx.writer.as_mut())
            .context("failed to store superblock")?;

        // Dump inode table
        inode_table
            .store(bootstrap_ctx.writer.as_mut())
            .context("failed to store inode table")?;

        // Dump prefetch table
        if let Some(mut prefetch_table) = ctx.prefetch.get_v5_prefetch_table() {
            prefetch_table
                .store(bootstrap_ctx.writer.as_mut())
                .context("failed to store prefetch table")?;
        }

        // Dump blob table
        blob_table
            .store(bootstrap_ctx.writer.as_mut())
            .context("failed to store blob table")?;

        // Dump extended blob table
        blob_table
            .store_extended(bootstrap_ctx.writer.as_mut())
            .context("failed to store extended blob table")?;

        // Dump inodes and chunks
        timing_tracer!(
            {
                self.tree.walk_dfs_pre(&mut |t| {
                    t.borrow_mut_node()
                        .dump_bootstrap_v5(ctx, bootstrap_ctx.writer.as_mut())
                        .context("failed to dump bootstrap")
                })
            },
            "dump_bootstrap"
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::node::{Node, NodeInfo};
    use nydus_rafs::metadata::inode::InodeWrapper;
    use nydus_rafs::metadata::layout::v5::RafsV5Inode;
    use nydus_rafs::metadata::layout::RafsXAttrs;
    use std::ffi::OsString;
    use std::path::PathBuf;

    fn make_v5_file_node(name: &str, size: u64) -> Node {
        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFREG as u32 | 0o644);
        inode.set_name_size(name.len());
        inode.set_size(size);
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
            xattrs: RafsXAttrs::default(),
            v6_force_extended_inode: false,
        };
        Node::new(inode, info, 0)
    }

    fn make_v5_dir_node(name: &str) -> Node {
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
            xattrs: RafsXAttrs::default(),
            v6_force_extended_inode: false,
        };
        Node::new(inode, info, 0)
    }

    #[test]
    fn test_v5_set_inode_blocks_regular_file() {
        let mut node = make_v5_file_node("test.txt", 4096);
        node.v5_set_inode_blocks();
        // blocks = div_round_up(size + xattr_size, 512) = div_round_up(4096 + 0, 512) = 8
        assert_eq!(node.inode.blocks(), 8);
    }

    #[test]
    fn test_v5_set_inode_blocks_small_file() {
        let mut node = make_v5_file_node("small.txt", 100);
        node.v5_set_inode_blocks();
        // blocks = div_round_up(100, 512) = 1
        assert_eq!(node.inode.blocks(), 1);
    }

    #[test]
    fn test_v5_set_inode_blocks_zero_size() {
        let mut node = make_v5_file_node("empty.txt", 0);
        node.v5_set_inode_blocks();
        // blocks = div_round_up(0, 512) = 0
        assert_eq!(node.inode.blocks(), 0);
    }

    #[test]
    fn test_v5_set_inode_blocks_exact_block_boundary() {
        let mut node = make_v5_file_node("exact.txt", 512);
        node.v5_set_inode_blocks();
        // blocks = div_round_up(512, 512) = 1
        assert_eq!(node.inode.blocks(), 1);
    }

    #[test]
    fn test_v5_set_inode_blocks_just_over_boundary() {
        let mut node = make_v5_file_node("over.txt", 513);
        node.v5_set_inode_blocks();
        // blocks = div_round_up(513, 512) = 2
        assert_eq!(node.inode.blocks(), 2);
    }

    #[test]
    fn test_v5_set_inode_blocks_v6_inode_noop() {
        // v5_set_inode_blocks should be a no-op for V6 inodes
        let mut inode = InodeWrapper::new(RafsVersion::V6);
        inode.set_mode(libc::S_IFREG as u32 | 0o644);
        inode.set_size(4096);
        let info = NodeInfo::default();
        let mut node = Node::new(inode, info, 0);
        let blocks_before = node.inode.blocks();
        node.v5_set_inode_blocks();
        assert_eq!(node.inode.blocks(), blocks_before);
    }

    #[test]
    fn test_v5_set_dir_size_non_dir() {
        // v5_set_dir_size should be a no-op for non-directory nodes
        let mut node = make_v5_file_node("file.txt", 100);
        let original_size = node.inode.size();
        node.v5_set_dir_size(RafsVersion::V5, &[]);
        assert_eq!(node.inode.size(), original_size);
    }

    #[test]
    fn test_v5_set_dir_size_v6_noop() {
        // v5_set_dir_size should be a no-op for V6 version
        let mut node = make_v5_dir_node("dir");
        let original_size = node.inode.size();
        node.v5_set_dir_size(RafsVersion::V6, &[]);
        assert_eq!(node.inode.size(), original_size);
    }

    #[test]
    fn test_v5_set_dir_size_empty_dir() {
        let mut node = make_v5_dir_node("emptydir");
        node.v5_set_dir_size(RafsVersion::V5, &[]);
        // Empty directory should get size 4096
        assert_eq!(node.inode.size(), 4096);
    }

    #[test]
    fn test_v5_set_dir_size_with_children() {
        let mut dir_node = make_v5_dir_node("parent");
        let child1 = make_v5_file_node("a.txt", 100);
        let child2 = make_v5_file_node("b.txt", 200);
        let children = vec![Tree::new(child1), Tree::new(child2)];

        dir_node.v5_set_dir_size(RafsVersion::V5, &children);

        // d_size = sum of (name_size + RAFS_V5_VIRTUAL_ENTRY_SIZE) for each child
        // child1: name_size("a.txt") = 5, + 8 = 13
        // child2: name_size("b.txt") = 5, + 8 = 13
        // total = 26, rounded up to 4k = 4096
        assert_eq!(dir_node.inode.size(), 4096);
        // blocks should also be set
        assert!(dir_node.inode.blocks() > 0);
    }

    #[test]
    fn test_v5_set_dir_size_rounds_up() {
        let mut dir_node = make_v5_dir_node("bigdir");

        // Create enough children so d_size > 4096
        let mut children = Vec::new();
        for i in 0..500 {
            let name = format!("file_{:04}.txt", i);
            children.push(Tree::new(make_v5_file_node(&name, 100)));
        }

        dir_node.v5_set_dir_size(RafsVersion::V5, &children);
        // Size should be rounded up to 4k boundary
        assert!(dir_node.inode.size() >= 4096);
        assert_eq!(dir_node.inode.size() % 4096, 0);
    }

    #[test]
    fn test_v5_set_inode_blocks_with_xattrs() {
        let mut inode = InodeWrapper::V5(RafsV5Inode::default());
        inode.set_mode(libc::S_IFREG as u32 | 0o644);
        inode.set_size(100);
        let mut xattrs = RafsXAttrs::new();
        xattrs.add("user.test".into(), b"value".to_vec()).unwrap();
        let info = NodeInfo {
            xattrs,
            ..NodeInfo::default()
        };
        let mut node = Node::new(inode, info, 0);
        node.v5_set_inode_blocks();
        // blocks should account for xattr size
        // blocks = div_round_up(100 + xattr_aligned_size, 512)
        assert!(node.inode.blocks() >= 1);
    }
}
