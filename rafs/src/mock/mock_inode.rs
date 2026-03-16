// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2020 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::Result;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;

use fuse_backend_rs::abi::fuse_abi;
use fuse_backend_rs::api::filesystem::Entry;
use nydus_storage::device::v5::BlobV5ChunkInfo;
use nydus_storage::device::{BlobChunkInfo, BlobDevice, BlobInfo, BlobIoVec};
use nydus_utils::{digest::RafsDigest, ByteSize};

use super::mock_chunk::MockChunkInfo;
use super::mock_super::CHUNK_SIZE;
use crate::metadata::inode::RafsInodeFlags;
use crate::metadata::layout::v5::{
    rafsv5_alloc_bio_vecs, RafsV5BlobTable, RafsV5InodeChunkOps, RafsV5InodeOps,
};
use crate::metadata::{
    layout::{XattrName, XattrValue},
    Inode, RafsInode, RafsInodeWalkHandler, RafsSuperMeta, RAFS_ATTR_BLOCK_SIZE,
};
use crate::RafsInodeExt;

#[derive(Default, Clone, Debug)]
#[allow(unused)]
pub struct MockInode {
    i_ino: Inode,
    i_name: OsString,
    i_digest: RafsDigest,
    i_parent: u64,
    i_mode: u32,
    i_projid: u32,
    i_uid: u32,
    i_gid: u32,
    i_flags: RafsInodeFlags,
    i_size: u64,
    i_blocks: u64,
    i_nlink: u32,
    i_child_idx: u32,
    i_child_cnt: u32,
    // extra info need cache
    i_blksize: u32,
    i_rdev: u32,
    i_mtime_nsec: u32,
    i_mtime: u64,
    i_target: OsString, // for symbol link
    i_xattr: HashMap<OsString, Vec<u8>>,
    i_data: Vec<Arc<MockChunkInfo>>,
    i_child: Vec<Arc<MockInode>>,
    i_blob_table: Arc<RafsV5BlobTable>,
    i_meta: Arc<RafsSuperMeta>,
}

impl MockInode {
    pub fn mock(ino: Inode, size: u64, chunks: Vec<Arc<MockChunkInfo>>) -> Self {
        Self {
            i_ino: ino,
            i_size: size,
            i_child_cnt: chunks.len() as u32,
            i_data: chunks,
            // Ignore other bits for now.
            i_mode: libc::S_IFREG as u32,
            // It can't be changed yet.
            i_blksize: CHUNK_SIZE,
            ..Default::default()
        }
    }
}

impl RafsInode for MockInode {
    fn validate(&self, _max_inode: Inode, _chunk_size: u64) -> Result<()> {
        if self.is_symlink() && self.i_target.is_empty() {
            return Err(einval!("invalid inode"));
        }
        Ok(())
    }

    #[inline]
    fn get_entry(&self) -> Entry {
        Entry {
            attr: self.get_attr().into(),
            inode: self.i_ino,
            generation: 0,
            attr_flags: 0,
            attr_timeout: self.i_meta.attr_timeout,
            entry_timeout: self.i_meta.entry_timeout,
        }
    }

    #[inline]
    fn get_attr(&self) -> fuse_abi::Attr {
        fuse_abi::Attr {
            ino: self.i_ino,
            size: self.i_size,
            blocks: self.i_blocks,
            mode: self.i_mode,
            nlink: self.i_nlink as u32,
            blksize: RAFS_ATTR_BLOCK_SIZE,
            rdev: self.i_rdev,
            ..Default::default()
        }
    }

    fn walk_children_inodes(
        &self,
        _entry_offset: u64,
        _handler: RafsInodeWalkHandler,
    ) -> Result<()> {
        todo!()
    }

    fn get_symlink(&self) -> Result<OsString> {
        if !self.is_symlink() {
            Err(einval!("inode is not a symlink"))
        } else {
            Ok(self.i_target.clone())
        }
    }

    fn get_symlink_size(&self) -> u16 {
        if self.is_symlink() {
            self.i_target.byte_size() as u16
        } else {
            0
        }
    }

    fn get_child_by_name(&self, name: &OsStr) -> Result<Arc<dyn RafsInodeExt>> {
        let idx = self
            .i_child
            .binary_search_by(|c| c.i_name.as_os_str().cmp(name))
            .map_err(|_| enoent!())?;
        Ok(self.i_child[idx].clone())
    }

    #[inline]
    fn get_child_by_index(&self, index: u32) -> Result<Arc<dyn RafsInodeExt>> {
        Ok(self.i_child[index as usize].clone())
    }

    #[inline]
    fn get_child_count(&self) -> u32 {
        self.i_child_cnt
    }

    fn get_child_index(&self) -> Result<u32> {
        Ok(self.i_child_idx)
    }

    fn get_chunk_count(&self) -> u32 {
        self.get_child_count()
    }

    fn has_xattr(&self) -> bool {
        self.i_flags.contains(RafsInodeFlags::XATTR)
    }

    #[inline]
    fn get_xattr(&self, name: &OsStr) -> Result<Option<XattrValue>> {
        Ok(self.i_xattr.get(name).cloned())
    }

    fn get_xattrs(&self) -> Result<Vec<XattrName>> {
        Ok(self
            .i_xattr
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect::<Vec<XattrName>>())
    }

    #[inline]
    fn is_blkdev(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFBLK as u32
    }

    #[inline]
    fn is_chrdev(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFCHR as u32
    }

    #[inline]
    fn is_sock(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFSOCK as u32
    }

    #[inline]
    fn is_fifo(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFIFO as u32
    }

    fn is_dir(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32
    }

    fn is_symlink(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFLNK as u32
    }

    fn is_reg(&self) -> bool {
        self.i_mode & libc::S_IFMT as u32 == libc::S_IFREG as u32
    }

    fn is_hardlink(&self) -> bool {
        !self.is_dir() && self.i_nlink > 1
    }

    fn collect_descendants_inodes(
        &self,
        descendants: &mut Vec<Arc<dyn RafsInode>>,
    ) -> Result<usize> {
        if !self.is_dir() {
            return Err(enotdir!());
        }

        let mut child_dirs: Vec<Arc<dyn RafsInode>> = Vec::new();

        for child_inode in &self.i_child {
            if child_inode.is_dir() {
                trace!("Got dir {:?}", child_inode.name());
                child_dirs.push(child_inode.clone());
            } else {
                if child_inode.is_empty_size() {
                    continue;
                }
                descendants.push(child_inode.clone());
            }
        }

        for d in child_dirs {
            d.collect_descendants_inodes(descendants)?;
        }

        Ok(0)
    }

    fn alloc_bio_vecs(
        &self,
        _device: &BlobDevice,
        offset: u64,
        size: usize,
        user_io: bool,
    ) -> Result<Vec<BlobIoVec>> {
        rafsv5_alloc_bio_vecs(self, offset, size, user_io)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    impl_getter!(ino, i_ino, u64);
    impl_getter!(size, i_size, u64);
    impl_getter!(rdev, i_rdev, u32);
    impl_getter!(projid, i_projid, u32);
}

impl RafsInodeExt for MockInode {
    fn name(&self) -> OsString {
        self.i_name.clone()
    }

    fn flags(&self) -> u64 {
        self.i_flags.bits()
    }

    fn get_digest(&self) -> RafsDigest {
        self.i_digest
    }

    fn get_name_size(&self) -> u16 {
        self.i_name.byte_size() as u16
    }

    #[inline]
    fn get_chunk_info(&self, idx: u32) -> Result<Arc<dyn BlobChunkInfo>> {
        Ok(self.i_data[idx as usize].clone())
    }

    fn as_inode(&self) -> &dyn RafsInode {
        self
    }

    impl_getter!(parent, i_parent, u64);
}

impl RafsV5InodeChunkOps for MockInode {
    fn get_chunk_info_v5(&self, idx: u32) -> Result<Arc<dyn BlobV5ChunkInfo>> {
        Ok(self.i_data[idx as usize].clone())
    }
}

impl RafsV5InodeOps for MockInode {
    fn get_blob_by_index(&self, _idx: u32) -> Result<Arc<BlobInfo>> {
        Ok(Arc::new(BlobInfo::default()))
    }

    fn get_chunk_size(&self) -> u32 {
        CHUNK_SIZE
    }

    fn has_hole(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use nydus_utils::digest::Algorithm;

    use crate::metadata::layout::RAFS_V5_ROOT_INODE;

    use super::*;

    #[test]
    fn test_mock_node() {
        let size = 20;
        let mut chunks = Vec::<Arc<MockChunkInfo>>::new();
        let digest = RafsDigest::from_buf("foobar".as_bytes(), Algorithm::Blake3);
        let info = MockChunkInfo::mock(0, 1024, 1024, 2048, 1024);

        chunks.push(Arc::new(info.clone()));
        chunks.push(Arc::new(info));

        let mut node = MockInode::mock(13, size, chunks);
        node.i_flags = RafsInodeFlags::XATTR;
        node.i_mode = libc::S_IFDIR as u32;
        node.i_name = "foo".into();
        node.i_digest = digest;
        node.i_parent = RAFS_V5_ROOT_INODE;
        let mut child_node1 = MockInode::mock(14, size, Vec::<Arc<MockChunkInfo>>::new());
        child_node1.i_name = OsStr::new("child1").into();
        child_node1.i_size = 10;
        let mut child_node2 = MockInode::mock(15, size, Vec::<Arc<MockChunkInfo>>::new());
        child_node2.i_name = OsStr::new("child2").into();
        child_node1.i_size = 20;

        node.i_child.push(Arc::new(child_node1));
        node.i_child.push(Arc::new(child_node2));
        node.i_child_cnt = 2;
        node.i_child_idx = 2;

        node.i_xattr.insert("attr1".into(), "bar1".into());
        node.i_xattr.insert("attr2".into(), "bar2".into());
        node.i_xattr.insert("attr3".into(), "bar3".into());

        node.i_data.push(Arc::new(MockChunkInfo::default()));

        assert!(node.validate(0, 0).is_ok());
        assert_eq!(node.ino(), 13);
        assert_eq!(node.size(), 20);
        assert_eq!(node.rdev(), 0);
        assert_eq!(node.projid(), 0);
        assert_eq!(node.name(), "foo");
        assert_eq!(node.flags(), RafsInodeFlags::XATTR.bits());
        assert_eq!(node.get_digest(), digest);
        assert_eq!(node.get_name_size(), "foo".len() as u16);
        assert!(node.get_chunk_info(0).is_ok());
        assert!(node.get_chunk_info_v5(0).is_ok());
        assert_eq!(node.parent(), RAFS_V5_ROOT_INODE);
        assert!(node.get_blob_by_index(0).is_ok());
        assert_eq!(node.get_chunk_size(), CHUNK_SIZE);
        assert!(!node.has_hole());

        let ent = node.get_entry();
        assert_eq!(ent.inode, node.ino());
        assert_eq!(ent.attr_timeout, node.i_meta.attr_timeout);
        assert_eq!(ent.entry_timeout, node.i_meta.entry_timeout);
        assert_eq!(ent.attr, node.get_attr().into());

        assert!(node.get_symlink().is_err());
        assert_eq!(node.get_symlink_size(), 0 as u16);

        assert!(node.get_child_by_name(OsStr::new("child1")).is_ok());
        assert!(node.get_child_by_index(0).is_ok());
        assert!(node.get_child_by_index(1).is_ok());
        assert_eq!(node.get_child_count(), 2 as u32);
        assert_eq!(node.get_child_index().unwrap(), 2 as u32);
        assert_eq!(node.get_chunk_count(), 2 as u32);
        assert!(node.has_xattr());
        assert_eq!(
            node.get_xattr(OsStr::new("attr2")).unwrap().unwrap(),
            "bar2".as_bytes()
        );
        assert_eq!(node.get_xattrs().unwrap().len(), 3);

        assert!(!node.is_blkdev());
        assert!(!node.is_chrdev());
        assert!(!node.is_sock());
        assert!(!node.is_fifo());
        assert!(node.is_dir());
        assert!(!node.is_symlink());
        assert!(!node.is_reg());
        assert!(!node.is_hardlink());
        let mut inodes = Vec::<Arc<dyn RafsInode>>::new();
        node.collect_descendants_inodes(&mut inodes).unwrap();
        assert_eq!(inodes.len(), 2);
    }

    #[test]
    fn test_mock_inode_default() {
        let node = MockInode::default();
        assert_eq!(node.ino(), 0);
        assert_eq!(node.size(), 0);
        assert_eq!(node.rdev(), 0);
        assert_eq!(node.projid(), 0);
        assert_eq!(node.parent(), 0);
        assert_eq!(node.get_child_count(), 0);
        assert_eq!(node.get_child_index().unwrap(), 0);
        assert_eq!(node.get_chunk_count(), 0);
        assert!(!node.has_xattr());
        assert_eq!(node.flags(), 0);
        assert_eq!(node.name(), "");
        assert_eq!(node.get_name_size(), 0);
    }

    #[test]
    fn test_mock_inode_mock_constructor() {
        let chunk = Arc::new(MockChunkInfo::mock(0, 100, 50, 200, 50));
        let node = MockInode::mock(42, 1000, vec![chunk]);

        assert_eq!(node.ino(), 42);
        assert_eq!(node.size(), 1000);
        assert_eq!(node.get_child_count(), 1);
        assert!(node.is_reg()); // default mode is S_IFREG
        assert!(!node.is_dir());
        assert!(!node.is_symlink());
        assert_eq!(node.i_blksize, CHUNK_SIZE);
    }

    #[test]
    fn test_mock_inode_symlink() {
        let mut node = MockInode::mock(10, 100, vec![]);
        node.i_mode = libc::S_IFLNK as u32;
        node.i_target = OsString::from("/target/path");

        assert!(node.is_symlink());
        assert!(!node.is_reg());
        assert!(!node.is_dir());

        let symlink = node.get_symlink().unwrap();
        assert_eq!(symlink, OsString::from("/target/path"));
        assert_eq!(node.get_symlink_size(), "/target/path".len() as u16);
    }

    #[test]
    fn test_mock_inode_symlink_validate_fail() {
        let mut node = MockInode::mock(10, 100, vec![]);
        node.i_mode = libc::S_IFLNK as u32;
        // Empty target for a symlink should fail validation
        assert!(node.validate(100, 100).is_err());
    }

    #[test]
    fn test_mock_inode_symlink_validate_ok() {
        let mut node = MockInode::mock(10, 100, vec![]);
        node.i_mode = libc::S_IFLNK as u32;
        node.i_target = OsString::from("/some/target");
        assert!(node.validate(100, 100).is_ok());
    }

    #[test]
    fn test_mock_inode_non_symlink_get_symlink_error() {
        let node = MockInode::mock(10, 100, vec![]);
        // Default mode is S_IFREG, not symlink
        assert!(node.get_symlink().is_err());
        assert_eq!(node.get_symlink_size(), 0);
    }

    #[test]
    fn test_mock_inode_file_type_blkdev() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_mode = libc::S_IFBLK as u32;
        assert!(node.is_blkdev());
        assert!(!node.is_chrdev());
        assert!(!node.is_sock());
        assert!(!node.is_fifo());
        assert!(!node.is_dir());
        assert!(!node.is_symlink());
        assert!(!node.is_reg());
    }

    #[test]
    fn test_mock_inode_file_type_chrdev() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_mode = libc::S_IFCHR as u32;
        assert!(!node.is_blkdev());
        assert!(node.is_chrdev());
        assert!(!node.is_sock());
        assert!(!node.is_fifo());
        assert!(!node.is_dir());
        assert!(!node.is_symlink());
        assert!(!node.is_reg());
    }

    #[test]
    fn test_mock_inode_file_type_sock() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_mode = libc::S_IFSOCK as u32;
        assert!(node.is_sock());
        assert!(!node.is_blkdev());
        assert!(!node.is_chrdev());
        assert!(!node.is_fifo());
        assert!(!node.is_dir());
        assert!(!node.is_symlink());
        assert!(!node.is_reg());
    }

    #[test]
    fn test_mock_inode_file_type_fifo() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_mode = libc::S_IFIFO as u32;
        assert!(node.is_fifo());
        assert!(!node.is_blkdev());
        assert!(!node.is_chrdev());
        assert!(!node.is_sock());
        assert!(!node.is_dir());
        assert!(!node.is_symlink());
        assert!(!node.is_reg());
    }

    #[test]
    fn test_mock_inode_hardlink() {
        let mut node = MockInode::mock(1, 100, vec![]);
        // Regular file with nlink <= 1 is not a hardlink
        assert!(!node.is_hardlink());

        // Regular file with nlink > 1 is a hardlink
        node.i_nlink = 2;
        assert!(node.is_hardlink());

        // Directory with nlink > 1 is NOT a hardlink
        node.i_mode = libc::S_IFDIR as u32;
        node.i_nlink = 3;
        assert!(!node.is_hardlink());
    }

    #[test]
    fn test_mock_inode_xattr_not_found() {
        let node = MockInode::mock(1, 100, vec![]);
        // No xattrs set, querying should return None
        assert_eq!(node.get_xattr(OsStr::new("nonexistent")).unwrap(), None);
    }

    #[test]
    fn test_mock_inode_xattr_empty_list() {
        let node = MockInode::mock(1, 100, vec![]);
        assert!(!node.has_xattr());
        let xattrs = node.get_xattrs().unwrap();
        assert!(xattrs.is_empty());
    }

    #[test]
    fn test_mock_inode_get_attr_fields() {
        let mut node = MockInode::mock(42, 4096, vec![]);
        node.i_blocks = 8;
        node.i_nlink = 3;
        node.i_rdev = 99;
        node.i_mode = libc::S_IFREG as u32 | 0o755;

        let attr = node.get_attr();
        assert_eq!(attr.ino, 42);
        assert_eq!(attr.size, 4096);
        assert_eq!(attr.blocks, 8);
        assert_eq!(attr.nlink, 3);
        assert_eq!(attr.rdev, 99);
        assert_eq!(attr.mode, libc::S_IFREG as u32 | 0o755);
    }

    #[test]
    fn test_mock_inode_get_entry_fields() {
        let node = MockInode::mock(7, 512, vec![]);
        let entry = node.get_entry();
        assert_eq!(entry.inode, 7);
        assert_eq!(entry.generation, 0);
        assert_eq!(entry.attr_flags, 0);
    }

    #[test]
    fn test_mock_inode_get_child_by_name_not_found() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_mode = libc::S_IFDIR as u32;
        let mut child = MockInode::mock(2, 0, vec![]);
        child.i_name = OsStr::new("existing").into();
        node.i_child.push(Arc::new(child));

        assert!(node.get_child_by_name(OsStr::new("nonexistent")).is_err());
    }

    #[test]
    fn test_mock_inode_collect_descendants_not_dir() {
        let node = MockInode::mock(1, 100, vec![]);
        // Regular file, not a dir
        let mut descendants = Vec::new();
        assert!(node.collect_descendants_inodes(&mut descendants).is_err());
    }

    #[test]
    fn test_mock_inode_collect_descendants_empty_children_skipped() {
        let mut dir = MockInode::mock(1, 0, vec![]);
        dir.i_mode = libc::S_IFDIR as u32;

        // Add a child with zero size - should be skipped
        let mut child = MockInode::mock(2, 0, vec![]);
        child.i_name = OsStr::new("empty_file").into();
        child.i_size = 0;
        dir.i_child.push(Arc::new(child));

        // Add a child with nonzero size - should be collected
        let mut child2 = MockInode::mock(3, 100, vec![]);
        child2.i_name = OsStr::new("nonempty_file").into();
        dir.i_child.push(Arc::new(child2));

        let mut descendants = Vec::new();
        dir.collect_descendants_inodes(&mut descendants).unwrap();
        assert_eq!(descendants.len(), 1);
        assert_eq!(descendants[0].ino(), 3);
    }

    #[test]
    fn test_mock_inode_collect_descendants_nested_dirs() {
        let mut root = MockInode::mock(1, 0, vec![]);
        root.i_mode = libc::S_IFDIR as u32;

        let mut subdir = MockInode::mock(2, 0, vec![]);
        subdir.i_mode = libc::S_IFDIR as u32;
        subdir.i_name = OsStr::new("subdir").into();

        let mut leaf = MockInode::mock(3, 50, vec![]);
        leaf.i_name = OsStr::new("leaf").into();
        subdir.i_child.push(Arc::new(leaf));

        root.i_child.push(Arc::new(subdir));

        let mut descendants = Vec::new();
        root.collect_descendants_inodes(&mut descendants).unwrap();
        assert_eq!(descendants.len(), 1);
        assert_eq!(descendants[0].ino(), 3);
    }

    #[test]
    fn test_mock_inode_as_any_downcast() {
        let node = MockInode::mock(99, 512, vec![]);
        let any = node.as_any();
        let recovered = any.downcast_ref::<MockInode>().unwrap();
        assert_eq!(recovered.ino(), 99);
        assert_eq!(recovered.size(), 512);
    }

    #[test]
    fn test_mock_inode_as_inode() {
        let node = MockInode::mock(55, 256, vec![]);
        let inode = node.as_inode();
        assert_eq!(inode.ino(), 55);
        assert_eq!(inode.size(), 256);
    }

    #[test]
    fn test_mock_inode_mode_with_permissions() {
        let mut node = MockInode::mock(1, 0, vec![]);
        // Directory with rwxr-xr-x
        node.i_mode = libc::S_IFDIR as u32 | 0o755;
        assert!(node.is_dir());
        assert!(!node.is_reg());
        assert_eq!(node.get_attr().mode, libc::S_IFDIR as u32 | 0o755);
    }

    #[test]
    fn test_mock_inode_is_empty_size() {
        let node = MockInode::mock(1, 0, vec![]);
        assert!(node.is_empty_size());

        let node2 = MockInode::mock(2, 100, vec![]);
        assert!(!node2.is_empty_size());
    }

    #[test]
    fn test_mock_inode_rdev_and_projid() {
        let mut node = MockInode::mock(1, 0, vec![]);
        node.i_rdev = 42;
        node.i_projid = 7;
        assert_eq!(node.rdev(), 42);
        assert_eq!(node.projid(), 7);
    }

    #[test]
    fn test_mock_inode_v5_ops() {
        let node = MockInode::mock(1, 100, vec![]);
        assert!(!node.has_hole());
        assert_eq!(node.get_chunk_size(), CHUNK_SIZE);
        // get_blob_by_index always returns a default BlobInfo
        let blob = node.get_blob_by_index(0).unwrap();
        assert_eq!(blob.blob_id(), "");
    }

    #[test]
    fn test_mock_inode_validate_regular_file() {
        let node = MockInode::mock(1, 100, vec![]);
        // Regular file should validate fine
        assert!(node.validate(100, 100).is_ok());
    }

    #[test]
    fn test_mock_inode_get_chunk_info_v5() {
        let chunk = Arc::new(MockChunkInfo::mock(0, 100, 50, 200, 50));
        let node = MockInode::mock(1, 100, vec![chunk]);
        let chunk_info = node.get_chunk_info_v5(0).unwrap();
        assert_eq!(chunk_info.file_offset(), 0);
        assert_eq!(chunk_info.as_base().compressed_offset(), 100);
    }
}
