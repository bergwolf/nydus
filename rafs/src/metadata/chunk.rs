// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2021-2022 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Context, Result};
use nydus_storage::device::v5::BlobV5ChunkInfo;
use nydus_storage::device::{BlobChunkFlags, BlobChunkInfo};
use nydus_storage::meta::BlobMetaChunk;
use nydus_utils::digest::RafsDigest;

use crate::metadata::cached_v5::CachedChunkInfoV5;
use crate::metadata::direct_v5::DirectChunkInfoV5;
use crate::metadata::direct_v6::{DirectChunkInfoV6, TarfsChunkInfoV6};
use crate::metadata::layout::v5::RafsV5ChunkInfo;
use crate::metadata::{RafsStore, RafsVersion};
use crate::RafsIoWrite;

/// A wrapper to encapsulate different versions of chunk information objects.
#[derive(Clone)]
pub enum ChunkWrapper {
    /// Chunk info for RAFS v5.
    V5(RafsV5ChunkInfo),
    /// Chunk info RAFS v6, reuse `RafsV5ChunkInfo` as IR for v6.
    V6(RafsV5ChunkInfo),
    /// Reference to a `BlobChunkInfo` object.
    Ref(Arc<dyn BlobChunkInfo>),
}

impl Debug for ChunkWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::V5(c) => write!(f, "{:?}", c),
            Self::V6(c) => write!(f, "{:?}", c),
            Self::Ref(c) => {
                let chunk = to_rafs_v5_chunk_info(as_blob_v5_chunk_info(c.deref()));
                write!(f, "{:?}", chunk)
            }
        }
    }
}

impl Display for ChunkWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let base_format = format!(
            "id {}, index {}, blob_index {}, file_offset {}, compressed {}/{}, uncompressed {}/{}",
            self.id(),
            self.index(),
            self.blob_index(),
            self.file_offset(),
            self.compressed_offset(),
            self.compressed_size(),
            self.uncompressed_offset(),
            self.uncompressed_size(),
        );

        let full_format = if self.has_crc32() {
            format!("{}, crc32 {:#x}", base_format, self.crc32())
        } else {
            base_format
        };
        write!(f, "{}", full_format)
    }
}

impl ChunkWrapper {
    /// Create a new `ChunkWrapper` object with default value.
    pub fn new(version: RafsVersion) -> Self {
        match version {
            RafsVersion::V5 => ChunkWrapper::V5(RafsV5ChunkInfo::default()),
            RafsVersion::V6 => ChunkWrapper::V6(RafsV5ChunkInfo::default()),
        }
    }

    /// Create a `ChunkWrapper` object from a `BlobChunkInfo` trait object.
    pub fn from_chunk_info(cki: Arc<dyn BlobChunkInfo>) -> Self {
        Self::Ref(cki)
    }

    /// Get digest of chunk data, which is also used as chunk ID.
    pub fn id(&self) -> &RafsDigest {
        match self {
            ChunkWrapper::V5(c) => &c.block_id,
            ChunkWrapper::V6(c) => &c.block_id,
            ChunkWrapper::Ref(c) => c.chunk_id(),
        }
    }

    /// Set digest of chunk data, which is also used as chunk ID.
    pub fn set_id(&mut self, id: RafsDigest) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.block_id = id,
            ChunkWrapper::V6(c) => c.block_id = id,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get index of the data blob associated with the chunk.
    pub fn blob_index(&self) -> u32 {
        match self {
            ChunkWrapper::V5(c) => c.blob_index,
            ChunkWrapper::V6(c) => c.blob_index,
            ChunkWrapper::Ref(c) => c.blob_index(),
        }
    }

    /// Set index of the data blob associated with the chunk.
    pub fn set_blob_index(&mut self, index: u32) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.blob_index = index,
            ChunkWrapper::V6(c) => c.blob_index = index,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get offset into the compressed data blob to fetch chunk data.
    pub fn compressed_offset(&self) -> u64 {
        match self {
            ChunkWrapper::V5(c) => c.compressed_offset,
            ChunkWrapper::V6(c) => c.compressed_offset,
            ChunkWrapper::Ref(c) => c.compressed_offset(),
        }
    }

    /// Set offset into the compressed data blob to fetch chunk data.
    pub fn set_compressed_offset(&mut self, offset: u64) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.compressed_offset = offset,
            ChunkWrapper::V6(c) => c.compressed_offset = offset,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get size of compressed chunk data.
    pub fn compressed_size(&self) -> u32 {
        match self {
            ChunkWrapper::V5(c) => c.compressed_size,
            ChunkWrapper::V6(c) => c.compressed_size,
            ChunkWrapper::Ref(c) => c.compressed_size(),
        }
    }

    /// Set size of compressed chunk data.
    pub fn set_compressed_size(&mut self, size: u32) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.compressed_size = size,
            ChunkWrapper::V6(c) => c.compressed_size = size,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get offset into the uncompressed data blob file to get chunk data.
    pub fn uncompressed_offset(&self) -> u64 {
        match self {
            ChunkWrapper::V5(c) => c.uncompressed_offset,
            ChunkWrapper::V6(c) => c.uncompressed_offset,
            ChunkWrapper::Ref(c) => c.uncompressed_offset(),
        }
    }

    /// Set offset into the uncompressed data blob file to get chunk data.
    pub fn set_uncompressed_offset(&mut self, offset: u64) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.uncompressed_offset = offset,
            ChunkWrapper::V6(c) => c.uncompressed_offset = offset,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get size of uncompressed chunk data.
    pub fn uncompressed_size(&self) -> u32 {
        match self {
            ChunkWrapper::V5(c) => c.uncompressed_size,
            ChunkWrapper::V6(c) => c.uncompressed_size,
            ChunkWrapper::Ref(c) => c.uncompressed_size(),
        }
    }

    /// Set size of uncompressed chunk data.
    pub fn set_uncompressed_size(&mut self, size: u32) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.uncompressed_size = size,
            ChunkWrapper::V6(c) => c.uncompressed_size = size,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get chunk index into the RAFS chunk information array, used by RAFS v5.
    pub fn index(&self) -> u32 {
        match self {
            ChunkWrapper::V5(c) => c.index,
            ChunkWrapper::V6(c) => c.index,
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref()).index(),
        }
    }

    /// Set chunk index into the RAFS chunk information array, used by RAFS v5.
    pub fn set_index(&mut self, index: u32) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.index = index,
            ChunkWrapper::V6(c) => c.index = index,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get chunk offset in the file it belongs to, RAFS v5.
    pub fn file_offset(&self) -> u64 {
        match self {
            ChunkWrapper::V5(c) => c.file_offset,
            ChunkWrapper::V6(c) => c.file_offset,
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref()).file_offset(),
        }
    }

    /// Set chunk offset in the file it belongs to, RAFS v5.
    pub fn set_file_offset(&mut self, offset: u64) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.file_offset = offset,
            ChunkWrapper::V6(c) => c.file_offset = offset,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Check whether the chunk is compressed or not.
    pub fn is_compressed(&self) -> bool {
        match self {
            ChunkWrapper::V5(c) => c.flags.contains(BlobChunkFlags::COMPRESSED),
            ChunkWrapper::V6(c) => c.flags.contains(BlobChunkFlags::COMPRESSED),
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref())
                .flags()
                .contains(BlobChunkFlags::COMPRESSED),
        }
    }

    /// Set flag for whether chunk is compressed.
    pub fn set_compressed(&mut self, compressed: bool) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.flags.set(BlobChunkFlags::COMPRESSED, compressed),
            ChunkWrapper::V6(c) => c.flags.set(BlobChunkFlags::COMPRESSED, compressed),
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Check whether the chunk is encrypted or not.
    pub fn is_encrypted(&self) -> bool {
        match self {
            ChunkWrapper::V5(c) => c.flags.contains(BlobChunkFlags::ENCRYPTED),
            ChunkWrapper::V6(c) => c.flags.contains(BlobChunkFlags::ENCRYPTED),
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref())
                .flags()
                .contains(BlobChunkFlags::ENCRYPTED),
        }
    }

    /// Set flag for whether chunk is encrypted.
    pub fn set_encrypted(&mut self, encrypted: bool) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.flags.set(BlobChunkFlags::ENCRYPTED, encrypted),
            ChunkWrapper::V6(c) => c.flags.set(BlobChunkFlags::ENCRYPTED, encrypted),
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Set flag for whether chunk is batch chunk.
    pub fn set_batch(&mut self, batch: bool) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.flags.set(BlobChunkFlags::BATCH, batch),
            ChunkWrapper::V6(c) => c.flags.set(BlobChunkFlags::BATCH, batch),
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Check whether the chunk is batch chunk or not.
    pub fn is_batch(&self) -> bool {
        match self {
            ChunkWrapper::V5(c) => c.flags.contains(BlobChunkFlags::BATCH),
            ChunkWrapper::V6(c) => c.flags.contains(BlobChunkFlags::BATCH),
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref())
                .flags()
                .contains(BlobChunkFlags::BATCH),
        }
    }

    /// Set crc32 of chunk data.
    pub fn set_crc32(&mut self, crc32: u32) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.crc32 = crc32,
            ChunkWrapper::V6(c) => c.crc32 = crc32,
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    /// Get crc32 of chunk data.
    pub fn crc32(&self) -> u32 {
        match self {
            ChunkWrapper::V5(c) => c.crc32,
            ChunkWrapper::V6(c) => c.crc32,
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref()).crc32(),
        }
    }

    /// Check whether the chunk has CRC or not.
    pub fn has_crc32(&self) -> bool {
        match self {
            ChunkWrapper::V5(c) => c.flags.contains(BlobChunkFlags::HAS_CRC32),
            ChunkWrapper::V6(c) => c.flags.contains(BlobChunkFlags::HAS_CRC32),
            ChunkWrapper::Ref(c) => as_blob_v5_chunk_info(c.deref())
                .flags()
                .contains(BlobChunkFlags::HAS_CRC32),
        }
    }

    /// Set flag for whether chunk has CRC.
    pub fn set_has_crc32(&mut self, has_crc: bool) {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => c.flags.set(BlobChunkFlags::HAS_CRC32, has_crc),
            ChunkWrapper::V6(c) => c.flags.set(BlobChunkFlags::HAS_CRC32, has_crc),
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Set a group of chunk information fields.
    pub fn set_chunk_info(
        &mut self,
        blob_index: u32,
        chunk_index: u32,
        file_offset: u64,
        uncompressed_offset: u64,
        uncompressed_size: u32,
        compressed_offset: u64,
        compressed_size: u32,
        is_compressed: bool,
        is_encrypted: bool,
        has_crc32: bool,
    ) -> Result<()> {
        self.ensure_owned();
        match self {
            ChunkWrapper::V5(c) => {
                c.index = chunk_index;
                c.blob_index = blob_index;
                c.file_offset = file_offset;
                c.compressed_offset = compressed_offset;
                c.compressed_size = compressed_size;
                c.uncompressed_offset = uncompressed_offset;
                c.uncompressed_size = uncompressed_size;
                if is_compressed {
                    c.flags |= BlobChunkFlags::COMPRESSED;
                }
                if has_crc32 {
                    c.flags |= BlobChunkFlags::HAS_CRC32;
                }
            }
            ChunkWrapper::V6(c) => {
                c.index = chunk_index;
                c.blob_index = blob_index;
                c.file_offset = file_offset;
                c.compressed_offset = compressed_offset;
                c.compressed_size = compressed_size;
                c.uncompressed_offset = uncompressed_offset;
                c.uncompressed_size = uncompressed_size;
                if is_compressed {
                    c.flags |= BlobChunkFlags::COMPRESSED;
                }
                if is_encrypted {
                    c.flags |= BlobChunkFlags::ENCRYPTED;
                }
                if has_crc32 {
                    c.flags |= BlobChunkFlags::HAS_CRC32;
                }
            }
            ChunkWrapper::Ref(_c) => panic!("unexpected"),
        }

        Ok(())
    }

    /// Copy chunk information from another `ChunkWrapper` object.
    pub fn copy_from(&mut self, other: &Self) {
        self.ensure_owned();
        match (self, other) {
            (ChunkWrapper::V5(s), ChunkWrapper::V5(o)) => s.clone_from(o),
            (ChunkWrapper::V6(s), ChunkWrapper::V6(o)) => s.clone_from(o),
            (ChunkWrapper::V5(s), ChunkWrapper::V6(o)) => s.clone_from(o),
            (ChunkWrapper::V6(s), ChunkWrapper::V5(o)) => s.clone_from(o),
            (ChunkWrapper::V5(s), ChunkWrapper::Ref(o)) => {
                s.clone_from(&to_rafs_v5_chunk_info(as_blob_v5_chunk_info(o.deref())))
            }
            (ChunkWrapper::V6(s), ChunkWrapper::Ref(o)) => {
                s.clone_from(&to_rafs_v5_chunk_info(as_blob_v5_chunk_info(o.deref())))
            }
            (ChunkWrapper::Ref(_s), ChunkWrapper::V5(_o)) => panic!("unexpected"),
            (ChunkWrapper::Ref(_s), ChunkWrapper::V6(_o)) => panic!("unexpected"),
            (ChunkWrapper::Ref(_s), ChunkWrapper::Ref(_o)) => panic!("unexpected"),
        }
    }

    /// Store the chunk information object into RAFS metadata blob.
    pub fn store(&self, w: &mut dyn RafsIoWrite) -> Result<usize> {
        match self {
            ChunkWrapper::V5(c) => c.store(w).context("failed to store rafs v5 chunk"),
            ChunkWrapper::V6(c) => c.store(w).context("failed to store rafs v6 chunk"),
            ChunkWrapper::Ref(c) => {
                let chunk = to_rafs_v5_chunk_info(as_blob_v5_chunk_info(c.deref()));
                chunk.store(w).context("failed to store rafs v6 chunk")
            }
        }
    }

    fn ensure_owned(&mut self) {
        if let Self::Ref(cki) = self {
            if let Some(cki_v6) = cki.as_any().downcast_ref::<BlobMetaChunk>() {
                *self = Self::V6(to_rafs_v5_chunk_info(cki_v6));
            } else if let Some(cki_v6) = cki.as_any().downcast_ref::<DirectChunkInfoV6>() {
                *self = Self::V6(to_rafs_v5_chunk_info(cki_v6));
            } else if let Some(cki_v6) = cki.as_any().downcast_ref::<TarfsChunkInfoV6>() {
                *self = Self::V6(to_rafs_v5_chunk_info(cki_v6));
            } else if let Some(cki_v5) = cki.as_any().downcast_ref::<CachedChunkInfoV5>() {
                *self = Self::V5(to_rafs_v5_chunk_info(cki_v5));
            } else if let Some(cki_v5) = cki.as_any().downcast_ref::<DirectChunkInfoV5>() {
                *self = Self::V5(to_rafs_v5_chunk_info(cki_v5));
            } else {
                panic!("unknown chunk information struct");
            }
        }
    }
}

fn as_blob_v5_chunk_info(cki: &dyn BlobChunkInfo) -> &dyn BlobV5ChunkInfo {
    if let Some(cki_v6) = cki.as_any().downcast_ref::<BlobMetaChunk>() {
        cki_v6
    } else if let Some(cki_v6) = cki.as_any().downcast_ref::<DirectChunkInfoV6>() {
        cki_v6
    } else if let Some(cki_v6) = cki.as_any().downcast_ref::<TarfsChunkInfoV6>() {
        cki_v6
    } else if let Some(cki_v5) = cki.as_any().downcast_ref::<CachedChunkInfoV5>() {
        cki_v5
    } else if let Some(cki_v5) = cki.as_any().downcast_ref::<DirectChunkInfoV5>() {
        cki_v5
    } else {
        panic!("unknown chunk information struct");
    }
}

/// Construct a `RafsV5ChunkInfo` object from a `dyn BlobChunkInfo` object.
fn to_rafs_v5_chunk_info(cki: &dyn BlobV5ChunkInfo) -> RafsV5ChunkInfo {
    RafsV5ChunkInfo {
        block_id: *cki.chunk_id(),
        blob_index: cki.blob_index(),
        flags: cki.flags(),
        compressed_size: cki.compressed_size(),
        uncompressed_size: cki.uncompressed_size(),
        compressed_offset: cki.compressed_offset(),
        uncompressed_offset: cki.uncompressed_offset(),
        file_offset: cki.file_offset(),
        index: cki.index(),
        crc32: cki.crc32(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockChunkInfo;
    use nydus_utils::digest;

    fn test_chunk_wrapper(mut wrapper: ChunkWrapper) {
        let dig = RafsDigest::from_buf([0xc; 32].as_slice(), digest::Algorithm::Blake3);
        wrapper.set_id(dig);
        assert_eq!(wrapper.id().to_owned(), dig);
        wrapper.set_blob_index(1024);
        assert_eq!(wrapper.blob_index(), 1024);
        wrapper.set_compressed_offset(1024);
        assert_eq!(wrapper.compressed_offset(), 1024);
        wrapper.set_compressed_size(1024);
        assert_eq!(wrapper.compressed_size(), 1024);
        wrapper.set_uncompressed_offset(1024);
        assert_eq!(wrapper.uncompressed_offset(), 1024);
        wrapper.set_uncompressed_size(1024);
        assert_eq!(wrapper.uncompressed_size(), 1024);
        wrapper.set_index(1024);
        assert_eq!(wrapper.index(), 1024);
        wrapper.set_file_offset(1024);
        assert_eq!(wrapper.file_offset(), 1024);
        wrapper.set_compressed(true);
        assert!(wrapper.is_compressed());
        wrapper.set_batch(true);
        assert!(wrapper.is_batch());
        wrapper
            .set_chunk_info(2048, 2048, 2048, 2048, 2048, 2048, 2048, true, true, true)
            .unwrap();
        assert_eq!(wrapper.blob_index(), 2048);
        assert_eq!(wrapper.compressed_offset(), 2048);
        assert_eq!(wrapper.compressed_size(), 2048);
        assert_eq!(wrapper.uncompressed_offset(), 2048);
        assert_eq!(wrapper.uncompressed_size(), 2048);
        assert_eq!(wrapper.file_offset(), 2048);
        assert!(wrapper.is_compressed());
    }

    #[test]
    fn test_chunk_wrapper_v5() {
        let wrapper = ChunkWrapper::new(RafsVersion::V5);
        test_chunk_wrapper(wrapper);
        let wrapper = ChunkWrapper::Ref(Arc::new(CachedChunkInfoV5::default()));
        test_chunk_wrapper(wrapper);
    }

    #[test]
    fn test_chunk_wrapper_v6() {
        let wrapper = ChunkWrapper::new(RafsVersion::V6);
        test_chunk_wrapper(wrapper);
        let wrapper = ChunkWrapper::Ref(Arc::new(TarfsChunkInfoV6::new(0, 0, 0, 0)));
        test_chunk_wrapper(wrapper);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref() {
        let wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        assert_eq!(wrapper.id().to_owned(), RafsDigest::default());
        assert_eq!(wrapper.blob_index(), 0);
        assert_eq!(wrapper.compressed_offset(), 0);
        assert_eq!(wrapper.compressed_size(), 0);
        assert_eq!(wrapper.uncompressed_offset(), 0);
        assert_eq!(wrapper.uncompressed_size(), 0);
        assert_eq!(wrapper.index(), 0);
        assert_eq!(wrapper.file_offset(), 0);
        assert!(!wrapper.is_compressed());
        assert!(!wrapper.is_batch());
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_id() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        let dig = RafsDigest::from_buf([0xc; 32].as_slice(), digest::Algorithm::Blake3);
        wrapper.set_id(dig);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_blob_index() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_blob_index(1024);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_compressed_offset() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_compressed_offset(2048);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_uncompressed_size() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_uncompressed_size(1024);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_uncompressed_offset() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_uncompressed_offset(1024);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_compressed_size() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_compressed_size(2048);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_index() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_index(2048);
    }
    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_file_offset() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_file_offset(1024);
    }
    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_compressed() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_compressed(true);
    }
    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_batch() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.set_batch(true);
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_set_chunk_info() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper
            .set_chunk_info(2048, 2048, 2048, 2048, 2048, 2048, 2048, true, true, true)
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn test_chunk_wrapper_ref_ensure_owned() {
        let mut wrapper = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        wrapper.ensure_owned();
    }

    fn test_copy_from(mut w1: ChunkWrapper, w2: ChunkWrapper) {
        w1.copy_from(&w2);
        assert_eq!(w1.blob_index(), w2.blob_index());
        assert_eq!(w1.compressed_offset(), w2.compressed_offset());
        assert_eq!(w1.compressed_size(), w2.compressed_size());
        assert_eq!(w1.uncompressed_offset(), w2.uncompressed_offset());
        assert_eq!(w1.uncompressed_size(), w2.uncompressed_size());
    }

    #[test]
    fn test_chunk_wrapper_copy_from() {
        let wrapper_v6 = ChunkWrapper::Ref(Arc::new(TarfsChunkInfoV6::new(0, 1, 128, 256)));
        let wrapper_v5 = ChunkWrapper::Ref(Arc::new(CachedChunkInfoV5::new()));
        test_copy_from(wrapper_v5.clone(), wrapper_v5.clone());
        test_copy_from(wrapper_v5.clone(), wrapper_v6.clone());
        test_copy_from(wrapper_v6.clone(), wrapper_v5);
        test_copy_from(wrapper_v6.clone(), wrapper_v6);
    }

    #[test]
    #[should_panic]
    fn test_ref_copy1() {
        let wrapper_ref = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        test_copy_from(wrapper_ref.clone(), wrapper_ref);
    }

    #[test]
    #[should_panic]
    fn test_ref_copy2() {
        let wrapper_ref = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        let wrapper_v5 = ChunkWrapper::Ref(Arc::new(CachedChunkInfoV5::default()));
        test_copy_from(wrapper_ref, wrapper_v5);
    }

    #[test]
    #[should_panic]
    fn test_ref_copy3() {
        let wrapper_ref = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        let wrapper_v6 = ChunkWrapper::Ref(Arc::new(TarfsChunkInfoV6::new(0, 0, 0, 0)));
        test_copy_from(wrapper_ref, wrapper_v6);
    }

    #[test]
    #[should_panic]
    fn test_ref_copy4() {
        let wrapper_ref = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        let wrapper_v6 = ChunkWrapper::Ref(Arc::new(TarfsChunkInfoV6::new(0, 0, 0, 0)));
        test_copy_from(wrapper_v6, wrapper_ref);
    }

    #[test]
    #[should_panic]
    fn test_ref_copy5() {
        let wrapper_ref = ChunkWrapper::Ref(Arc::new(MockChunkInfo::default()));
        let wrapper_v5 = ChunkWrapper::Ref(Arc::new(CachedChunkInfoV5::default()));
        test_copy_from(wrapper_v5, wrapper_ref);
    }

    #[test]
    fn test_fmt() {
        let wrapper_v5 = ChunkWrapper::Ref(Arc::new(CachedChunkInfoV5::default()));
        assert_eq!(
            format!("{:?}", wrapper_v5),
            "RafsV5ChunkInfo { block_id: RafsDigest { data: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }, blob_index: 0, flags: (empty), compressed_size: 0, uncompressed_size: 0, compressed_offset: 0, uncompressed_offset: 0, file_offset: 0, index: 0, crc32: 0 }"
        );
    }

    #[test]
    fn test_chunk_wrapper_v5_construction_defaults() {
        let wrapper = ChunkWrapper::new(RafsVersion::V5);
        assert_eq!(wrapper.blob_index(), 0);
        assert_eq!(wrapper.compressed_offset(), 0);
        assert_eq!(wrapper.compressed_size(), 0);
        assert_eq!(wrapper.uncompressed_offset(), 0);
        assert_eq!(wrapper.uncompressed_size(), 0);
        assert_eq!(wrapper.index(), 0);
        assert_eq!(wrapper.file_offset(), 0);
        assert!(!wrapper.is_compressed());
        assert!(!wrapper.is_encrypted());
        assert!(!wrapper.is_batch());
        assert!(!wrapper.has_crc32());
        assert_eq!(wrapper.crc32(), 0);
    }

    #[test]
    fn test_chunk_wrapper_v6_construction_defaults() {
        let wrapper = ChunkWrapper::new(RafsVersion::V6);
        assert_eq!(wrapper.blob_index(), 0);
        assert_eq!(wrapper.compressed_offset(), 0);
        assert_eq!(wrapper.compressed_size(), 0);
        assert_eq!(wrapper.uncompressed_offset(), 0);
        assert_eq!(wrapper.uncompressed_size(), 0);
        assert_eq!(wrapper.index(), 0);
        assert_eq!(wrapper.file_offset(), 0);
        assert!(!wrapper.is_compressed());
        assert!(!wrapper.is_encrypted());
        assert!(!wrapper.is_batch());
    }

    #[test]
    fn test_chunk_wrapper_set_encrypted_v6() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        assert!(!wrapper.is_encrypted());
        wrapper.set_encrypted(true);
        assert!(wrapper.is_encrypted());
        wrapper.set_encrypted(false);
        assert!(!wrapper.is_encrypted());
    }

    #[test]
    fn test_chunk_wrapper_set_encrypted_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        assert!(!wrapper.is_encrypted());
        wrapper.set_encrypted(true);
        assert!(wrapper.is_encrypted());
        wrapper.set_encrypted(false);
        assert!(!wrapper.is_encrypted());
    }

    #[test]
    fn test_chunk_wrapper_crc32_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        assert_eq!(wrapper.crc32(), 0);
        assert!(!wrapper.has_crc32());

        wrapper.set_crc32(0x12345678);
        assert_eq!(wrapper.crc32(), 0x12345678);

        wrapper.set_has_crc32(true);
        assert!(wrapper.has_crc32());
        wrapper.set_has_crc32(false);
        assert!(!wrapper.has_crc32());
    }

    #[test]
    fn test_chunk_wrapper_crc32_v6() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        wrapper.set_crc32(0xdeadbeef);
        assert_eq!(wrapper.crc32(), 0xdeadbeef);

        wrapper.set_has_crc32(true);
        assert!(wrapper.has_crc32());
    }

    #[test]
    fn test_chunk_wrapper_set_chunk_info_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        wrapper
            .set_chunk_info(
                1,    // blob_index
                2,    // chunk_index
                100,  // file_offset
                200,  // uncompressed_offset
                300,  // uncompressed_size
                400,  // compressed_offset
                500,  // compressed_size
                true, // is_compressed
                true, // is_encrypted (ignored for V5)
                true, // has_crc32
            )
            .unwrap();

        assert_eq!(wrapper.blob_index(), 1);
        assert_eq!(wrapper.index(), 2);
        assert_eq!(wrapper.file_offset(), 100);
        assert_eq!(wrapper.uncompressed_offset(), 200);
        assert_eq!(wrapper.uncompressed_size(), 300);
        assert_eq!(wrapper.compressed_offset(), 400);
        assert_eq!(wrapper.compressed_size(), 500);
        assert!(wrapper.is_compressed());
        assert!(wrapper.has_crc32());
        // V5 does not set encrypted flag in set_chunk_info
        assert!(!wrapper.is_encrypted());
    }

    #[test]
    fn test_chunk_wrapper_set_chunk_info_v6() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        wrapper
            .set_chunk_info(
                3,     // blob_index
                4,     // chunk_index
                1000,  // file_offset
                2000,  // uncompressed_offset
                3000,  // uncompressed_size
                4000,  // compressed_offset
                5000,  // compressed_size
                true,  // is_compressed
                true,  // is_encrypted
                true,  // has_crc32
            )
            .unwrap();

        assert_eq!(wrapper.blob_index(), 3);
        assert_eq!(wrapper.index(), 4);
        assert_eq!(wrapper.file_offset(), 1000);
        assert_eq!(wrapper.uncompressed_offset(), 2000);
        assert_eq!(wrapper.uncompressed_size(), 3000);
        assert_eq!(wrapper.compressed_offset(), 4000);
        assert_eq!(wrapper.compressed_size(), 5000);
        assert!(wrapper.is_compressed());
        assert!(wrapper.is_encrypted());
        assert!(wrapper.has_crc32());
    }

    #[test]
    fn test_chunk_wrapper_set_chunk_info_no_flags() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        wrapper
            .set_chunk_info(0, 0, 0, 0, 100, 0, 100, false, false, false)
            .unwrap();
        assert!(!wrapper.is_compressed());
        assert!(!wrapper.is_encrypted());
        assert!(!wrapper.has_crc32());
    }

    #[test]
    fn test_chunk_wrapper_copy_from_v5_to_v6() {
        let mut v5 = ChunkWrapper::new(RafsVersion::V5);
        v5.set_blob_index(10);
        v5.set_compressed_offset(0x1000);
        v5.set_compressed_size(0x200);
        v5.set_uncompressed_offset(0x2000);
        v5.set_uncompressed_size(0x400);
        v5.set_index(5);
        v5.set_file_offset(0x5000);

        let mut v6 = ChunkWrapper::new(RafsVersion::V6);
        v6.copy_from(&v5);
        assert_eq!(v6.blob_index(), 10);
        assert_eq!(v6.compressed_offset(), 0x1000);
        assert_eq!(v6.compressed_size(), 0x200);
        assert_eq!(v6.uncompressed_offset(), 0x2000);
        assert_eq!(v6.uncompressed_size(), 0x400);
        assert_eq!(v6.index(), 5);
        assert_eq!(v6.file_offset(), 0x5000);
    }

    #[test]
    fn test_chunk_wrapper_copy_from_v6_to_v5() {
        let mut v6 = ChunkWrapper::new(RafsVersion::V6);
        v6.set_blob_index(20);
        v6.set_compressed_offset(0x3000);
        v6.set_compressed_size(0x100);

        let mut v5 = ChunkWrapper::new(RafsVersion::V5);
        v5.copy_from(&v6);
        assert_eq!(v5.blob_index(), 20);
        assert_eq!(v5.compressed_offset(), 0x3000);
        assert_eq!(v5.compressed_size(), 0x100);
    }

    #[test]
    fn test_chunk_wrapper_display_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        wrapper.set_blob_index(1);
        wrapper.set_index(2);
        wrapper.set_file_offset(0x100);
        wrapper.set_compressed_offset(0x200);
        wrapper.set_compressed_size(0x50);
        wrapper.set_uncompressed_offset(0x300);
        wrapper.set_uncompressed_size(0x80);

        let display = format!("{}", wrapper);
        assert!(display.contains("blob_index 1"));
        assert!(display.contains("index 2"));
        assert!(display.contains("file_offset 256"));
        assert!(display.contains("compressed 512/80"));
        assert!(display.contains("uncompressed 768/128"));
    }

    #[test]
    fn test_chunk_wrapper_display_v6() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        wrapper.set_blob_index(5);
        wrapper.set_compressed_offset(0x1000);
        wrapper.set_compressed_size(0x100);
        wrapper.set_uncompressed_offset(0x2000);
        wrapper.set_uncompressed_size(0x200);

        let display = format!("{}", wrapper);
        assert!(display.contains("blob_index 5"));
        assert!(display.contains("compressed 4096/256"));
        assert!(display.contains("uncompressed 8192/512"));
    }

    #[test]
    fn test_chunk_wrapper_display_with_crc32() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        wrapper.set_has_crc32(true);
        wrapper.set_crc32(0xaabb);
        let display = format!("{}", wrapper);
        assert!(display.contains("crc32 0xaabb"));
    }

    #[test]
    fn test_chunk_wrapper_debug_v5() {
        let wrapper = ChunkWrapper::new(RafsVersion::V5);
        let debug = format!("{:?}", wrapper);
        assert!(debug.contains("RafsV5ChunkInfo"));
    }

    #[test]
    fn test_chunk_wrapper_debug_v6() {
        let wrapper = ChunkWrapper::new(RafsVersion::V6);
        let debug = format!("{:?}", wrapper);
        assert!(debug.contains("RafsV5ChunkInfo"));
    }

    #[test]
    fn test_chunk_wrapper_from_chunk_info() {
        let mock = MockChunkInfo::mock(100, 200, 50, 300, 60);
        let wrapper = ChunkWrapper::from_chunk_info(Arc::new(mock));
        assert_eq!(wrapper.compressed_offset(), 200);
        assert_eq!(wrapper.compressed_size(), 50);
        assert_eq!(wrapper.uncompressed_offset(), 300);
        assert_eq!(wrapper.uncompressed_size(), 60);
    }

    #[test]
    fn test_chunk_wrapper_set_batch_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        assert!(!wrapper.is_batch());
        wrapper.set_batch(true);
        assert!(wrapper.is_batch());
        wrapper.set_batch(false);
        assert!(!wrapper.is_batch());
    }

    #[test]
    fn test_chunk_wrapper_set_batch_v6() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V6);
        assert!(!wrapper.is_batch());
        wrapper.set_batch(true);
        assert!(wrapper.is_batch());
        wrapper.set_batch(false);
        assert!(!wrapper.is_batch());
    }

    #[test]
    fn test_chunk_wrapper_set_compressed_v5() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        assert!(!wrapper.is_compressed());
        wrapper.set_compressed(true);
        assert!(wrapper.is_compressed());
        wrapper.set_compressed(false);
        assert!(!wrapper.is_compressed());
    }

    #[test]
    fn test_chunk_wrapper_id_roundtrip() {
        let mut wrapper = ChunkWrapper::new(RafsVersion::V5);
        let dig = RafsDigest::from_buf([0xab; 32].as_slice(), digest::Algorithm::Sha256);
        wrapper.set_id(dig);
        assert_eq!(*wrapper.id(), dig);
    }
}
