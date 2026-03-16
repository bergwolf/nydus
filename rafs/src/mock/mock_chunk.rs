// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2020 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use nydus_utils::digest::RafsDigest;
use storage::device::v5::BlobV5ChunkInfo;
use storage::device::{BlobChunkFlags, BlobChunkInfo};

/// Cached information about an Rafs Data Chunk.
#[derive(Clone, Default, Debug)]
pub struct MockChunkInfo {
    // block hash
    c_block_id: Arc<RafsDigest>,
    // blob containing the block
    c_blob_index: u32,
    // chunk index in blob
    c_index: u32,
    // position of the block within the file
    c_file_offset: u64,
    // offset of the block within the blob
    c_compress_offset: u64,
    c_decompress_offset: u64,
    // size of the block, compressed
    c_compr_size: u32,
    c_decompress_size: u32,
    c_flags: BlobChunkFlags,
    crc32: u32,
}

impl MockChunkInfo {
    pub fn mock(
        file_offset: u64,
        compress_offset: u64,
        compress_size: u32,
        decompress_offset: u64,
        decompress_size: u32,
    ) -> Self {
        MockChunkInfo {
            c_file_offset: file_offset,
            c_compress_offset: compress_offset,
            c_compr_size: compress_size,
            c_decompress_offset: decompress_offset,
            c_decompress_size: decompress_size,
            ..Default::default()
        }
    }
}

impl BlobChunkInfo for MockChunkInfo {
    fn chunk_id(&self) -> &RafsDigest {
        &self.c_block_id
    }

    fn id(&self) -> u32 {
        self.c_index
    }

    fn is_batch(&self) -> bool {
        false
    }

    fn is_compressed(&self) -> bool {
        self.c_flags.contains(BlobChunkFlags::COMPRESSED)
    }

    fn is_encrypted(&self) -> bool {
        false
    }

    fn has_crc32(&self) -> bool {
        self.c_flags.contains(BlobChunkFlags::HAS_CRC32)
    }

    fn crc32(&self) -> u32 {
        if self.has_crc32() {
            self.crc32
        } else {
            0
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    impl_getter!(blob_index, c_blob_index, u32);
    impl_getter!(compressed_offset, c_compress_offset, u64);
    impl_getter!(compressed_size, c_compr_size, u32);
    impl_getter!(uncompressed_offset, c_decompress_offset, u64);
    impl_getter!(uncompressed_size, c_decompress_size, u32);
}

impl BlobV5ChunkInfo for MockChunkInfo {
    fn index(&self) -> u32 {
        self.c_index
    }

    fn file_offset(&self) -> u64 {
        self.c_file_offset
    }

    fn flags(&self) -> BlobChunkFlags {
        self.c_flags
    }

    fn as_base(&self) -> &dyn BlobChunkInfo {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nydus_utils::digest::{Algorithm, RafsDigest};
    use storage::device::{v5::BlobV5ChunkInfo, BlobChunkFlags, BlobChunkInfo};

    use super::MockChunkInfo;

    #[test]
    fn test_mock_chunk_info() {
        let mut info = MockChunkInfo::mock(0, 1024, 512, 2048, 512);
        let digest = RafsDigest::from_buf("foobar".as_bytes(), Algorithm::Blake3);
        info.c_block_id = Arc::new(digest);
        info.c_blob_index = 1;
        info.c_flags = BlobChunkFlags::COMPRESSED;
        info.c_index = 2;

        let base = info.as_base();
        let any = info.as_any();
        let rev = any.downcast_ref::<MockChunkInfo>().unwrap();

        assert_eq!(info.chunk_id().data, digest.data);
        assert_eq!(info.id(), 2);
        assert_eq!(base.id(), rev.id());
        assert!(info.is_compressed());
        assert!(!info.is_encrypted());
        assert_eq!(info.blob_index(), 1);
        assert_eq!(info.flags(), BlobChunkFlags::COMPRESSED);
        assert_eq!(info.compressed_offset(), 1024);
        assert_eq!(info.compressed_size(), 512);
        assert_eq!(info.compressed_end(), 1024 + 512);

        assert_eq!(info.uncompressed_offset(), 2048);
        assert_eq!(info.uncompressed_size(), 512);
        assert_eq!(info.uncompressed_end(), 2048 + 512);
        assert_eq!(info.file_offset(), 0);
    }

    #[test]
    fn test_mock_chunk_default() {
        let info = MockChunkInfo::default();

        assert_eq!(info.id(), 0);
        assert_eq!(info.blob_index(), 0);
        assert_eq!(info.compressed_offset(), 0);
        assert_eq!(info.compressed_size(), 0);
        assert_eq!(info.uncompressed_offset(), 0);
        assert_eq!(info.uncompressed_size(), 0);
        assert_eq!(info.file_offset(), 0);
        assert_eq!(info.index(), 0);
        assert!(!info.is_compressed());
        assert!(!info.is_encrypted());
        assert!(!info.is_batch());
        assert!(!info.has_crc32());
        assert_eq!(info.crc32(), 0);
        assert_eq!(info.flags(), BlobChunkFlags::empty());
        assert_eq!(info.compressed_end(), 0);
        assert_eq!(info.uncompressed_end(), 0);
    }

    #[test]
    fn test_mock_chunk_mock_constructor_field_values() {
        let info = MockChunkInfo::mock(100, 200, 300, 400, 500);

        assert_eq!(info.file_offset(), 100);
        assert_eq!(info.compressed_offset(), 200);
        assert_eq!(info.compressed_size(), 300);
        assert_eq!(info.uncompressed_offset(), 400);
        assert_eq!(info.uncompressed_size(), 500);
        // Fields not set by mock() should be default
        assert_eq!(info.blob_index(), 0);
        assert_eq!(info.id(), 0);
        assert_eq!(info.index(), 0);
    }

    #[test]
    fn test_mock_chunk_zero_sizes() {
        let info = MockChunkInfo::mock(0, 0, 0, 0, 0);

        assert_eq!(info.compressed_offset(), 0);
        assert_eq!(info.compressed_size(), 0);
        assert_eq!(info.compressed_end(), 0);
        assert_eq!(info.uncompressed_offset(), 0);
        assert_eq!(info.uncompressed_size(), 0);
        assert_eq!(info.uncompressed_end(), 0);
        assert_eq!(info.file_offset(), 0);
    }

    #[test]
    fn test_mock_chunk_max_values() {
        let info = MockChunkInfo::mock(u64::MAX, u64::MAX, u32::MAX, u64::MAX, u32::MAX);

        assert_eq!(info.file_offset(), u64::MAX);
        assert_eq!(info.compressed_offset(), u64::MAX);
        assert_eq!(info.compressed_size(), u32::MAX);
        assert_eq!(info.uncompressed_offset(), u64::MAX);
        assert_eq!(info.uncompressed_size(), u32::MAX);
    }

    #[test]
    fn test_mock_chunk_flag_combinations() {
        let mut info = MockChunkInfo::default();

        // No flags set
        assert!(!info.is_compressed());
        assert!(!info.has_crc32());
        assert_eq!(info.crc32(), 0);

        // Only COMPRESSED
        info.c_flags = BlobChunkFlags::COMPRESSED;
        assert!(info.is_compressed());
        assert!(!info.has_crc32());

        // Only HAS_CRC32
        info.c_flags = BlobChunkFlags::HAS_CRC32;
        assert!(!info.is_compressed());
        assert!(info.has_crc32());
        // crc32 value is still 0 in the field
        assert_eq!(info.crc32(), 0);

        // HAS_CRC32 with a value
        info.crc32 = 0xDEADBEEF;
        assert_eq!(info.crc32(), 0xDEADBEEF);

        // Both COMPRESSED and HAS_CRC32
        info.c_flags = BlobChunkFlags::COMPRESSED | BlobChunkFlags::HAS_CRC32;
        assert!(info.is_compressed());
        assert!(info.has_crc32());
        assert_eq!(info.crc32(), 0xDEADBEEF);
    }

    #[test]
    fn test_mock_chunk_crc32_without_flag() {
        let mut info = MockChunkInfo::default();
        info.crc32 = 12345;
        // Without HAS_CRC32 flag, crc32() should return 0
        assert!(!info.has_crc32());
        assert_eq!(info.crc32(), 0);

        // With HAS_CRC32 flag, crc32() should return the value
        info.c_flags = BlobChunkFlags::HAS_CRC32;
        assert_eq!(info.crc32(), 12345);
    }

    #[test]
    fn test_mock_chunk_as_any_downcast() {
        let info = MockChunkInfo::mock(10, 20, 30, 40, 50);
        let any = info.as_any();
        let downcast = any.downcast_ref::<MockChunkInfo>();
        assert!(downcast.is_some());
        let recovered = downcast.unwrap();
        assert_eq!(recovered.file_offset(), 10);
        assert_eq!(recovered.compressed_offset(), 20);
    }

    #[test]
    fn test_mock_chunk_as_base_returns_self() {
        let info = MockChunkInfo::mock(5, 10, 15, 20, 25);
        let base = info.as_base();
        assert_eq!(base.compressed_offset(), 10);
        assert_eq!(base.compressed_size(), 15);
        assert_eq!(base.uncompressed_offset(), 20);
        assert_eq!(base.uncompressed_size(), 25);
    }

    #[test]
    fn test_mock_chunk_clone() {
        let mut info = MockChunkInfo::mock(1, 2, 3, 4, 5);
        info.c_blob_index = 42;
        info.c_index = 7;
        let cloned = info.clone();
        assert_eq!(cloned.blob_index(), 42);
        assert_eq!(cloned.id(), 7);
        assert_eq!(cloned.file_offset(), 1);
        assert_eq!(cloned.compressed_offset(), 2);
        assert_eq!(cloned.compressed_size(), 3);
        assert_eq!(cloned.uncompressed_offset(), 4);
        assert_eq!(cloned.uncompressed_size(), 5);
    }

    #[test]
    fn test_mock_chunk_is_batch_always_false() {
        let mut info = MockChunkInfo::default();
        assert!(!info.is_batch());
        info.c_flags = BlobChunkFlags::COMPRESSED;
        assert!(!info.is_batch());
    }

    #[test]
    fn test_mock_chunk_is_encrypted_always_false() {
        let mut info = MockChunkInfo::default();
        assert!(!info.is_encrypted());
        info.c_flags = BlobChunkFlags::COMPRESSED | BlobChunkFlags::HAS_CRC32;
        assert!(!info.is_encrypted());
    }

    #[test]
    fn test_mock_chunk_digest_default() {
        let info = MockChunkInfo::default();
        let default_digest = RafsDigest::default();
        assert_eq!(info.chunk_id().data, default_digest.data);
    }

    #[test]
    fn test_mock_chunk_blob_v5_index_and_id_consistency() {
        let mut info = MockChunkInfo::default();
        info.c_index = 99;
        // BlobChunkInfo::id() and BlobV5ChunkInfo::index() both use c_index
        assert_eq!(info.id(), 99);
        assert_eq!(info.index(), 99);
    }
}
