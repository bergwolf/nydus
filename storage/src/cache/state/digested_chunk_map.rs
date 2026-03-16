// Copyright 2021 Ant Group. All rights reserved.
// Copyright (C) 2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

//! A chunk state tracking driver for legacy Nydus images without chunk array
//!
//! This module provides a chunk state tracking driver for legacy Rafs images without chunk array,
//! which uses chunk digest as id to track chunk readiness state. The [DigestedChunkMap] is not
//! optimal in case of performance and memory consumption. So it is only used to keep backward
/// compatibility with the old nydus image format.
use std::collections::HashSet;
use std::io::Result;
use std::sync::RwLock;

use nydus_utils::digest::RafsDigest;

use crate::cache::state::{ChunkIndexGetter, ChunkMap};
use crate::device::BlobChunkInfo;

/// An implementation of [ChunkMap](trait.ChunkMap.html) to support chunk state tracking by using
/// `HashSet<RafsDigest>`.
///
/// The `DigestedChunkMap` is an implementation of [ChunkMap] which uses a hash set
/// (HashSet<chunk_digest>) to record whether a chunk has already been cached by the blob cache.
/// The implementation is memory and computation heavy, so it is used only to keep backward
/// compatibility with the previous old nydus bootstrap format. For new clients, please use other
/// alternative implementations.
#[derive(Default)]
pub struct DigestedChunkMap {
    cache: RwLock<HashSet<RafsDigest>>,
}

impl DigestedChunkMap {
    /// Create a new instance of `DigestedChunkMap`.
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashSet::new()),
        }
    }
}

impl ChunkMap for DigestedChunkMap {
    fn is_ready(&self, chunk: &dyn BlobChunkInfo) -> Result<bool> {
        Ok(self.cache.read().unwrap().contains(chunk.chunk_id()))
    }

    fn set_ready_and_clear_pending(&self, chunk: &dyn BlobChunkInfo) -> Result<()> {
        // Do not expect poisoned lock.
        self.cache.write().unwrap().insert(*chunk.chunk_id());
        Ok(())
    }
}

impl ChunkIndexGetter for DigestedChunkMap {
    type Index = RafsDigest;

    fn get_index(chunk: &dyn BlobChunkInfo) -> Self::Index {
        *chunk.chunk_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::MockChunkInfo;

    #[test]
    fn test_new_creates_empty_map() {
        let map = DigestedChunkMap::new();
        let chunk = MockChunkInfo::new();
        assert!(!map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_is_ready_returns_false_for_unknown_chunk() {
        let map = DigestedChunkMap::new();
        let mut chunk = MockChunkInfo::new();
        chunk.block_id = RafsDigest { data: [1u8; 32] };
        assert!(!map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_set_ready_then_is_ready() {
        let map = DigestedChunkMap::new();
        let mut chunk = MockChunkInfo::new();
        chunk.block_id = RafsDigest { data: [1u8; 32] };

        map.set_ready_and_clear_pending(&chunk).unwrap();
        assert!(map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_multiple_chunks_tracked_independently() {
        let map = DigestedChunkMap::new();
        let mut chunk1 = MockChunkInfo::new();
        chunk1.block_id = RafsDigest { data: [1u8; 32] };
        let mut chunk2 = MockChunkInfo::new();
        chunk2.block_id = RafsDigest { data: [2u8; 32] };

        // Mark only chunk1 as ready
        map.set_ready_and_clear_pending(&chunk1).unwrap();

        assert!(map.is_ready(&chunk1).unwrap());
        assert!(!map.is_ready(&chunk2).unwrap());

        // Now mark chunk2 as ready too
        map.set_ready_and_clear_pending(&chunk2).unwrap();
        assert!(map.is_ready(&chunk1).unwrap());
        assert!(map.is_ready(&chunk2).unwrap());
    }

    #[test]
    fn test_get_index_returns_chunk_digest() {
        let mut chunk = MockChunkInfo::new();
        let digest = RafsDigest { data: [42u8; 32] };
        chunk.block_id = digest;
        assert_eq!(DigestedChunkMap::get_index(&chunk), digest);
    }

    #[test]
    fn test_default_creates_empty_map() {
        let map = DigestedChunkMap::default();
        let chunk = MockChunkInfo::new();
        assert!(!map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_set_ready_is_idempotent() {
        let map = DigestedChunkMap::new();
        let mut chunk = MockChunkInfo::new();
        chunk.block_id = RafsDigest { data: [5u8; 32] };

        map.set_ready_and_clear_pending(&chunk).unwrap();
        map.set_ready_and_clear_pending(&chunk).unwrap();
        assert!(map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_default_methods() {
        let map = DigestedChunkMap::new();
        let chunk = MockChunkInfo::new();
        assert!(!map.is_pending(&chunk).unwrap());
        assert!(!map.is_persist());
        assert!(map.as_range_map().is_none());
    }
}
