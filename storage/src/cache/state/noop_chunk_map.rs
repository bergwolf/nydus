// Copyright (C) 2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::io::Result;

use crate::cache::state::{ChunkIndexGetter, ChunkMap};
use crate::device::BlobChunkInfo;

/// A dummy implementation of the [ChunkMap] trait.
///
/// The `NoopChunkMap` is an dummy implementation of [ChunkMap], which just reports every chunk as
/// always ready to use or not. It may be used to support disk based backend storage.
pub struct NoopChunkMap {
    cached: bool,
}

impl NoopChunkMap {
    /// Create a new instance of `NoopChunkMap`.
    pub fn new(cached: bool) -> Self {
        Self { cached }
    }
}

impl ChunkMap for NoopChunkMap {
    fn is_ready(&self, _chunk: &dyn BlobChunkInfo) -> Result<bool> {
        Ok(self.cached)
    }
}

impl ChunkIndexGetter for NoopChunkMap {
    type Index = u32;

    fn get_index(chunk: &dyn BlobChunkInfo) -> Self::Index {
        chunk.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::MockChunkInfo;

    #[test]
    fn test_noop_chunk_map_cached_true() {
        let map = NoopChunkMap::new(true);
        let chunk = MockChunkInfo::new();
        assert!(map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_noop_chunk_map_cached_false() {
        let map = NoopChunkMap::new(false);
        let chunk = MockChunkInfo::new();
        assert!(!map.is_ready(&chunk).unwrap());
    }

    #[test]
    fn test_noop_chunk_map_get_index() {
        let mut chunk = MockChunkInfo::new();
        chunk.index = 42;
        assert_eq!(NoopChunkMap::get_index(&chunk), 42);
    }

    #[test]
    fn test_noop_chunk_map_is_ready_ignores_chunk_identity() {
        let map = NoopChunkMap::new(true);
        let mut chunk1 = MockChunkInfo::new();
        chunk1.index = 1;
        let mut chunk2 = MockChunkInfo::new();
        chunk2.index = 2;
        // Both report ready regardless of chunk identity
        assert!(map.is_ready(&chunk1).unwrap());
        assert!(map.is_ready(&chunk2).unwrap());
    }

    #[test]
    fn test_noop_chunk_map_default_methods() {
        let map = NoopChunkMap::new(false);
        let chunk = MockChunkInfo::new();
        // is_pending should return false (default impl)
        assert!(!map.is_pending(&chunk).unwrap());
        // is_persist should return false (default impl)
        assert!(!map.is_persist());
        // as_range_map should return None (default impl)
        assert!(map.as_range_map().is_none());
    }
}
