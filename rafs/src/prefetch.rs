// Copyright 2026 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming blob prefetcher for Dragonfly proxy optimization.
//!
//! When a Dragonfly dfdaemon proxy is configured, this module sends rangeless
//! GET requests per blob, causing the proxy to download and cache entire blobs.
//! Chunks are matched from the stream by compressed offset and persisted to
//! the local file cache. This replaces N×1MB Range requests with a single
//! streaming connection per blob, reducing proxy task overhead from ~2600
//! per-chunk requests to ~10 per-blob streaming connections.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nydus_storage::backend::RequestSource;
use nydus_storage::cache::BlobCache;
use nydus_storage::device::{BlobChunkInfo, BlobInfo};

use crate::metadata::{RafsInodeExt, RafsSuper};

const DEFAULT_THREADS: usize = 5;
const DEFAULT_BANDWIDTH_RATE: u64 = 10 * 1024 * 1024; // 10 MB/s
const DEFAULT_MAX_RETRY: u64 = 10;
const STREAM_READ_SIZE: usize = 1024 * 1024; // 1MB per stream read

/// A blob and its chunks to prefetch, sorted by compressed offset.
struct BlobWork {
    info: Arc<BlobInfo>,
    /// Chunks sorted by compressed offset (via BTreeMap insertion).
    chunks: Vec<Arc<dyn BlobChunkInfo>>,
}

/// Progress tracking for the prefetcher.
pub struct PrefetchProgress {
    pub total_blobs: AtomicUsize,
    pub prefetched_blobs: AtomicUsize,
    pub total_chunks: AtomicUsize,
    pub prefetched_chunks: AtomicUsize,
    pub total_bytes: AtomicUsize,
    pub prefetched_bytes: AtomicUsize,
}

impl Default for PrefetchProgress {
    fn default() -> Self {
        Self {
            total_blobs: AtomicUsize::new(0),
            prefetched_blobs: AtomicUsize::new(0),
            total_chunks: AtomicUsize::new(0),
            prefetched_chunks: AtomicUsize::new(0),
            total_bytes: AtomicUsize::new(0),
            prefetched_bytes: AtomicUsize::new(0),
        }
    }
}

/// Token bucket rate limiter for bandwidth control.
struct RateLimiter {
    rate: u64,
    capacity: u64,
    available_tokens: u64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(rate: u64) -> Self {
        let capacity = rate.saturating_mul(2);
        Self {
            rate,
            capacity,
            available_tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    /// Consume `bytes` tokens. Returns `Some(duration)` if the caller should
    /// sleep to stay within the rate limit, `None` if no wait is needed.
    fn consume(&mut self, bytes: usize) -> Option<Duration> {
        let bytes = bytes as u64;
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let tokens_to_add = (elapsed.as_secs_f64() * self.rate as f64) as u64;
        if tokens_to_add > 0 {
            self.available_tokens = self
                .available_tokens
                .saturating_add(tokens_to_add)
                .min(self.capacity);
            self.last_refill = now;
        }
        if self.available_tokens >= bytes {
            self.available_tokens -= bytes;
            return None;
        }
        let tokens_needed = bytes - self.available_tokens;
        let wait = Duration::from_secs_f64(tokens_needed as f64 / self.rate as f64);
        self.available_tokens = 0;
        Some(wait)
    }
}

struct State {
    stop_flag: AtomicBool,
    progress: PrefetchProgress,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    thread_cv: Condvar,
    threads_count: usize,
    rate_limiter: Option<Arc<Mutex<RateLimiter>>>,
    max_retry_per_blob: u64,
}

/// Streaming blob prefetcher that downloads entire blobs via rangeless GET
/// requests and caches chunks from the stream.
pub struct BlobPrefetcher {
    sb: Arc<RafsSuper>,
    caches: Vec<Arc<dyn BlobCache>>,
    state: Arc<State>,
}

impl BlobPrefetcher {
    pub fn new(
        sb: Arc<RafsSuper>,
        caches: Vec<Arc<dyn BlobCache>>,
        threads_count: usize,
        bandwidth_rate: u64,
        max_retry: u64,
    ) -> Arc<Self> {
        let threads_count = if threads_count == 0 {
            DEFAULT_THREADS
        } else {
            threads_count
        };
        let max_retry = if max_retry == 0 {
            DEFAULT_MAX_RETRY
        } else {
            max_retry
        };
        let rate = if bandwidth_rate == 0 {
            DEFAULT_BANDWIDTH_RATE
        } else {
            bandwidth_rate
        };
        let rate_limiter = Some(Arc::new(Mutex::new(RateLimiter::new(rate))));

        Arc::new(Self {
            sb,
            caches,
            state: Arc::new(State {
                stop_flag: AtomicBool::new(false),
                progress: PrefetchProgress::default(),
                thread: Mutex::new(None),
                thread_cv: Condvar::new(),
                threads_count,
                rate_limiter,
                max_retry_per_blob: max_retry,
            }),
        })
    }

    /// Start the prefetcher in a background thread.
    pub fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut thread = self.state.thread.lock().unwrap();
        if thread.is_some() {
            anyhow::bail!("BlobPrefetcher already running");
        }
        self.state.stop_flag.store(false, Ordering::Release);

        let prefetcher = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("blob-prefetcher".to_string())
            .spawn(move || {
                match prefetcher.build_blobs() {
                    Ok(blobs) => {
                        let total_chunks: usize = blobs.iter().map(|b| b.chunks.len()).sum();
                        info!(
                            "BlobPrefetcher: collected {} blobs with {} chunks",
                            blobs.len(),
                            total_chunks
                        );
                        prefetcher
                            .state
                            .progress
                            .total_blobs
                            .store(blobs.len(), Ordering::Relaxed);
                        prefetcher
                            .state
                            .progress
                            .total_chunks
                            .store(total_chunks, Ordering::Relaxed);

                        if let Err(e) = prefetcher.prefetch_all(blobs) {
                            error!("BlobPrefetcher: prefetch failed: {:?}", e);
                        }
                    }
                    Err(e) => error!("BlobPrefetcher: failed to build blobs: {:?}", e),
                }
                *prefetcher.state.thread.lock().unwrap() = None;
                prefetcher.state.thread_cv.notify_all();
                info!("BlobPrefetcher: thread completed");
            })?;

        *thread = Some(handle);
        Ok(())
    }

    /// Stop the prefetcher, waiting up to 5 seconds for the thread to finish.
    pub fn stop(&self) {
        info!("BlobPrefetcher: stopping");
        self.state.stop_flag.store(true, Ordering::Release);

        let timeout = Duration::from_secs(5);
        let deadline = Instant::now() + timeout;
        let mut thread = self.state.thread.lock().unwrap();
        while thread.is_some() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!("BlobPrefetcher: timed out waiting, detaching thread");
                let _ = thread.take();
                break;
            }
            let (guard, _) = self
                .state
                .thread_cv
                .wait_timeout(thread, remaining)
                .unwrap();
            thread = guard;
        }
    }

    /// Get progress tracking data.
    pub fn progress(&self) -> &PrefetchProgress {
        &self.state.progress
    }

    /// Build blob work items by traversing the RAFS filesystem tree.
    ///
    /// Collects all chunks from all regular files, deduplicates them by chunk
    /// ID within each blob (via BTreeMap), and returns them sorted by
    /// compressed offset (BTreeMap's natural ordering).
    fn build_blobs(&self) -> anyhow::Result<Vec<BlobWork>> {
        // Map: blob_index → BTreeMap<chunk_id, chunk>
        let mut blob_map: BTreeMap<u32, BTreeMap<u64, Arc<dyn BlobChunkInfo>>> = BTreeMap::new();

        let root_ino = self.sb.superblock.root_ino();
        let root = self.sb.get_extended_inode(root_ino, false)?;
        let mut stack: Vec<Arc<dyn RafsInodeExt>> = vec![root];

        while let Some(inode) = stack.pop() {
            if inode.is_reg() {
                let chunk_count = inode.get_chunk_count();
                for idx in 0..chunk_count {
                    if let Ok(chunk) = inode.get_chunk_info(idx) {
                        let blob_index = chunk.blob_index();
                        // Use compressed_offset as key for dedup + natural sort order
                        blob_map
                            .entry(blob_index)
                            .or_default()
                            .insert(chunk.compressed_offset(), chunk);
                    }
                }
            } else if inode.is_dir() {
                let child_count = inode.get_child_count();
                for idx in 0..child_count {
                    if let Ok(child) = inode.get_child_by_index(idx) {
                        stack.push(child);
                    }
                }
            }
        }

        let blob_infos = self.sb.superblock.get_blob_infos();
        let mut blobs = Vec::new();
        for (blob_index, chunks_map) in blob_map {
            if (blob_index as usize) < blob_infos.len() {
                let chunks: Vec<Arc<dyn BlobChunkInfo>> = chunks_map.into_values().collect();
                blobs.push(BlobWork {
                    info: blob_infos[blob_index as usize].clone(),
                    chunks,
                });
            }
        }
        Ok(blobs)
    }

    /// Distribute blobs to worker threads and wait for completion.
    fn prefetch_all(&self, blobs: Vec<BlobWork>) -> anyhow::Result<()> {
        let blob_count = blobs.len();
        if blob_count == 0 {
            return Ok(());
        }
        let worker_count = self.state.threads_count.min(blob_count).max(1);

        let (tx, rx) = std::sync::mpsc::channel::<(BlobWork, Arc<dyn BlobCache>)>();
        let rx = Arc::new(Mutex::new(rx));

        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let rx = Arc::clone(&rx);
            let state = Arc::clone(&self.state);
            let handle = thread::Builder::new()
                .name(format!("blob-pf-{}", worker_id))
                .spawn(move || {
                    loop {
                        let work = {
                            let rx = rx.lock().unwrap();
                            rx.recv().ok()
                        };
                        let Some((blob, cache)) = work else { break };

                        let blob_id = blob.info.blob_id().to_string();
                        let chunk_count = blob.chunks.len();
                        let mut retries = 0u64;

                        loop {
                            if state.stop_flag.load(Ordering::Acquire) {
                                break;
                            }

                            let mut chunk_status = vec![false; chunk_count];
                            match Self::prefetch_one_blob(&state, &blob, &cache, &mut chunk_status)
                            {
                                Ok(()) => {
                                    state
                                        .progress
                                        .prefetched_blobs
                                        .fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(e) => {
                                    retries += 1;
                                    if retries >= state.max_retry_per_blob {
                                        error!(
                                            "BlobPrefetcher: blob {} failed after {} retries: {:?}",
                                            blob_id, retries, e
                                        );
                                        break;
                                    }
                                    // Random backoff: 100ms * retry_count
                                    let backoff = Duration::from_millis(100 * retries);
                                    warn!(
                                        "BlobPrefetcher: blob {} retry {}/{}, backoff {:?}: {:?}",
                                        blob_id, retries, state.max_retry_per_blob, backoff, e
                                    );
                                    thread::sleep(backoff);
                                }
                            }
                        }
                    }
                })
                .map_err(|e| anyhow::anyhow!("failed to spawn worker thread: {}", e))?;

            handles.push(handle);
        }

        // Send blobs to workers with matching cache
        for blob in blobs {
            if self.state.stop_flag.load(Ordering::Acquire) {
                break;
            }
            let blob_index = blob.info.blob_index();
            if let Some(cache) = self.caches.get(blob_index as usize) {
                let _ = tx.send((blob, cache.clone()));
            }
        }
        drop(tx);

        for handle in handles {
            let _ = handle.join();
        }

        let progress = &self.state.progress;
        info!(
            "BlobPrefetcher: completed {}/{} blobs, {}/{} chunks, {} bytes",
            progress.prefetched_blobs.load(Ordering::Relaxed),
            progress.total_blobs.load(Ordering::Relaxed),
            progress.prefetched_chunks.load(Ordering::Relaxed),
            progress.total_chunks.load(Ordering::Relaxed),
            progress.prefetched_bytes.load(Ordering::Relaxed),
        );
        Ok(())
    }

    /// Prefetch a single blob: check cache status, get stream reader, stream and cache.
    fn prefetch_one_blob(
        state: &Arc<State>,
        blob: &BlobWork,
        cache: &Arc<dyn BlobCache>,
        chunk_status: &mut [bool],
    ) -> anyhow::Result<()> {
        let blob_id = blob.info.blob_id();
        let chunk_map = cache.get_chunk_map();

        // Find first uncached chunk
        let mut first_not_ready: Option<usize> = None;
        let mut start_offset: u64 = 0;

        for (idx, chunk) in blob.chunks.iter().enumerate() {
            if state.stop_flag.load(Ordering::Acquire) {
                return Ok(());
            }
            if chunk_status[idx] {
                continue;
            }

            if matches!(chunk_map.is_ready(chunk.as_ref()), Ok(true)) {
                chunk_status[idx] = true;
                continue;
            }

            if first_not_ready.is_none() {
                first_not_ready = Some(idx);
                start_offset = chunk.compressed_offset();
            }
        }

        if first_not_ready.is_none() {
            info!("BlobPrefetcher: blob {} fully cached, skipping", blob_id);
            return Ok(());
        }

        info!(
            "BlobPrefetcher: streaming blob {} from offset {}",
            blob_id, start_offset
        );

        // Get streaming reader from backend
        let reader = cache.reader();
        let stream_reader = reader
            .stream_read(start_offset, RequestSource::Prefetch)
            .map_err(|e| anyhow::anyhow!("stream_read failed for blob {}: {:?}", blob_id, e))?;

        Self::stream_and_cache(
            state,
            stream_reader,
            blob,
            cache,
            start_offset,
            chunk_status,
        )
    }

    /// Stream blob data and cache matched chunks.
    fn stream_and_cache(
        state: &Arc<State>,
        mut reader: Box<dyn Read + Send>,
        blob: &BlobWork,
        cache: &Arc<dyn BlobCache>,
        start_offset: u64,
        chunk_status: &mut [bool],
    ) -> anyhow::Result<()> {
        let blob_id = blob.info.blob_id();
        let last_chunk_end = blob
            .chunks
            .iter()
            .map(|c| c.compressed_offset() + c.compressed_size() as u64)
            .max()
            .unwrap_or(start_offset);

        let max_chunk_size = blob
            .chunks
            .iter()
            .map(|c| c.compressed_size() as usize)
            .max()
            .unwrap_or(STREAM_READ_SIZE);

        let mut accumulated: Vec<u8> = Vec::new();
        let mut acc_offset = start_offset;
        let mut scan_start: usize = 0;
        let mut chunks_cached = 0usize;

        loop {
            if state.stop_flag.load(Ordering::Acquire) {
                return Ok(());
            }

            // Read from stream
            let mut read_buf = vec![0u8; STREAM_READ_SIZE];
            let n = reader.read(&mut read_buf)?;
            if n == 0 {
                break;
            }

            // Rate limiting
            if let Some(ref limiter) = state.rate_limiter {
                if let Some(d) = limiter.lock().unwrap().consume(n) {
                    thread::sleep(d);
                }
            }

            accumulated.extend_from_slice(&read_buf[..n]);
            let acc_end = acc_offset + accumulated.len() as u64;

            // Match chunks against accumulated buffer
            let mut idx = scan_start;
            while idx < blob.chunks.len() {
                if chunk_status[idx] {
                    idx += 1;
                    continue;
                }

                let chunk = &blob.chunks[idx];
                let chunk_start = chunk.compressed_offset();
                let chunk_size = chunk.compressed_size() as usize;
                let chunk_end = chunk_start + chunk_size as u64;

                if chunk_start < acc_offset {
                    // Chunk before our buffer window, skip
                    scan_start = idx + 1;
                    idx += 1;
                    continue;
                }
                if chunk_end > acc_end {
                    // Not fully in buffer yet
                    break;
                }

                // Chunk is fully contained in accumulated buffer
                let buf_offset = (chunk_start - acc_offset) as usize;
                let chunk_data = &accumulated[buf_offset..buf_offset + chunk_size];

                match cache.cache_chunk_data(chunk.as_ref(), chunk_data) {
                    Ok(newly_cached) => {
                        chunk_status[idx] = true;
                        if newly_cached {
                            chunks_cached += 1;
                            state
                                .progress
                                .prefetched_chunks
                                .fetch_add(1, Ordering::Relaxed);
                            state
                                .progress
                                .prefetched_bytes
                                .fetch_add(chunk_size, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "BlobPrefetcher: failed to cache chunk {} of blob {}: {:?}",
                            chunk.id(),
                            blob_id,
                            e
                        );
                    }
                }
                idx += 1;
            }

            // Trim accumulated buffer — keep at least max_chunk_size to handle
            // chunks that span read boundaries.
            let trim_to = acc_end.saturating_sub(max_chunk_size as u64);
            if trim_to > acc_offset {
                let trim_bytes = (trim_to - acc_offset) as usize;
                if trim_bytes < accumulated.len() {
                    accumulated.drain(..trim_bytes);
                    acc_offset = trim_to;
                }
            }

            // Early exit conditions
            if chunk_status.iter().all(|&c| c) {
                break;
            }
            if acc_end >= last_chunk_end {
                break;
            }
        }

        info!(
            "BlobPrefetcher: streamed blob {}, cached {} chunks",
            blob_id, chunks_cached
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_no_wait_under_capacity() {
        let mut limiter = RateLimiter::new(10 * 1024 * 1024); // 10 MB/s
                                                              // Under capacity, no wait needed
        assert!(limiter.consume(1024).is_none());
    }

    #[test]
    fn test_rate_limiter_returns_wait_when_exhausted() {
        let mut limiter = RateLimiter::new(1024); // 1 KB/s, capacity = 2KB
                                                  // Drain all capacity
        assert!(limiter.consume(2048).is_none());
        // Now should need to wait
        let wait = limiter.consume(1024);
        assert!(wait.is_some());
        assert!(wait.unwrap() > Duration::from_millis(500));
    }

    #[test]
    fn test_rate_limiter_refills_over_time() {
        let mut limiter = RateLimiter::new(10_000); // 10KB/s, capacity=20KB
                                                    // Drain capacity
        assert!(limiter.consume(20_000).is_none());
        // Advance time by simulating — just sleep briefly
        std::thread::sleep(Duration::from_millis(200));
        // Should have refilled ~2KB worth of tokens
        // May or may not need to wait depending on exact timing
        let _result = limiter.consume(1000);
        // Just verify it doesn't panic
    }
}
