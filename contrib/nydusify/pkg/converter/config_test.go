// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package converter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfig(t *testing.T) {
	opt := Opt{
		WorkDir:          "/tmp/workdir",
		NydusImagePath:   "/usr/bin/nydus-image",
		BackendType:      "oss",
		BackendConfig:    `{"bucket":"test"}`,
		BackendForcePush: true,
		ChunkDictRef:     "registry.example.com/dict:latest",
		Docker2OCI:       true,
		MergePlatform:    false,
		OCIRef:           true,
		WithReferrer:     false,
		PrefetchPatterns: "/",
		Compressor:       "zstd",
		FsVersion:        "6",
		FsAlignChunk:     true,
		ChunkSize:        "0x100000",
		BatchSize:        "0x100000",
		CacheRef:         "registry.example.com/cache:v1",
		CacheVersion:     "v1",
		CacheMaxRecords:  200,
	}

	cfg := getConfig(opt)

	require.Equal(t, "/tmp/workdir", cfg["work_dir"])
	require.Equal(t, "/usr/bin/nydus-image", cfg["builder"])
	require.Equal(t, "oss", cfg["backend_type"])
	require.Equal(t, `{"bucket":"test"}`, cfg["backend_config"])
	require.Equal(t, "true", cfg["backend_force_push"])
	require.Equal(t, "registry.example.com/dict:latest", cfg["chunk_dict_ref"])
	require.Equal(t, "true", cfg["docker2oci"])
	require.Equal(t, "false", cfg["merge_manifest"])
	require.Equal(t, "true", cfg["oci_ref"])
	require.Equal(t, "false", cfg["with_referrer"])
	require.Equal(t, "/", cfg["prefetch_patterns"])
	require.Equal(t, "zstd", cfg["compressor"])
	require.Equal(t, "6", cfg["fs_version"])
	require.Equal(t, "true", cfg["fs_align_chunk"])
	require.Equal(t, "0x100000", cfg["fs_chunk_size"])
	require.Equal(t, "0x100000", cfg["batch_size"])
	require.Equal(t, "registry.example.com/cache:v1", cfg["cache_ref"])
	require.Equal(t, "v1", cfg["cache_version"])
	require.Equal(t, "200", cfg["cache_max_records"])
}

func TestGetConfigDefaults(t *testing.T) {
	opt := Opt{}
	cfg := getConfig(opt)

	require.Equal(t, "", cfg["work_dir"])
	require.Equal(t, "", cfg["builder"])
	require.Equal(t, "false", cfg["backend_force_push"])
	require.Equal(t, "false", cfg["docker2oci"])
	require.Equal(t, "false", cfg["merge_manifest"])
	require.Equal(t, "false", cfg["oci_ref"])
	require.Equal(t, "false", cfg["with_referrer"])
	require.Equal(t, "false", cfg["fs_align_chunk"])
	require.Equal(t, "0", cfg["cache_max_records"])
}

func TestGetConfigAllKeysPresent(t *testing.T) {
	opt := Opt{}
	cfg := getConfig(opt)

	expectedKeys := []string{
		"work_dir", "builder", "backend_type", "backend_config",
		"backend_force_push", "chunk_dict_ref", "docker2oci",
		"merge_manifest", "oci_ref", "with_referrer",
		"prefetch_patterns", "compressor", "fs_version",
		"fs_align_chunk", "fs_chunk_size", "batch_size",
		"cache_ref", "cache_version", "cache_max_records",
	}

	for _, key := range expectedKeys {
		_, ok := cfg[key]
		require.True(t, ok, "expected key %q to be present in config", key)
	}
}

func TestOptStructFields(t *testing.T) {
	opt := Opt{
		Source:            "source-image:latest",
		Target:            "target-image:latest",
		SourceInsecure:    true,
		TargetInsecure:    false,
		ChunkDictInsecure: true,
		CacheInsecure:     false,
		AllPlatforms:      true,
		Platforms:         "linux/amd64,linux/arm64",
		PushRetryCount:    3,
		PushRetryDelay:    "5s",
	}

	require.Equal(t, "source-image:latest", opt.Source)
	require.Equal(t, "target-image:latest", opt.Target)
	require.True(t, opt.SourceInsecure)
	require.False(t, opt.TargetInsecure)
	require.True(t, opt.ChunkDictInsecure)
	require.False(t, opt.CacheInsecure)
	require.True(t, opt.AllPlatforms)
	require.Equal(t, "linux/amd64,linux/arm64", opt.Platforms)
	require.Equal(t, 3, opt.PushRetryCount)
	require.Equal(t, "5s", opt.PushRetryDelay)
}
