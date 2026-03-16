// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBuilder(t *testing.T) {
	builder := NewBuilder("/usr/bin/nydus-image")
	require.NotNil(t, builder)
	require.Equal(t, "/usr/bin/nydus-image", builder.binaryPath)
	require.NotNil(t, builder.stdout)
	require.NotNil(t, builder.stderr)
}

func TestNewBuilderEmptyPath(t *testing.T) {
	builder := NewBuilder("")
	require.NotNil(t, builder)
	require.Equal(t, "", builder.binaryPath)
}

func TestBuilderOptionFields(t *testing.T) {
	opt := BuilderOption{
		ParentBootstrapPath: "/parent/bootstrap",
		ChunkDict:           "/chunk/dict",
		BootstrapPath:       "/bootstrap",
		RootfsPath:          "/rootfs",
		BackendType:         "oss",
		BackendConfig:       `{"bucket":"test"}`,
		WhiteoutSpec:        "oci",
		OutputJSONPath:      "/output.json",
		PrefetchPatterns:    "/",
		BlobPath:            "/blobs/blob1",
		AlignedChunk:        true,
		Compressor:          "zstd",
		ChunkSize:           "0x100000",
		FsVersion:           "6",
	}

	require.Equal(t, "/parent/bootstrap", opt.ParentBootstrapPath)
	require.Equal(t, "/chunk/dict", opt.ChunkDict)
	require.Equal(t, "/bootstrap", opt.BootstrapPath)
	require.Equal(t, "/rootfs", opt.RootfsPath)
	require.Equal(t, "oss", opt.BackendType)
	require.Equal(t, `{"bucket":"test"}`, opt.BackendConfig)
	require.Equal(t, "oci", opt.WhiteoutSpec)
	require.Equal(t, "/output.json", opt.OutputJSONPath)
	require.Equal(t, "/", opt.PrefetchPatterns)
	require.Equal(t, "/blobs/blob1", opt.BlobPath)
	require.True(t, opt.AlignedChunk)
	require.Equal(t, "zstd", opt.Compressor)
	require.Equal(t, "0x100000", opt.ChunkSize)
	require.Equal(t, "6", opt.FsVersion)
}

func TestCompactOptionFields(t *testing.T) {
	opt := CompactOption{
		ChunkDict:           "/chunk/dict",
		BootstrapPath:       "/bootstrap",
		OutputBootstrapPath: "/output/bootstrap",
		BackendType:         "oss",
		BackendConfigPath:   "/backend/config.json",
		OutputJSONPath:      "/output.json",
		MinUsedRatio:        "5",
		CompactBlobSize:     "10485760",
		MaxCompactSize:      "104857600",
		LayersToCompact:     "32",
		BlobsDir:            "/blobs",
	}

	require.Equal(t, "/chunk/dict", opt.ChunkDict)
	require.Equal(t, "/bootstrap", opt.BootstrapPath)
	require.Equal(t, "/output/bootstrap", opt.OutputBootstrapPath)
	require.Equal(t, "oss", opt.BackendType)
	require.Equal(t, "/backend/config.json", opt.BackendConfigPath)
	require.Equal(t, "/output.json", opt.OutputJSONPath)
	require.Equal(t, "5", opt.MinUsedRatio)
	require.Equal(t, "10485760", opt.CompactBlobSize)
	require.Equal(t, "104857600", opt.MaxCompactSize)
	require.Equal(t, "32", opt.LayersToCompact)
	require.Equal(t, "/blobs", opt.BlobsDir)
}

func TestGenerateOptionFields(t *testing.T) {
	opt := GenerateOption{
		BootstrapPaths:         []string{"/path1", "/path2"},
		DatabasePath:           "/database",
		ChunkdictBootstrapPath: "/chunkdict/bootstrap",
		OutputPath:             "/output",
	}

	require.Equal(t, []string{"/path1", "/path2"}, opt.BootstrapPaths)
	require.Equal(t, "/database", opt.DatabasePath)
	require.Equal(t, "/chunkdict/bootstrap", opt.ChunkdictBootstrapPath)
	require.Equal(t, "/output", opt.OutputPath)
}

func TestRunInvalidBinary(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.run([]string{"create"}, "")
	require.Error(t, err)
}

func TestRunWithBuilderOption(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Run(BuilderOption{
		BootstrapPath:  "/tmp/bootstrap",
		RootfsPath:     "/tmp/rootfs",
		WhiteoutSpec:   "oci",
		OutputJSONPath: "/tmp/output.json",
		BlobPath:       "/tmp/blob",
		FsVersion:      "6",
	})
	require.Error(t, err)
}

func TestCompactInvalidBinary(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Compact(CompactOption{
		BootstrapPath:     "/tmp/bootstrap",
		BlobsDir:          "/tmp/blobs",
		MinUsedRatio:      "5",
		CompactBlobSize:   "10485760",
		MaxCompactSize:    "104857600",
		LayersToCompact:   "32",
		BackendType:       "localfs",
		BackendConfigPath: "/tmp/backend.json",
		OutputJSONPath:    "/tmp/output.json",
	})
	require.Error(t, err)
}

func TestGenerateInvalidBinary(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Generate(GenerateOption{
		BootstrapPaths:         []string{"/tmp/bootstrap"},
		DatabasePath:           "/tmp/db",
		ChunkdictBootstrapPath: "/tmp/chunkdict",
		OutputPath:             "/tmp/output",
	})
	require.Error(t, err)
}

func TestBuilderOptionDefaultValues(t *testing.T) {
	opt := BuilderOption{}
	require.Empty(t, opt.ParentBootstrapPath)
	require.Empty(t, opt.ChunkDict)
	require.Empty(t, opt.BootstrapPath)
	require.Empty(t, opt.RootfsPath)
	require.Empty(t, opt.BackendType)
	require.Empty(t, opt.BackendConfig)
	require.Empty(t, opt.WhiteoutSpec)
	require.Empty(t, opt.OutputJSONPath)
	require.Empty(t, opt.PrefetchPatterns)
	require.Empty(t, opt.BlobPath)
	require.False(t, opt.AlignedChunk)
	require.Empty(t, opt.Compressor)
	require.Empty(t, opt.ChunkSize)
	require.Empty(t, opt.FsVersion)
}

func TestCompactOptionDefaultValues(t *testing.T) {
	opt := CompactOption{}
	require.Empty(t, opt.ChunkDict)
	require.Empty(t, opt.BootstrapPath)
	require.Empty(t, opt.OutputBootstrapPath)
	require.Empty(t, opt.BackendType)
	require.Empty(t, opt.BackendConfigPath)
	require.Empty(t, opt.OutputJSONPath)
	require.Empty(t, opt.MinUsedRatio)
	require.Empty(t, opt.CompactBlobSize)
	require.Empty(t, opt.MaxCompactSize)
	require.Empty(t, opt.LayersToCompact)
	require.Empty(t, opt.BlobsDir)
}

func TestGenerateOptionDefaultValues(t *testing.T) {
	opt := GenerateOption{}
	require.Nil(t, opt.BootstrapPaths)
	require.Empty(t, opt.DatabasePath)
	require.Empty(t, opt.ChunkdictBootstrapPath)
	require.Empty(t, opt.OutputPath)
}

func TestNewBuilderDifferentPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"absolute path", "/usr/local/bin/nydus-image"},
		{"relative path", "./nydus-image"},
		{"just binary name", "nydus-image"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuilder(tt.path)
			require.Equal(t, tt.path, b.binaryPath)
		})
	}
}
