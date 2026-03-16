// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowOptionFields(t *testing.T) {
	opt := WorkflowOption{
		ChunkDict:        "/chunk/dict",
		TargetDir:        "/target",
		NydusImagePath:   "/usr/bin/nydus-image",
		PrefetchPatterns: "/",
		FsVersion:        "6",
		Compressor:       "zstd",
		ChunkSize:        "0x100000",
	}

	require.Equal(t, "/chunk/dict", opt.ChunkDict)
	require.Equal(t, "/target", opt.TargetDir)
	require.Equal(t, "/usr/bin/nydus-image", opt.NydusImagePath)
	require.Equal(t, "/", opt.PrefetchPatterns)
	require.Equal(t, "6", opt.FsVersion)
	require.Equal(t, "zstd", opt.Compressor)
	require.Equal(t, "0x100000", opt.ChunkSize)
}

func TestNewWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	opt := WorkflowOption{
		TargetDir:      tmpDir,
		NydusImagePath: "/usr/bin/nydus-image",
	}

	wf, err := NewWorkflow(opt)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, filepath.Join(tmpDir, "blobs"), wf.blobsDir)
	require.NotNil(t, wf.builder)

	// Verify blobs directory was created
	info, err := os.Stat(filepath.Join(tmpDir, "blobs"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestNewWorkflowCreatesCleanBlobsDir(t *testing.T) {
	tmpDir := t.TempDir()
	blobsDir := filepath.Join(tmpDir, "blobs")

	// Create a pre-existing file in blobs dir
	err := os.MkdirAll(blobsDir, 0755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(blobsDir, "stale-blob"), []byte("stale"), 0644)
	require.NoError(t, err)

	opt := WorkflowOption{
		TargetDir:      tmpDir,
		NydusImagePath: "/usr/bin/nydus-image",
	}

	wf, err := NewWorkflow(opt)
	require.NoError(t, err)
	require.NotNil(t, wf)

	// Verify stale file was removed
	_, err = os.Stat(filepath.Join(blobsDir, "stale-blob"))
	require.True(t, os.IsNotExist(err))
}

func TestBuildOutputJSONPath(t *testing.T) {
	wf := &Workflow{
		bootstrapPath: "/tmp/bootstrap",
	}
	require.Equal(t, "/tmp/bootstrap-output.json", wf.buildOutputJSONPath())
}

func TestGetLatestBlobPath(t *testing.T) {
	tmpDir := t.TempDir()
	blobsDir := filepath.Join(tmpDir, "blobs")
	err := os.MkdirAll(blobsDir, 0755)
	require.NoError(t, err)

	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      blobsDir,
	}

	// Create output JSON with blob IDs
	outputJSON := debugJSON{
		Version: "1.0.0",
		Blobs:   []string{"blob-id-1", "blob-id-2"},
	}
	jsonBytes, err := json.Marshal(outputJSON)
	require.NoError(t, err)
	err = os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)
	require.NoError(t, err)

	blobPath, err := wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(blobsDir, "blob-id-2"), blobPath)
	require.Equal(t, "1.0.0", wf.BuilderVersion)
}

func TestGetLatestBlobPathEmptyBlobs(t *testing.T) {
	tmpDir := t.TempDir()
	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      filepath.Join(tmpDir, "blobs"),
	}

	outputJSON := debugJSON{
		Version: "1.0.0",
		Blobs:   []string{},
	}
	jsonBytes, err := json.Marshal(outputJSON)
	require.NoError(t, err)
	err = os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)
	require.NoError(t, err)

	blobPath, err := wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, "", blobPath)
}

func TestGetLatestBlobPathSameAsPrevious(t *testing.T) {
	tmpDir := t.TempDir()
	blobsDir := filepath.Join(tmpDir, "blobs")
	err := os.MkdirAll(blobsDir, 0755)
	require.NoError(t, err)

	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      blobsDir,
		lastBlobID:    "blob-id-1",
	}

	outputJSON := debugJSON{
		Version: "1.0.0",
		Blobs:   []string{"blob-id-1"},
	}
	jsonBytes, err := json.Marshal(outputJSON)
	require.NoError(t, err)
	err = os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)
	require.NoError(t, err)

	blobPath, err := wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, "", blobPath)
}

func TestGetLatestBlobPathMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "nonexistent-bootstrap"),
		blobsDir:      filepath.Join(tmpDir, "blobs"),
	}

	_, err := wf.getLatestBlobPath()
	require.Error(t, err)
}

func TestGetLatestBlobPathInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      filepath.Join(tmpDir, "blobs"),
	}

	err := os.WriteFile(wf.buildOutputJSONPath(), []byte("invalid json"), 0644)
	require.NoError(t, err)

	_, err = wf.getLatestBlobPath()
	require.Error(t, err)
}

func TestBuilderStdoutStderr(t *testing.T) {
	builder := NewBuilder("/usr/bin/nydus-image")
	require.Equal(t, os.Stdout, builder.stdout)
	require.Equal(t, os.Stderr, builder.stderr)
}

func TestRunWithParentBootstrap(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Run(BuilderOption{
		ParentBootstrapPath: "/tmp/parent-bootstrap",
		BootstrapPath:       "/tmp/bootstrap",
		RootfsPath:          "/tmp/rootfs",
		WhiteoutSpec:        "oci",
		OutputJSONPath:      "/tmp/output.json",
		BlobPath:            "/tmp/blob",
		FsVersion:           "6",
	})
	require.Error(t, err)
}

func TestRunWithAllOptions(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Run(BuilderOption{
		ParentBootstrapPath: "/tmp/parent",
		BootstrapPath:       "/tmp/bootstrap",
		RootfsPath:          "/tmp/rootfs",
		WhiteoutSpec:        "oci",
		OutputJSONPath:      "/tmp/output.json",
		BlobPath:            "/tmp/blob",
		FsVersion:           "6",
		AlignedChunk:        true,
		ChunkDict:           "/tmp/chunkdict",
		Compressor:          "zstd",
		PrefetchPatterns:    "/",
		ChunkSize:           "0x100000",
	})
	require.Error(t, err)
}

func TestRunWithNoParentBootstrapNoOptionalFields(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Run(BuilderOption{
		BootstrapPath:  "/tmp/bootstrap",
		RootfsPath:     "/tmp/rootfs",
		WhiteoutSpec:   "oci",
		OutputJSONPath: "/tmp/output.json",
		BlobPath:       "/tmp/blob",
		FsVersion:      "5",
	})
	require.Error(t, err)
}

func TestCompactWithOutputBootstrapAndChunkDict(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Compact(CompactOption{
		BootstrapPath:       "/tmp/bootstrap",
		OutputBootstrapPath: "/tmp/out-bootstrap",
		BlobsDir:            "/tmp/blobs",
		MinUsedRatio:        "5",
		CompactBlobSize:     "10485760",
		MaxCompactSize:      "104857600",
		LayersToCompact:     "32",
		BackendType:         "localfs",
		BackendConfigPath:   "/tmp/backend.json",
		OutputJSONPath:      "/tmp/output.json",
		ChunkDict:           "/tmp/chunkdict",
	})
	require.Error(t, err)
}

func TestCompactWithoutOptionalFields(t *testing.T) {
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

func TestGenerateWithMultipleBootstraps(t *testing.T) {
	builder := NewBuilder("/nonexistent/binary")
	err := builder.Generate(GenerateOption{
		BootstrapPaths:         []string{"/tmp/bootstrap1", "/tmp/bootstrap2", "/tmp/bootstrap3"},
		DatabasePath:           "/tmp/db",
		ChunkdictBootstrapPath: "/tmp/chunkdict",
		OutputPath:             "/tmp/output",
	})
	require.Error(t, err)
}

func TestWorkflowBackendConfig(t *testing.T) {
	tmpDir := t.TempDir()
	opt := WorkflowOption{
		TargetDir:      tmpDir,
		NydusImagePath: "/usr/bin/nydus-image",
	}

	wf, err := NewWorkflow(opt)
	require.NoError(t, err)
	require.Contains(t, wf.backendConfig, tmpDir)
	require.Contains(t, wf.backendConfig, "blobs")
}

func TestWorkflowFields(t *testing.T) {
	tmpDir := t.TempDir()
	opt := WorkflowOption{
		TargetDir:        tmpDir,
		NydusImagePath:   "/usr/bin/nydus-image",
		ChunkDict:        "/chunkdict",
		PrefetchPatterns: "/",
		FsVersion:        "6",
		Compressor:       "zstd",
		ChunkSize:        "0x100000",
	}

	wf, err := NewWorkflow(opt)
	require.NoError(t, err)
	require.Equal(t, "/chunkdict", wf.ChunkDict)
	require.Equal(t, "/", wf.PrefetchPatterns)
	require.Equal(t, "6", wf.FsVersion)
	require.Equal(t, "zstd", wf.Compressor)
	require.Equal(t, "0x100000", wf.ChunkSize)
	require.Empty(t, wf.BuilderVersion)
	require.Empty(t, wf.lastBlobID)
}

func TestGetLatestBlobPathMultipleBlobs(t *testing.T) {
	tmpDir := t.TempDir()
	blobsDir := filepath.Join(tmpDir, "blobs")
	err := os.MkdirAll(blobsDir, 0755)
	require.NoError(t, err)

	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      blobsDir,
		lastBlobID:    "blob-id-1",
	}

	outputJSON := debugJSON{
		Version: "2.0.0",
		Blobs:   []string{"blob-id-1", "blob-id-2", "blob-id-3"},
	}
	jsonBytes, err := json.Marshal(outputJSON)
	require.NoError(t, err)
	err = os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)
	require.NoError(t, err)

	blobPath, err := wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(blobsDir, "blob-id-3"), blobPath)
	require.Equal(t, "2.0.0", wf.BuilderVersion)
	require.Equal(t, "blob-id-3", wf.lastBlobID)
}

func TestDebugJSONSerialization(t *testing.T) {
	dj := debugJSON{
		Version: "1.0.0",
		Blobs:   []string{"a", "b"},
	}
	data, err := json.Marshal(dj)
	require.NoError(t, err)

	var result debugJSON
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	require.Equal(t, dj, result)
}

func TestDebugJSONEmpty(t *testing.T) {
	dj := debugJSON{}
	data, err := json.Marshal(dj)
	require.NoError(t, err)

	var result debugJSON
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	require.Empty(t, result.Version)
	require.Nil(t, result.Blobs)
}
