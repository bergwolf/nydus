// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
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

func TestBuildOutputJSONPathVariousBootstraps(t *testing.T) {
	tests := []struct {
		name          string
		bootstrapPath string
		expected      string
	}{
		{"simple path", "/tmp/bootstrap", "/tmp/bootstrap-output.json"},
		{"nested path", "/a/b/c/bootstrap", "/a/b/c/bootstrap-output.json"},
		{"with extension", "/tmp/bootstrap.bin", "/tmp/bootstrap.bin-output.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf := &Workflow{bootstrapPath: tt.bootstrapPath}
			require.Equal(t, tt.expected, wf.buildOutputJSONPath())
		})
	}
}

func TestWorkflowOptionDefaults(t *testing.T) {
	opt := WorkflowOption{}
	require.Empty(t, opt.ChunkDict)
	require.Empty(t, opt.TargetDir)
	require.Empty(t, opt.NydusImagePath)
	require.Empty(t, opt.PrefetchPatterns)
	require.Empty(t, opt.FsVersion)
	require.Empty(t, opt.Compressor)
	require.Empty(t, opt.ChunkSize)
}

func TestWorkflowStructDefaults(t *testing.T) {
	wf := Workflow{}
	require.Empty(t, wf.BuilderVersion)
	require.Empty(t, wf.bootstrapPath)
	require.Empty(t, wf.blobsDir)
	require.Empty(t, wf.backendConfig)
	require.Empty(t, wf.parentBootstrapPath)
	require.Nil(t, wf.builder)
	require.Empty(t, wf.lastBlobID)
}

func TestGetLatestBlobPathSequentialCalls(t *testing.T) {
	tmpDir := t.TempDir()
	blobsDir := filepath.Join(tmpDir, "blobs")
	err := os.MkdirAll(blobsDir, 0755)
	require.NoError(t, err)

	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      blobsDir,
	}

	// First call with blob-id-1
	outputJSON := debugJSON{Version: "1.0", Blobs: []string{"blob-id-1"}}
	jsonBytes, _ := json.Marshal(outputJSON)
	os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)

	blobPath, err := wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(blobsDir, "blob-id-1"), blobPath)

	// Second call with same blob-id-1 should return empty
	blobPath, err = wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Empty(t, blobPath)

	// Third call with new blob-id-2 added
	outputJSON = debugJSON{Version: "1.0", Blobs: []string{"blob-id-1", "blob-id-2"}}
	jsonBytes, _ = json.Marshal(outputJSON)
	os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)

	blobPath, err = wf.getLatestBlobPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(blobsDir, "blob-id-2"), blobPath)
}

func TestGetLatestBlobPathSetsBuilderVersion(t *testing.T) {
	tmpDir := t.TempDir()
	wf := &Workflow{
		bootstrapPath: filepath.Join(tmpDir, "bootstrap"),
		blobsDir:      filepath.Join(tmpDir, "blobs"),
	}

	versions := []string{"0.1.0", "1.0.0-rc1", "2.5.3"}
	for _, v := range versions {
		outputJSON := debugJSON{Version: v, Blobs: []string{}}
		jsonBytes, _ := json.Marshal(outputJSON)
		os.WriteFile(wf.buildOutputJSONPath(), jsonBytes, 0644)

		_, err := wf.getLatestBlobPath()
		require.NoError(t, err)
		require.Equal(t, v, wf.BuilderVersion)
	}
}

func TestDebugJSONWithNullBlobs(t *testing.T) {
	jsonStr := `{"Version":"1.0","Blobs":null}`
	var dj debugJSON
	err := json.Unmarshal([]byte(jsonStr), &dj)
	require.NoError(t, err)
	require.Equal(t, "1.0", dj.Version)
	require.Nil(t, dj.Blobs)
}

func TestNewWorkflowSetsBuilder(t *testing.T) {
	tmpDir := t.TempDir()
	paths := []string{
		"/usr/bin/nydus-image",
		"/usr/local/bin/nydus-image",
		"./nydus-image",
	}
	for _, p := range paths {
		wf, err := NewWorkflow(WorkflowOption{
			TargetDir:      tmpDir,
			NydusImagePath: p,
		})
		require.NoError(t, err)
		require.NotNil(t, wf.builder)
		require.Equal(t, p, wf.builder.binaryPath)
	}
}

func TestNewWorkflowBackendConfigFormat(t *testing.T) {
	tmpDir := t.TempDir()
	wf, err := NewWorkflow(WorkflowOption{
		TargetDir:      tmpDir,
		NydusImagePath: "/usr/bin/nydus-image",
	})
	require.NoError(t, err)

	// Verify it's valid JSON
	var cfg map[string]string
	err = json.Unmarshal([]byte(wf.backendConfig), &cfg)
	require.NoError(t, err)
	require.Contains(t, cfg, "dir")
	require.Equal(t, filepath.Join(tmpDir, "blobs"), cfg["dir"])
}

// TestBuildWithMockedRunner consolidates all tests that mock Builder.Run
// into a single test function with a persistent patch to avoid repeated
// patch/reset cycles that are unreliable on ARM64.
func TestBuildWithMockedRunner(t *testing.T) {
	tmpDir := t.TempDir()
	wf, err := NewWorkflow(WorkflowOption{
		TargetDir:      tmpDir,
		NydusImagePath: "/usr/bin/nydus-image",
	})
	require.NoError(t, err)

	var runFunc func(*Builder, BuilderOption) error
	patches := gomonkey.ApplyMethod(wf.builder, "Run",
		func(b *Builder, opt BuilderOption) error {
			return runFunc(b, opt)
		})
	defer patches.Reset()

	t.Run("run error", func(t *testing.T) {
		runFunc = func(_ *Builder, _ BuilderOption) error {
			return fmt.Errorf("builder run failed")
		}
		_, err := wf.Build("/tmp/layer", "oci", "", filepath.Join(tmpDir, "bootstrap-err"), false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "build layer")
	})

	t.Run("success no blob generated", func(t *testing.T) {
		subDir := t.TempDir()
		subWf, err := NewWorkflow(WorkflowOption{
			TargetDir:      subDir,
			NydusImagePath: "/usr/bin/nydus-image",
		})
		require.NoError(t, err)

		bootstrapPath := filepath.Join(subDir, "bootstrap")
		runFunc = func(_ *Builder, opt BuilderOption) error {
			output := debugJSON{Version: "1.0", Blobs: []string{}}
			data, _ := json.Marshal(output)
			return os.WriteFile(opt.OutputJSONPath, data, 0644)
		}
		blobPath, err := subWf.Build("/tmp/layer", "oci", "", bootstrapPath, false)
		require.NoError(t, err)
		require.Empty(t, blobPath)
	})

	t.Run("success with blob generated", func(t *testing.T) {
		subDir := t.TempDir()
		subWf, err := NewWorkflow(WorkflowOption{
			TargetDir:      subDir,
			NydusImagePath: "/usr/bin/nydus-image",
		})
		require.NoError(t, err)

		bootstrapPath := filepath.Join(subDir, "bootstrap")
		runFunc = func(_ *Builder, opt BuilderOption) error {
			output := debugJSON{Version: "1.0", Blobs: []string{"abc123"}}
			data, _ := json.Marshal(output)
			if err := os.WriteFile(opt.OutputJSONPath, data, 0644); err != nil {
				return err
			}
			return os.WriteFile(opt.BlobPath, []byte("blob data"), 0644)
		}
		blobPath, err := subWf.Build("/tmp/layer", "oci", "", bootstrapPath, false)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(subDir, "blobs", "abc123"), blobPath)
	})

	t.Run("success with empty blob", func(t *testing.T) {
		subDir := t.TempDir()
		subWf, err := NewWorkflow(WorkflowOption{
			TargetDir:      subDir,
			NydusImagePath: "/usr/bin/nydus-image",
		})
		require.NoError(t, err)

		bootstrapPath := filepath.Join(subDir, "bootstrap")
		runFunc = func(_ *Builder, opt BuilderOption) error {
			output := debugJSON{Version: "1.0", Blobs: []string{"abc123"}}
			data, _ := json.Marshal(output)
			if err := os.WriteFile(opt.OutputJSONPath, data, 0644); err != nil {
				return err
			}
			return os.WriteFile(opt.BlobPath, []byte{}, 0644)
		}
		blobPath, err := subWf.Build("/tmp/layer", "oci", "", bootstrapPath, false)
		require.NoError(t, err)
		require.Empty(t, blobPath)
	})

	t.Run("with parent bootstrap", func(t *testing.T) {
		subDir := t.TempDir()
		subWf, err := NewWorkflow(WorkflowOption{
			TargetDir:      subDir,
			NydusImagePath: "/usr/bin/nydus-image",
		})
		require.NoError(t, err)

		bootstrapPath := filepath.Join(subDir, "bootstrap")
		parentBootstrapPath := filepath.Join(subDir, "parent-bootstrap")
		runFunc = func(_ *Builder, opt BuilderOption) error {
			require.Equal(t, parentBootstrapPath, opt.ParentBootstrapPath)
			output := debugJSON{Version: "1.0", Blobs: []string{}}
			data, _ := json.Marshal(output)
			return os.WriteFile(opt.OutputJSONPath, data, 0644)
		}
		_, err = subWf.Build("/tmp/layer", "oci", parentBootstrapPath, bootstrapPath, false)
		require.NoError(t, err)
		require.Equal(t, bootstrapPath, subWf.parentBootstrapPath)
	})

	t.Run("second layer uses parent", func(t *testing.T) {
		subDir := t.TempDir()
		subWf, err := NewWorkflow(WorkflowOption{
			TargetDir:      subDir,
			NydusImagePath: "/usr/bin/nydus-image",
		})
		require.NoError(t, err)

		callCount := 0
		runFunc = func(_ *Builder, opt BuilderOption) error {
			callCount++
			output := debugJSON{Version: "1.0", Blobs: []string{}}
			data, _ := json.Marshal(output)
			return os.WriteFile(opt.OutputJSONPath, data, 0644)
		}

		bootstrap1 := filepath.Join(subDir, "bootstrap1")
		_, err = subWf.Build("/tmp/layer1", "oci", "", bootstrap1, false)
		require.NoError(t, err)

		bootstrap2 := filepath.Join(subDir, "bootstrap2")
		_, err = subWf.Build("/tmp/layer2", "oci", "", bootstrap2, false)
		require.NoError(t, err)
		require.Equal(t, 2, callCount)
	})
}
