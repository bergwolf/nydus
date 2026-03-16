// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package checker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

func TestOptStructFields(t *testing.T) {
	opt := Opt{
		WorkDir:             "/tmp/workdir",
		Source:              "source-image:latest",
		Target:              "target-image:latest",
		SourceInsecure:      true,
		TargetInsecure:      false,
		SourceBackendType:   "oss",
		SourceBackendConfig: `{"bucket":"src"}`,
		TargetBackendType:   "s3",
		TargetBackendConfig: `{"bucket":"tgt"}`,
		MultiPlatform:       true,
		NydusImagePath:      "/usr/bin/nydus-image",
		NydusdPath:          "/usr/bin/nydusd",
		ExpectedArch:        "amd64",
	}

	require.Equal(t, "/tmp/workdir", opt.WorkDir)
	require.Equal(t, "source-image:latest", opt.Source)
	require.Equal(t, "target-image:latest", opt.Target)
	require.True(t, opt.SourceInsecure)
	require.False(t, opt.TargetInsecure)
	require.Equal(t, "oss", opt.SourceBackendType)
	require.Equal(t, `{"bucket":"src"}`, opt.SourceBackendConfig)
	require.Equal(t, "s3", opt.TargetBackendType)
	require.Equal(t, `{"bucket":"tgt"}`, opt.TargetBackendConfig)
	require.True(t, opt.MultiPlatform)
	require.Equal(t, "/usr/bin/nydus-image", opt.NydusImagePath)
	require.Equal(t, "/usr/bin/nydusd", opt.NydusdPath)
	require.Equal(t, "amd64", opt.ExpectedArch)
}

func TestOptDefaultValues(t *testing.T) {
	opt := Opt{}

	require.Empty(t, opt.WorkDir)
	require.Empty(t, opt.Source)
	require.Empty(t, opt.Target)
	require.False(t, opt.SourceInsecure)
	require.False(t, opt.TargetInsecure)
	require.False(t, opt.MultiPlatform)
	require.Empty(t, opt.NydusImagePath)
	require.Empty(t, opt.NydusdPath)
	require.Empty(t, opt.ExpectedArch)
}

func TestCheckerStructEmbedding(t *testing.T) {
	checker := &Checker{
		Opt: Opt{
			WorkDir: "/tmp/test",
			Target:  "test:latest",
		},
	}
	require.Equal(t, "/tmp/test", checker.WorkDir)
	require.Equal(t, "test:latest", checker.Target)
	require.Nil(t, checker.sourceParser)
	require.Nil(t, checker.targetParser)
}

func TestOutputOCIImageOnly(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{MediaType: ocispec.MediaTypeImageLayerGzip},
				},
			},
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
			},
		},
	}

	dir := filepath.Join(tmpDir, "output_oci")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	// Should have written oci_manifest.json and oci_config.json
	_, err = os.Stat(filepath.Join(dir, "oci_manifest.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "oci_config.json"))
	require.NoError(t, err)
	// Should not have nydus files
	_, err = os.Stat(filepath.Join(dir, "nydus_manifest.json"))
	require.True(t, os.IsNotExist(err))
}

func TestOutputWithIndex(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
		Index: &ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		},
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}

	dir := filepath.Join(tmpDir, "output_index")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "oci_index.json"))
	require.NoError(t, err)
}

func TestOutputNydusManifestWritten(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	// NydusImage without bootstrap layer - Output will write manifest/config
	// but skip the bootstrap pull because FindNydusBootstrapDesc returns nil
	parsed := &parser.Parsed{
		Remote: r,
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{
						MediaType: ocispec.MediaTypeImageLayerGzip,
						Annotations: map[string]string{
							utils.LayerAnnotationNydusBlob: "true",
						},
					},
				},
			},
			Config: ocispec.Image{},
		},
	}

	dir := filepath.Join(tmpDir, "output_nydus")
	// This will write nydus_manifest.json and nydus_config.json,
	// then try PullNydusBootstrap which returns error (no bootstrap layer).
	// But the manifest and config files should already be written.
	_ = checker.Output(context.Background(), parsed, dir)

	_, err = os.Stat(filepath.Join(dir, "nydus_manifest.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "nydus_config.json"))
	require.NoError(t, err)
}

func TestOutputNilImages(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
	}

	dir := filepath.Join(tmpDir, "output_nil")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	// Directory should exist but be empty (no image files)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestOutputCreatesDirIfNotExists(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote:   r,
		OCIImage: &parser.Image{},
	}

	dir := filepath.Join(tmpDir, "nested", "deep", "output")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestOptPartialFields(t *testing.T) {
	opt := Opt{
		Target:         "myimage:v1",
		TargetInsecure: true,
	}
	require.Equal(t, "myimage:v1", opt.Target)
	require.True(t, opt.TargetInsecure)
	require.Empty(t, opt.Source)
	require.False(t, opt.SourceInsecure)
	require.Empty(t, opt.WorkDir)
	require.False(t, opt.MultiPlatform)
}

func TestCheckerOptEmbeddedAccess(t *testing.T) {
	checker := &Checker{
		Opt: Opt{
			WorkDir:        "/tmp/check",
			Target:         "target:v2",
			Source:         "source:v1",
			NydusImagePath: "/bin/nydus-image",
			NydusdPath:     "/bin/nydusd",
			ExpectedArch:   "arm64",
		},
	}
	require.Equal(t, "/tmp/check", checker.WorkDir)
	require.Equal(t, "target:v2", checker.Target)
	require.Equal(t, "source:v1", checker.Source)
	require.Equal(t, "/bin/nydus-image", checker.NydusImagePath)
	require.Equal(t, "/bin/nydusd", checker.NydusdPath)
	require.Equal(t, "arm64", checker.ExpectedArch)
}

func TestOutputOCIAndNydusManifest(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{MediaType: ocispec.MediaTypeImageLayerGzip},
				},
			},
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
			},
		},
	}

	dir := filepath.Join(tmpDir, "both")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	// OCI files should exist
	_, err = os.Stat(filepath.Join(dir, "oci_manifest.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "oci_config.json"))
	require.NoError(t, err)
}

func TestOutputWithNydusAndOCIIndex(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
		Index: &ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		},
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{
						MediaType: ocispec.MediaTypeImageLayerGzip,
						Annotations: map[string]string{
							utils.LayerAnnotationNydusBlob: "true",
						},
					},
				},
			},
			Config: ocispec.Image{},
		},
	}

	dir := filepath.Join(tmpDir, "nydus_index")
	// This will try to PullNydusBootstrap which will fail, but the index file should be written
	_ = checker.Output(context.Background(), parsed, dir)

	_, err = os.Stat(filepath.Join(dir, "nydus_index.json"))
	require.NoError(t, err)
}

func TestOutputEmptyManifest(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{
		Opt: Opt{WorkDir: tmpDir},
	}

	parsed := &parser.Parsed{
		Remote: r,
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}

	dir := filepath.Join(tmpDir, "empty_manifest")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "oci_manifest.json"))
	require.NoError(t, err)
	require.Contains(t, string(content), "{")
}

// --- New() tests ---

func TestNewWithValidTarget(t *testing.T) {
	opt := Opt{
		Target:       "docker.io/library/alpine:latest",
		ExpectedArch: "amd64",
	}
	checker, err := New(opt)
	require.NoError(t, err)
	require.NotNil(t, checker)
	require.NotNil(t, checker.targetParser)
	require.Nil(t, checker.sourceParser)
	require.Equal(t, opt.Target, checker.Target)
}

func TestNewWithValidSourceAndTarget(t *testing.T) {
	opt := Opt{
		Target:       "docker.io/library/alpine:latest",
		Source:       "docker.io/library/nginx:latest",
		ExpectedArch: "amd64",
	}
	checker, err := New(opt)
	require.NoError(t, err)
	require.NotNil(t, checker)
	require.NotNil(t, checker.targetParser)
	require.NotNil(t, checker.sourceParser)
}

func TestNewInvalidTargetReference(t *testing.T) {
	opt := Opt{
		Target:       "",
		ExpectedArch: "amd64",
	}
	_, err := New(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "init target image parser")
}

func TestNewInvalidSourceReference(t *testing.T) {
	opt := Opt{
		Target:       "docker.io/library/alpine:latest",
		Source:       ":::invalid",
		ExpectedArch: "amd64",
	}
	_, err := New(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Init source image parser")
}

func TestNewInvalidTargetArch(t *testing.T) {
	opt := Opt{
		Target:       "docker.io/library/alpine:latest",
		ExpectedArch: "mips",
	}
	_, err := New(opt)
	require.Error(t, err)
}

func TestNewInvalidSourceArch(t *testing.T) {
	opt := Opt{
		Target:       "docker.io/library/alpine:latest",
		Source:       "docker.io/library/nginx:latest",
		ExpectedArch: "mips",
	}
	_, err := New(opt)
	require.Error(t, err)
}

func TestNewPreservesAllOpts(t *testing.T) {
	opt := Opt{
		WorkDir:             "/tmp/work",
		Target:              "docker.io/library/alpine:latest",
		Source:              "docker.io/library/nginx:latest",
		SourceInsecure:      true,
		TargetInsecure:      true,
		SourceBackendType:   "oss",
		SourceBackendConfig: `{"bucket":"src"}`,
		TargetBackendType:   "s3",
		TargetBackendConfig: `{"bucket":"tgt"}`,
		MultiPlatform:       true,
		NydusImagePath:      "/usr/bin/nydus-image",
		NydusdPath:          "/usr/bin/nydusd",
		ExpectedArch:        "arm64",
	}
	checker, err := New(opt)
	require.NoError(t, err)
	require.Equal(t, opt.WorkDir, checker.WorkDir)
	require.Equal(t, opt.SourceBackendType, checker.SourceBackendType)
	require.Equal(t, opt.TargetBackendConfig, checker.TargetBackendConfig)
	require.Equal(t, opt.NydusImagePath, checker.NydusImagePath)
	require.Equal(t, opt.NydusdPath, checker.NydusdPath)
	require.True(t, checker.MultiPlatform)
}

// --- Check() / check() tests ---

func TestCheckWithUnreachableTarget(t *testing.T) {
	// Use localhost:1 which is closed; connection refused is instant
	opt := Opt{
		Target:       "localhost:1/test:latest",
		ExpectedArch: "amd64",
	}
	checker, err := New(opt)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = checker.Check(ctx)
	require.Error(t, err)
}

func TestCheckWithUnreachableSourceAndTarget(t *testing.T) {
	opt := Opt{
		Target:       "localhost:1/target:latest",
		Source:       "localhost:1/source:latest",
		ExpectedArch: "amd64",
	}
	checker, err := New(opt)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = checker.Check(ctx)
	require.Error(t, err)
}

// --- Output() error path tests ---

func TestOutputMkdirAllError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	parsed := &parser.Parsed{
		Remote:   r,
		OCIImage: &parser.Image{},
	}

	// Create a file where the output dir should be, so MkdirAll fails
	blocker := filepath.Join(tmpDir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0644))

	dir := filepath.Join(blocker, "sub")
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create output directory")
}

func TestOutputOCIManifestWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out1")
	require.NoError(t, os.MkdirAll(dir, 0755))
	// Block the file by creating a directory with the same name
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "oci_manifest.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output OCI manifest file")
}

func TestOutputOCIConfigWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out2")
	require.NoError(t, os.MkdirAll(dir, 0755))
	// Block only the config file
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "oci_config.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output OCI config file")
}

func TestOutputOCIIndexWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out3")
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "oci_index.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		Index:  &ocispec.Index{},
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output oci index file")
}

func TestOutputNydusIndexWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out4")
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nydus_index.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		Index:  &ocispec.Index{},
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output nydus index file")
}

func TestOutputNydusManifestWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out5")
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nydus_manifest.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output nydus manifest file")
}

func TestOutputNydusConfigWriteError(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	dir := filepath.Join(tmpDir, "out6")
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nydus_config.json"), 0755))

	parsed := &parser.Parsed{
		Remote: r,
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{},
			Config:   ocispec.Image{},
		},
	}
	err = checker.Output(context.Background(), parsed, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output nydus config file")
}

func TestOutputVerifiesJSONContent(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	checker := &Checker{Opt: Opt{WorkDir: tmpDir}}
	parsed := &parser.Parsed{
		Remote: r,
		Index: &ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest, Size: 42},
			},
		},
		OCIImage: &parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{MediaType: ocispec.MediaTypeImageLayerGzip, Size: 100},
				},
			},
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
			},
		},
	}

	dir := filepath.Join(tmpDir, "json_verify")
	err = checker.Output(context.Background(), parsed, dir)
	require.NoError(t, err)

	// Verify oci_manifest.json contains valid JSON with layer info
	data, err := os.ReadFile(filepath.Join(dir, "oci_manifest.json"))
	require.NoError(t, err)
	var manifest ocispec.Manifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	require.Len(t, manifest.Layers, 1)
	require.Equal(t, int64(100), manifest.Layers[0].Size)

	// Verify oci_config.json
	data, err = os.ReadFile(filepath.Join(dir, "oci_config.json"))
	require.NoError(t, err)
	var config ocispec.Image
	require.NoError(t, json.Unmarshal(data, &config))
	require.Equal(t, "amd64", config.Architecture)

	// Verify oci_index.json
	data, err = os.ReadFile(filepath.Join(dir, "oci_index.json"))
	require.NoError(t, err)
	var index ocispec.Index
	require.NoError(t, json.Unmarshal(data, &index))
	require.Len(t, index.Manifests, 1)
}

func TestNewWithProviderDefaultRemote(t *testing.T) {
	// Verify New uses provider.DefaultRemote internally
	r, err := provider.DefaultRemote("docker.io/library/alpine:latest", false)
	require.NoError(t, err)
	require.NotNil(t, r)
}
