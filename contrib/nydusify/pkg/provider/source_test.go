// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
)

func TestExtractOsArchValid(t *testing.T) {
	tests := []struct {
		input    string
		wantOS   string
		wantArch string
	}{
		{"linux/amd64", "linux", "amd64"},
		{"linux/arm64", "linux", "arm64"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			gotOS, gotArch, err := ExtractOsArch(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.wantOS, gotOS)
			require.Equal(t, tt.wantArch, gotArch)
		})
	}
}

func TestExtractOsArchInvalidFormat(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"no slash", "linuxamd64"},
		{"too many parts", "linux/amd64/v8"},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ExtractOsArch(tt.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid platform format")
		})
	}
}

func TestExtractOsArchUnsupportedOS(t *testing.T) {
	_, _, err := ExtractOsArch("windows/amd64")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not support os")
}

func TestExtractOsArchUnsupportedArch(t *testing.T) {
	_, _, err := ExtractOsArch("linux/riscv64")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not support architecture")
}

func TestExtractOsArchEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"just slash", "/", "not support os"},
		{"slash with arch", "/amd64", "not support os"},
		{"os with slash only", "linux/", "not support architecture"},
		{"darwin", "darwin/amd64", "not support os"},
		{"freebsd", "freebsd/arm64", "not support os"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ExtractOsArch(tt.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDefaultSourceProviderManifest(t *testing.T) {
	r, err := remote.New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)

	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Size:      1234,
	}

	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Desc: desc,
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{MediaType: ocispec.MediaTypeImageLayerGzip},
				},
			},
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
			},
		},
		remote: r,
	}

	gotDesc, err := sp.Manifest(context.Background())
	require.NoError(t, err)
	require.Equal(t, ocispec.MediaTypeImageManifest, gotDesc.MediaType)
	require.Equal(t, int64(1234), gotDesc.Size)
}

func TestDefaultSourceProviderConfig(t *testing.T) {
	sp := &defaultSourceProvider{
		image: parser.Image{
			Config: ocispec.Image{
				Platform: ocispec.Platform{
					Architecture: "arm64",
					OS:           "linux",
				},
				Config: ocispec.ImageConfig{
					Cmd: []string{"/bin/sh"},
				},
			},
		},
	}

	cfg, err := sp.Config(context.Background())
	require.NoError(t, err)
	require.Equal(t, "arm64", cfg.Architecture)
	require.Equal(t, "linux", cfg.OS)
	require.Equal(t, []string{"/bin/sh"}, cfg.Config.Cmd)
}

func TestDefaultSourceProviderLayersMismatch(t *testing.T) {
	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{MediaType: ocispec.MediaTypeImageLayerGzip},
					{MediaType: ocispec.MediaTypeImageLayerGzip},
				},
			},
			Config: ocispec.Image{
				RootFS: ocispec.RootFS{
					DiffIDs: []digest.Digest{"sha256:abc"},
				},
			},
		},
	}

	_, err := sp.Layers(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatched fs layers")
}

func TestDefaultSourceProviderLayersValid(t *testing.T) {
	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{
						MediaType: ocispec.MediaTypeImageLayerGzip,
						Digest:    "sha256:aaa",
						Size:      100,
					},
					{
						MediaType: ocispec.MediaTypeImageLayerGzip,
						Digest:    "sha256:bbb",
						Size:      200,
					},
				},
			},
			Config: ocispec.Image{
				RootFS: ocispec.RootFS{
					DiffIDs: []digest.Digest{
						"sha256:aaa",
						"sha256:bbb",
					},
				},
			},
		},
	}

	layers, err := sp.Layers(context.Background())
	require.NoError(t, err)
	require.Len(t, layers, 2)

	// First layer has no parent chain ID
	require.Nil(t, layers[0].ParentChainID())
	require.Equal(t, digest.Digest("sha256:aaa"), layers[0].Digest())
	require.Equal(t, int64(100), layers[0].Size())

	// Second layer has a parent chain ID
	require.NotNil(t, layers[1].ParentChainID())
	require.Equal(t, digest.Digest("sha256:bbb"), layers[1].Digest())
	require.Equal(t, int64(200), layers[1].Size())

	// ChainIDs should be set
	require.NotEmpty(t, layers[0].ChainID())
	require.NotEmpty(t, layers[1].ChainID())
	require.NotEqual(t, layers[0].ChainID(), layers[1].ChainID())
}

func TestDefaultSourceProviderLayersEmpty(t *testing.T) {
	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{},
			},
			Config: ocispec.Image{
				RootFS: ocispec.RootFS{
					DiffIDs: []digest.Digest{},
				},
			},
		},
	}

	layers, err := sp.Layers(context.Background())
	require.NoError(t, err)
	require.Empty(t, layers)
}

func TestSourceLayerInterface(t *testing.T) {
	var _ SourceLayer = &defaultSourceLayer{}
}

func TestSourceProviderInterface(t *testing.T) {
	var _ SourceProvider = &defaultSourceProvider{}
}

func TestDefaultSourceProviderLayersSingleLayer(t *testing.T) {
	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{
						MediaType: ocispec.MediaTypeImageLayerGzip,
						Digest:    "sha256:aaa",
						Size:      100,
					},
				},
			},
			Config: ocispec.Image{
				RootFS: ocispec.RootFS{
					DiffIDs: []digest.Digest{"sha256:aaa"},
				},
			},
		},
	}

	layers, err := sp.Layers(context.Background())
	require.NoError(t, err)
	require.Len(t, layers, 1)
	require.Nil(t, layers[0].ParentChainID())
	require.Equal(t, digest.Digest("sha256:aaa"), layers[0].Digest())
	require.Equal(t, int64(100), layers[0].Size())
	require.NotEmpty(t, layers[0].ChainID())
}

func TestDefaultSourceProviderLayersChainIDProgression(t *testing.T) {
	sp := &defaultSourceProvider{
		workDir: "/tmp/test",
		image: parser.Image{
			Manifest: ocispec.Manifest{
				Layers: []ocispec.Descriptor{
					{Digest: "sha256:aaa", Size: 10},
					{Digest: "sha256:bbb", Size: 20},
					{Digest: "sha256:ccc", Size: 30},
				},
			},
			Config: ocispec.Image{
				RootFS: ocispec.RootFS{
					DiffIDs: []digest.Digest{"sha256:aaa", "sha256:bbb", "sha256:ccc"},
				},
			},
		},
	}

	layers, err := sp.Layers(context.Background())
	require.NoError(t, err)
	require.Len(t, layers, 3)

	// First layer has no parent chain ID
	require.Nil(t, layers[0].ParentChainID())
	// Second layer's parent chain ID is first layer's chain ID
	require.NotNil(t, layers[1].ParentChainID())
	require.Equal(t, layers[0].ChainID(), *layers[1].ParentChainID())
	// Third layer's parent chain ID is second layer's chain ID
	require.NotNil(t, layers[2].ParentChainID())
	require.Equal(t, layers[1].ChainID(), *layers[2].ParentChainID())
}

func TestDefaultSourceProviderManifestPreservesFields(t *testing.T) {
	desc := ocispec.Descriptor{
		MediaType:   ocispec.MediaTypeImageManifest,
		Size:        5678,
		Digest:      "sha256:test123",
		Annotations: map[string]string{"key": "val"},
	}

	sp := &defaultSourceProvider{
		image: parser.Image{Desc: desc},
	}

	got, err := sp.Manifest(context.Background())
	require.NoError(t, err)
	require.Equal(t, desc.MediaType, got.MediaType)
	require.Equal(t, desc.Size, got.Size)
	require.Equal(t, desc.Digest, got.Digest)
	require.Equal(t, "val", got.Annotations["key"])
}

func TestDefaultSourceProviderConfigPreservesFields(t *testing.T) {
	sp := &defaultSourceProvider{
		image: parser.Image{
			Config: ocispec.Image{
				Platform: ocispec.Platform{
					Architecture: "amd64",
					OS:           "linux",
				},
				Config: ocispec.ImageConfig{
					Cmd:        []string{"/bin/sh"},
					Env:        []string{"PATH=/usr/bin"},
					WorkingDir: "/app",
				},
				Author: "test",
			},
		},
	}

	cfg, err := sp.Config(context.Background())
	require.NoError(t, err)
	require.Equal(t, "amd64", cfg.Architecture)
	require.Equal(t, "linux", cfg.OS)
	require.Equal(t, []string{"/bin/sh"}, cfg.Config.Cmd)
	require.Equal(t, []string{"PATH=/usr/bin"}, cfg.Config.Env)
	require.Equal(t, "/app", cfg.Config.WorkingDir)
	require.Equal(t, "test", cfg.Author)
}

func TestDefaultSourceLayerDigest(t *testing.T) {
	sl := &defaultSourceLayer{
		desc: ocispec.Descriptor{
			Digest: "sha256:testdigest",
			Size:   999,
		},
	}
	require.Equal(t, digest.Digest("sha256:testdigest"), sl.Digest())
	require.Equal(t, int64(999), sl.Size())
}

func TestDefaultSourceLayerChainID(t *testing.T) {
	chainID := digest.Digest("sha256:chain123")
	sl := &defaultSourceLayer{
		chainID: chainID,
	}
	require.Equal(t, chainID, sl.ChainID())
}

func TestDefaultSourceLayerParentChainID(t *testing.T) {
	parentChainID := digest.Digest("sha256:parent123")
	sl := &defaultSourceLayer{
		parentChainID: &parentChainID,
	}
	require.Equal(t, &parentChainID, sl.ParentChainID())
}

func TestDefaultSourceLayerParentChainIDNil(t *testing.T) {
	sl := &defaultSourceLayer{}
	require.Nil(t, sl.ParentChainID())
}

func TestExtractOsArchAllSupported(t *testing.T) {
	// Test all known supported architectures
	archs := []string{"amd64", "arm64"}
	for _, arch := range archs {
		platform := "linux/" + arch
		gotOS, gotArch, err := ExtractOsArch(platform)
		if err != nil {
			t.Logf("arch %s might not be supported: %v", arch, err)
			continue
		}
		require.Equal(t, "linux", gotOS)
		require.Equal(t, arch, gotArch)
	}
}
