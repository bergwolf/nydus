// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"context"
	"testing"

	"github.com/containerd/containerd/v2/core/images"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

func TestFindNydusBootstrapDescFound(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
		},
	}

	desc := FindNydusBootstrapDesc(manifest)
	require.NotNil(t, desc)
	require.Equal(t, "true", desc.Annotations[utils.LayerAnnotationNydusBootstrap])
}

func TestFindNydusBootstrapDescDockerMediaType(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: images.MediaTypeDockerSchema2LayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
		},
	}

	desc := FindNydusBootstrapDesc(manifest)
	require.NotNil(t, desc)
}

func TestFindNydusBootstrapDescNotFound(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
		},
	}

	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescEmptyLayers(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescNilLayers(t *testing.T) {
	manifest := &ocispec.Manifest{}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescWrongMediaType(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageLayer,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescOnlyChecksTopmost(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
		},
	}
	// The topmost layer (last) is a blob, not a bootstrap
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestNewParserValid(t *testing.T) {
	tests := []struct {
		arch string
	}{
		{"amd64"},
		{"arm64"},
	}

	for _, tt := range tests {
		t.Run(tt.arch, func(t *testing.T) {
			p, err := New(nil, tt.arch)
			require.NoError(t, err)
			require.NotNil(t, p)
			require.Equal(t, tt.arch, p.interestedArch)
		})
	}
}

func TestNewParserInvalidArch(t *testing.T) {
	tests := []struct {
		arch string
	}{
		{"riscv64"},
		{"ppc64le"},
		{"s390x"},
		{"unsupported"},
		{""},
	}

	for _, tt := range tests {
		t.Run(tt.arch, func(t *testing.T) {
			p, err := New(nil, tt.arch)
			require.Error(t, err)
			require.Nil(t, p)
			require.Contains(t, err.Error(), "invalid arch")
		})
	}
}

func TestImageStruct(t *testing.T) {
	img := Image{
		Desc: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      1234,
		},
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageLayerGzip},
			},
		},
		Config: ocispec.Image{
			Platform: ocispec.Platform{
				Architecture: "amd64",
				OS:           "linux",
			},
		},
	}

	require.Equal(t, ocispec.MediaTypeImageManifest, img.Desc.MediaType)
	require.Equal(t, int64(1234), img.Desc.Size)
	require.Len(t, img.Manifest.Layers, 1)
	require.Equal(t, "amd64", img.Config.Architecture)
	require.Equal(t, "linux", img.Config.OS)
}

func TestParsedStruct(t *testing.T) {
	parsed := Parsed{
		Index: &ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		},
		OCIImage: &Image{
			Config: ocispec.Image{Platform: ocispec.Platform{Architecture: "amd64"}},
		},
		NydusImage: &Image{
			Config: ocispec.Image{Platform: ocispec.Platform{Architecture: "amd64"}},
		},
	}

	require.NotNil(t, parsed.Index)
	require.Len(t, parsed.Index.Manifests, 1)
	require.NotNil(t, parsed.OCIImage)
	require.NotNil(t, parsed.NydusImage)
	require.Equal(t, "amd64", parsed.OCIImage.Config.Architecture)
}

func TestParsedStructNilFields(t *testing.T) {
	parsed := Parsed{}

	require.Nil(t, parsed.Remote)
	require.Nil(t, parsed.Index)
	require.Nil(t, parsed.OCIImage)
	require.Nil(t, parsed.NydusImage)
}

func TestMatchImagePlatform(t *testing.T) {
	p := &Parser{interestedArch: "amd64"}

	tests := []struct {
		name   string
		desc   *ocispec.Descriptor
		expect bool
	}{
		{
			"matching platform",
			&ocispec.Descriptor{
				Platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"},
			},
			true,
		},
		{
			"wrong arch",
			&ocispec.Descriptor{
				Platform: &ocispec.Platform{OS: "linux", Architecture: "arm64"},
			},
			false,
		},
		{
			"wrong os",
			&ocispec.Descriptor{
				Platform: &ocispec.Platform{OS: "windows", Architecture: "amd64"},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := p.matchImagePlatform(tt.desc)
			require.Equal(t, tt.expect, result)
		})
	}
}

func TestMatchImagePlatformArm64(t *testing.T) {
	p := &Parser{interestedArch: "arm64"}

	desc := &ocispec.Descriptor{
		Platform: &ocispec.Platform{OS: "linux", Architecture: "arm64"},
	}
	require.True(t, p.matchImagePlatform(desc))

	desc2 := &ocispec.Descriptor{
		Platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	require.False(t, p.matchImagePlatform(desc2))
}

func TestNewParserSetsRemote(t *testing.T) {
	r := &remote.Remote{Ref: "docker.io/library/alpine:latest"}
	p, err := New(r, "amd64")
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, r, p.Remote)
	require.Equal(t, "amd64", p.interestedArch)
}

func TestFindNydusBootstrapDescMissingAnnotation(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType:   ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescAnnotationFalse(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "false",
				},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescNilAnnotations(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType:   ocispec.MediaTypeImageLayerGzip,
				Annotations: nil,
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc)
}

func TestFindNydusBootstrapDescMultipleLayers(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: utils.MediaTypeNydusBlob,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
			{
				MediaType: utils.MediaTypeNydusBlob,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
			{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.NotNil(t, desc)
	require.Equal(t, "true", desc.Annotations[utils.LayerAnnotationNydusBootstrap])
}

func TestPullNydusBootstrapNoBootstrapLayer(t *testing.T) {
	r := &remote.Remote{Ref: "docker.io/library/alpine:latest"}
	p, err := New(r, "amd64")
	require.NoError(t, err)

	img := &Image{
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
	}

	_, err = p.PullNydusBootstrap(context.Background(), img)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found Nydus bootstrap layer")
}

func TestParsedRemoteField(t *testing.T) {
	r := &remote.Remote{Ref: "test:v1"}
	parsed := Parsed{
		Remote: r,
	}
	require.Equal(t, "test:v1", parsed.Remote.Ref)
}

func TestNewParserNilRemote(t *testing.T) {
	p, err := New(nil, "amd64")
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Nil(t, p.Remote)
}

func TestMatchImagePlatformEmptyFields(t *testing.T) {
	p := &Parser{interestedArch: "amd64"}

	desc := &ocispec.Descriptor{
		Platform: &ocispec.Platform{OS: "", Architecture: "amd64"},
	}
	require.False(t, p.matchImagePlatform(desc))

	desc2 := &ocispec.Descriptor{
		Platform: &ocispec.Platform{OS: "linux", Architecture: ""},
	}
	require.False(t, p.matchImagePlatform(desc2))

	desc3 := &ocispec.Descriptor{
		Platform: &ocispec.Platform{OS: "", Architecture: ""},
	}
	require.False(t, p.matchImagePlatform(desc3))
}

func TestMatchImagePlatformWithVariant(t *testing.T) {
	p := &Parser{interestedArch: "arm64"}

	desc := &ocispec.Descriptor{
		Platform: &ocispec.Platform{
			OS:           "linux",
			Architecture: "arm64",
			Variant:      "v8",
		},
	}
	require.True(t, p.matchImagePlatform(desc))
}

func TestFindNydusBootstrapDescSingleBlobLayer(t *testing.T) {
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: utils.MediaTypeNydusBlob,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.Nil(t, desc, "blob layers should not be detected as bootstrap")
}

func TestImageStructDefaults(t *testing.T) {
	img := Image{}
	require.Empty(t, img.Desc.MediaType)
	require.Nil(t, img.Manifest.Layers)
	require.Empty(t, img.Config.Architecture)
}

func TestParsedWithOnlyOCIImage(t *testing.T) {
	parsed := Parsed{
		OCIImage: &Image{
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
			},
		},
	}
	require.NotNil(t, parsed.OCIImage)
	require.Nil(t, parsed.NydusImage)
	require.Nil(t, parsed.Index)
}

func TestParsedWithOnlyNydusImage(t *testing.T) {
	parsed := Parsed{
		NydusImage: &Image{
			Config: ocispec.Image{
				Platform: ocispec.Platform{Architecture: "arm64", OS: "linux"},
			},
		},
	}
	require.Nil(t, parsed.OCIImage)
	require.NotNil(t, parsed.NydusImage)
	require.Equal(t, "arm64", parsed.NydusImage.Config.Architecture)
}

func TestFindNydusBootstrapDescWithMixedLayerTypes(t *testing.T) {
	// Last layer is bootstrap with docker media type
	manifest := &ocispec.Manifest{
		Layers: []ocispec.Descriptor{
			{
				MediaType: utils.MediaTypeNydusBlob,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
			{
				MediaType: utils.MediaTypeNydusBlob,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBlob: "true",
				},
			},
			{
				MediaType: images.MediaTypeDockerSchema2LayerGzip,
				Annotations: map[string]string{
					utils.LayerAnnotationNydusBootstrap: "true",
				},
			},
		},
	}
	desc := FindNydusBootstrapDesc(manifest)
	require.NotNil(t, desc)
	require.Equal(t, images.MediaTypeDockerSchema2LayerGzip, desc.MediaType)
}

func TestPullNydusBootstrapNoLayers(t *testing.T) {
	r := &remote.Remote{Ref: "docker.io/library/alpine:latest"}
	p, err := New(r, "amd64")
	require.NoError(t, err)

	img := &Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{},
		},
	}

	_, err = p.PullNydusBootstrap(context.Background(), img)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found Nydus bootstrap layer")
}

func TestImageWithMultipleLayersAndConfig(t *testing.T) {
	img := Image{
		Desc: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      5678,
		},
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				{MediaType: utils.MediaTypeNydusBlob, Size: 100},
				{MediaType: utils.MediaTypeNydusBlob, Size: 200},
				{MediaType: ocispec.MediaTypeImageLayerGzip, Size: 300},
			},
		},
		Config: ocispec.Image{
			Platform: ocispec.Platform{
				Architecture: "amd64",
				OS:           "linux",
			},
			RootFS: ocispec.RootFS{
				Type: "layers",
			},
		},
	}

	require.Len(t, img.Manifest.Layers, 3)
	require.Equal(t, int64(5678), img.Desc.Size)
	require.Equal(t, "layers", img.Config.RootFS.Type)
}

func TestParserInterestedArchStored(t *testing.T) {
	tests := []struct {
		arch string
	}{
		{"amd64"},
		{"arm64"},
	}
	for _, tt := range tests {
		t.Run(tt.arch, func(t *testing.T) {
			p, err := New(nil, tt.arch)
			require.NoError(t, err)
			require.Equal(t, tt.arch, p.interestedArch)
		})
	}
}
