// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/containerd/containerd/v2/core/images"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

// Global gomonkey patches applied once in TestMain to avoid repeated
// patch/reset cycles on the same method, which are unreliable on ARM64.
var (
	testPullFunc    func(context.Context, ocispec.Descriptor, bool) (io.ReadCloser, error)
	testResolveFunc func(context.Context) (*ocispec.Descriptor, error)
)

func TestMain(m *testing.M) {
	r := &remote.Remote{}
	pullPatches := gomonkey.ApplyMethod(r, "Pull",
		func(_ *remote.Remote, ctx context.Context, desc ocispec.Descriptor, byDigest bool) (io.ReadCloser, error) {
			return testPullFunc(ctx, desc, byDigest)
		})
	resolvePatches := gomonkey.ApplyMethod(r, "Resolve",
		func(_ *remote.Remote, ctx context.Context) (*ocispec.Descriptor, error) {
			return testResolveFunc(ctx)
		})

	code := m.Run()

	pullPatches.Reset()
	resolvePatches.Reset()
	os.Exit(code)
}

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

// mockPull mocks Remote.Pull to return JSON-serialized data for the given object.
func mockPull(pullFunc *func(context.Context, ocispec.Descriptor, bool) (io.ReadCloser, error), obj interface{}) {
	data, _ := json.Marshal(obj)
	*pullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

type failReader struct{}

func (f *failReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("read error")
}

// TestPullHelpers consolidates tests for pull, pullManifest, pullConfig, pullIndex.
// Uses the global testPullFunc set up in TestMain.
func TestPullHelpers(t *testing.T) {
	r := &remote.Remote{}
	p := &Parser{Remote: r, interestedArch: "amd64"}

	t.Run("pull success", func(t *testing.T) {
		expected := map[string]string{"key": "value"}
		mockPull(&testPullFunc, expected)

		var result map[string]string
		err := p.pull(context.Background(), &ocispec.Descriptor{}, &result)
		require.NoError(t, err)
		require.Equal(t, "value", result["key"])
	})

	t.Run("pull remote error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("connection refused")
		}

		var result map[string]string
		err := p.pull(context.Background(), &ocispec.Descriptor{}, &result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "pull image resource")
	})

	t.Run("pull invalid json", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("not json"))), nil
		}

		var result map[string]string
		err := p.pull(context.Background(), &ocispec.Descriptor{}, &result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal image resource")
	})

	t.Run("pull read error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return io.NopCloser(&failReader{}), nil
		}

		var result map[string]string
		err := p.pull(context.Background(), &ocispec.Descriptor{}, &result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read image resource")
	})

	t.Run("pullManifest success", func(t *testing.T) {
		manifest := ocispec.Manifest{
			MediaType: ocispec.MediaTypeImageManifest,
			Layers:    []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		mockPull(&testPullFunc, manifest)

		result, err := p.pullManifest(context.Background(), &ocispec.Descriptor{})
		require.NoError(t, err)
		require.Equal(t, ocispec.MediaTypeImageManifest, result.MediaType)
		require.Len(t, result.Layers, 1)
	})

	t.Run("pullManifest error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("network error")
		}

		_, err := p.pullManifest(context.Background(), &ocispec.Descriptor{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "pull image manifest")
	})

	t.Run("pullConfig success", func(t *testing.T) {
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		mockPull(&testPullFunc, config)

		result, err := p.pullConfig(context.Background(), &ocispec.Descriptor{})
		require.NoError(t, err)
		require.Equal(t, "amd64", result.Architecture)
		require.Equal(t, "linux", result.OS)
	})

	t.Run("pullConfig error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("network error")
		}

		_, err := p.pullConfig(context.Background(), &ocispec.Descriptor{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "pull image config")
	})

	t.Run("pullIndex success", func(t *testing.T) {
		index := ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		}
		mockPull(&testPullFunc, index)

		result, err := p.pullIndex(context.Background(), &ocispec.Descriptor{})
		require.NoError(t, err)
		require.Len(t, result.Manifests, 2)
	})

	t.Run("pullIndex error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("network error")
		}

		_, err := p.pullIndex(context.Background(), &ocispec.Descriptor{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "pull image index")
	})
}

// TestParseImageCases consolidates parseImage tests.
// Uses the global testPullFunc set up in TestMain.
func TestParseImageCases(t *testing.T) {
	r := &remote.Remote{}
	p := &Parser{Remote: r, interestedArch: "amd64"}

	t.Run("with manifest and config", func(t *testing.T) {
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		configData, _ := json.Marshal(config)
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		manifest := &ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		desc := &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest}

		img, err := p.parseImage(context.Background(), desc, manifest, false)
		require.NoError(t, err)
		require.NotNil(t, img)
		require.Equal(t, "amd64", img.Config.Architecture)
		require.Equal(t, "linux", img.Config.OS)
	})

	t.Run("wrong arch without ignoreArch", func(t *testing.T) {
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "arm64", OS: "linux"},
		}
		configData, _ := json.Marshal(config)
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		manifest := &ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
		}
		desc := &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest}

		_, err := p.parseImage(context.Background(), desc, manifest, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Found arch arm64")

		// With ignoreArch, should succeed
		img, err := p.parseImage(context.Background(), desc, manifest, true)
		require.NoError(t, err)
		require.NotNil(t, img)
	})

	t.Run("missing os and arch", func(t *testing.T) {
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "", OS: ""},
		}
		configData, _ := json.Marshal(config)
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		manifest := &ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
		}
		desc := &ocispec.Descriptor{}

		_, err := p.parseImage(context.Background(), desc, manifest, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not have os or architecture")

		img, err := p.parseImage(context.Background(), desc, manifest, true)
		require.NoError(t, err)
		require.NotNil(t, img)
	})

	t.Run("pull manifest when not provided", func(t *testing.T) {
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)

		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			if callCount == 1 {
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			}
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		desc := &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest}
		img, err := p.parseImage(context.Background(), desc, nil, false)
		require.NoError(t, err)
		require.NotNil(t, img)
		require.Equal(t, 2, callCount)
	})

	t.Run("pull config error", func(t *testing.T) {
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("config pull failed")
		}

		manifest := &ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
		}
		desc := &ocispec.Descriptor{}

		_, err := p.parseImage(context.Background(), desc, manifest, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "pull image config")
	})
}

// TestParseScenarios consolidates all Parse() tests.
// Uses the global testPullFunc and testResolveFunc set up in TestMain.
func TestParseScenarios(t *testing.T) {
	r := &remote.Remote{}
	p := &Parser{Remote: r, interestedArch: "amd64"}

	// Helper to set testResolveFunc for a given media type
	setResolve := func(mediaType string) {
		switch mediaType {
		case "error":
			testResolveFunc = func(_ context.Context) (*ocispec.Descriptor, error) {
				return nil, fmt.Errorf("resolve failed")
			}
		case "x509error":
			testResolveFunc = func(_ context.Context) (*ocispec.Descriptor, error) {
				return nil, fmt.Errorf("x509: certificate signed by unknown authority")
			}
		default:
			testResolveFunc = func(_ context.Context) (*ocispec.Descriptor, error) {
				return &ocispec.Descriptor{MediaType: mediaType}, nil
			}
		}
	}

	t.Run("single OCI manifest", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageManifest)
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{
				{
					MediaType:   ocispec.MediaTypeImageLayerGzip,
					Annotations: map[string]string{utils.LayerAnnotationNydusBlob: "true"},
				},
			},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			if callCount == 1 {
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			}
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.OCIImage)
		require.Nil(t, parsed.NydusImage)
	})

	t.Run("single Nydus manifest", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageManifest)
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{
				{
					MediaType:   ocispec.MediaTypeImageLayerGzip,
					Annotations: map[string]string{utils.LayerAnnotationNydusBootstrap: "true"},
				},
			},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			if callCount == 1 {
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			}
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.Nil(t, parsed.OCIImage)
		require.NotNil(t, parsed.NydusImage)
	})

	t.Run("resolve error", func(t *testing.T) {
		setResolve("error")
		_, err := p.Parse(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "resolve image")
	})

	t.Run("resolve x509 error", func(t *testing.T) {
		setResolve("x509error")
		_, err := p.Parse(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "resolve image")
	})

	t.Run("pull manifest error in parse", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageManifest)
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			return nil, fmt.Errorf("pull manifest failed")
		}

		_, err := p.Parse(context.Background())
		require.Error(t, err)
	})

	t.Run("index with OCI image", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageIndex)
		index := ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{
					MediaType: ocispec.MediaTypeImageManifest,
					Platform:  &ocispec.Platform{OS: "linux", Architecture: "amd64"},
				},
			},
		}
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		indexData, _ := json.Marshal(index)
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			switch callCount {
			case 1:
				return io.NopCloser(bytes.NewReader(indexData)), nil
			case 2:
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			default:
				return io.NopCloser(bytes.NewReader(configData)), nil
			}
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.Index)
		require.NotNil(t, parsed.OCIImage)
	})

	t.Run("index with nydus artifact type", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageIndex)
		index := ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{
					MediaType:    ocispec.MediaTypeImageManifest,
					ArtifactType: utils.ArtifactTypeNydusImageManifest,
					Platform:     &ocispec.Platform{OS: "linux", Architecture: "amd64"},
				},
			},
		}
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{
				{
					MediaType:   ocispec.MediaTypeImageLayerGzip,
					Annotations: map[string]string{utils.LayerAnnotationNydusBootstrap: "true"},
				},
			},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		indexData, _ := json.Marshal(index)
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			switch callCount {
			case 1:
				return io.NopCloser(bytes.NewReader(indexData)), nil
			case 2:
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			default:
				return io.NopCloser(bytes.NewReader(configData)), nil
			}
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.NydusImage)
	})

	t.Run("index with no platform", func(t *testing.T) {
		setResolve(ocispec.MediaTypeImageIndex)
		index := ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		}
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		indexData, _ := json.Marshal(index)
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			switch callCount {
			case 1:
				return io.NopCloser(bytes.NewReader(indexData)), nil
			case 2:
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			default:
				return io.NopCloser(bytes.NewReader(configData)), nil
			}
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.OCIImage)
	})

	t.Run("docker schema2 manifest", func(t *testing.T) {
		setResolve(images.MediaTypeDockerSchema2Manifest)
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			if callCount == 1 {
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			}
			return io.NopCloser(bytes.NewReader(configData)), nil
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.OCIImage)
	})

	t.Run("docker schema2 manifest list", func(t *testing.T) {
		setResolve(images.MediaTypeDockerSchema2ManifestList)
		index := ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{
					MediaType: ocispec.MediaTypeImageManifest,
					Platform:  &ocispec.Platform{OS: "linux", Architecture: "amd64"},
				},
			},
		}
		manifest := ocispec.Manifest{
			Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig},
			Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip}},
		}
		config := ocispec.Image{
			Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		}
		indexData, _ := json.Marshal(index)
		manifestData, _ := json.Marshal(manifest)
		configData, _ := json.Marshal(config)
		callCount := 0
		testPullFunc = func(_ context.Context, _ ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			callCount++
			switch callCount {
			case 1:
				return io.NopCloser(bytes.NewReader(indexData)), nil
			case 2:
				return io.NopCloser(bytes.NewReader(manifestData)), nil
			default:
				return io.NopCloser(bytes.NewReader(configData)), nil
			}
		}

		parsed, err := p.Parse(context.Background())
		require.NoError(t, err)
		require.NotNil(t, parsed)
		require.NotNil(t, parsed.Index)
	})
}
