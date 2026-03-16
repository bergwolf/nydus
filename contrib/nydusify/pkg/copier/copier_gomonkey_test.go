// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package copier

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	accelcontent "github.com/goharbor/acceleration-service/pkg/content"
	"github.com/goharbor/acceleration-service/pkg/remote"
	"github.com/goharbor/acceleration-service/pkg/utils"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/backend"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/checker/tool"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/converter/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	nydusifyUtils "github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

func TestPushBlobFromBackendUnsupportedMediaType(t *testing.T) {
	ctx := context.Background()
	src := ocispec.Descriptor{
		MediaType: "application/unsupported",
	}
	opt := Opt{}

	_, _, err := pushBlobFromBackend(ctx, nil, nil, src, opt)
	if err == nil {
		t.Fatal("expected error for unsupported media type")
	}
	if got := err.Error(); got != fmt.Sprintf("unsupported media type %s", "application/unsupported") {
		t.Errorf("error = %q", got)
	}
}

func TestPushBlobFromBackendOCIManifest(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a mock content store that returns content
	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir: tmpDir,
	}

	// Patch ReadJSON to return an error
	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, fmt.Errorf("read manifest failed")
	})
	defer patches.Reset()

	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from ReadJSON")
	}
}

func TestPushBlobFromBackendDockerManifest(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: images.MediaTypeDockerSchema2Manifest,
		Digest:    digest.FromString("docker-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir: tmpDir,
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, fmt.Errorf("read manifest failed")
	})
	defer patches.Reset()

	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from ReadJSON")
	}
}

func TestPushBlobFromBackendNoBootstrap(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir: tmpDir,
	}

	// Patch ReadJSON to return a manifest with no bootstrap
	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		if manifest, ok := x.(*ocispec.Manifest); ok {
			manifest.Layers = []ocispec.Descriptor{
				{
					MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
					Digest:    digest.FromString("regular-layer"),
					Size:      500,
				},
			}
		}
		return nil, nil
	})
	defer patches.Reset()

	// Patch FindNydusBootstrapDesc to return nil
	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return nil
	})
	defer patches2.Reset()

	descs, target, err := pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err != nil {
		t.Fatalf("expected no error when bootstrap not found, got: %v", err)
	}
	if descs != nil {
		t.Error("expected nil descs when no bootstrap")
	}
	if target != nil {
		t.Error("expected nil target when no bootstrap")
	}
}

func TestPushBlobFromBackendReaderAtError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	bootstrapDigest := digest.FromString("bootstrap")
	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir: tmpDir,
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		if manifest, ok := x.(*ocispec.Manifest); ok {
			manifest.Layers = []ocispec.Descriptor{
				{
					MediaType: ocispec.MediaTypeImageLayerGzip,
					Digest:    bootstrapDigest,
					Size:      500,
					Annotations: map[string]string{
						"containerd.io/snapshot/nydus-bootstrap": "true",
					},
				},
			}
		}
		return nil, nil
	})
	defer patches.Reset()

	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	// The ReaderAt call on the content store will fail because the content doesn't exist
	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from ReaderAt")
	}
}

func TestPushBlobFromBackendUnpackError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	bootstrapDigest := digest.FromString("bootstrap")
	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir: tmpDir,
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()

	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	// Patch UnpackFile to return error
	patches3 := gomonkey.ApplyFunc(nydusifyUtils.UnpackFile, func(r interface{}, name, target string) error {
		return fmt.Errorf("unpack failed")
	})
	defer patches3.Reset()

	// We still need the ReaderAt to not fail - but it will since content doesn't exist.
	// So this test the ReaderAt error path.
	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPushBlobFromBackendBuilderCheckError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir:        tmpDir,
		NydusImagePath: "/nonexistent/nydus-image",
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()

	bootstrapDigest := digest.FromString("bootstrap")
	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyFunc(nydusifyUtils.UnpackFile, func(r interface{}, name, target string) error {
		// Create a dummy bootstrap file
		return os.WriteFile(target, []byte("fake bootstrap"), 0644)
	})
	defer patches3.Reset()

	// Builder.Check will fail because /nonexistent/nydus-image doesn't exist
	patches4 := gomonkey.ApplyMethodFunc(&tool.Builder{}, "Check", func(option tool.BuilderOption) error {
		return fmt.Errorf("builder check failed")
	})
	defer patches4.Reset()

	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from builder check")
	}
}

func TestPushBlobFromBackendOutputReadError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir:        tmpDir,
		NydusImagePath: "/usr/bin/true",
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()

	bootstrapDigest := digest.FromString("bootstrap")
	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyFunc(nydusifyUtils.UnpackFile, func(r interface{}, name, target string) error {
		return os.WriteFile(target, []byte("fake bootstrap"), 0644)
	})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethodFunc(&tool.Builder{}, "Check", func(option tool.BuilderOption) error {
		// Don't create the output.json file - this will cause ReadFile to fail
		return nil
	})
	defer patches4.Reset()

	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from reading output file")
	}
}

func TestPushBlobFromBackendInvalidOutputJSON(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir:        tmpDir,
		NydusImagePath: "/usr/bin/true",
	}

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()

	bootstrapDigest := digest.FromString("bootstrap")
	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyFunc(nydusifyUtils.UnpackFile, func(r interface{}, name, target string) error {
		return os.WriteFile(target, []byte("fake bootstrap"), 0644)
	})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethodFunc(&tool.Builder{}, "Check", func(option tool.BuilderOption) error {
		// Create invalid JSON output
		outputPath := filepath.Join(tmpDir, "output.json")
		return os.WriteFile(outputPath, []byte("{invalid json}"), 0644)
	})
	defer patches4.Reset()

	_, _, err = pushBlobFromBackend(ctx, pvd, nil, src, opt)
	if err == nil {
		t.Fatal("expected error from invalid output JSON")
	}
}

func TestPushBlobFromBackendWithBlobsBackendSizeError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	baseStore, err := accelcontent.NewContent(
		func(ref string) (remote.CredentialFunc, bool, error) {
			return nil, false, nil
		},
		filepath.Join(tmpDir, "content"),
		tmpDir,
		"0MB",
	)
	if err != nil {
		t.Fatalf("create content store: %v", err)
	}

	pvd, err := provider.New(tmpDir, func(ref string) (remote.CredentialFunc, bool, error) {
		return nil, false, nil
	}, 200, "v1", nil, 0, baseStore)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	src := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("test-manifest"),
		Size:      100,
	}

	opt := Opt{
		WorkDir:        tmpDir,
		NydusImagePath: "/usr/bin/true",
	}

	// Create valid output.json with blobs
	outputJSON := output{
		Blobs: []string{"abc123", "def456", "abc123"}, // abc123 appears twice for dedup testing
	}
	outputBytes, _ := json.Marshal(outputJSON)

	patches := gomonkey.ApplyFunc(utils.ReadJSON, func(ctx context.Context, cs content.Store, x interface{}, desc ocispec.Descriptor) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()

	bootstrapDigest := digest.FromString("bootstrap")
	patches2 := gomonkey.ApplyFunc(parser.FindNydusBootstrapDesc, func(manifest *ocispec.Manifest) *ocispec.Descriptor {
		return &ocispec.Descriptor{
			Digest: bootstrapDigest,
			Size:   500,
		}
	})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyFunc(nydusifyUtils.UnpackFile, func(r interface{}, name, target string) error {
		return os.WriteFile(target, []byte("fake bootstrap"), 0644)
	})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethodFunc(&tool.Builder{}, "Check", func(option tool.BuilderOption) error {
		outputPath := filepath.Join(tmpDir, "output.json")
		return os.WriteFile(outputPath, outputBytes, 0644)
	})
	defer patches4.Reset()

	// Create a mock backend that returns error on Size
	bkd := &sizeErrorBackend{}

	_, _, err = pushBlobFromBackend(ctx, pvd, bkd, src, opt)
	if err == nil {
		t.Fatal("expected error from backend Size()")
	}
}

// sizeErrorBackend implements backend.Backend and returns error on Size
type sizeErrorBackend struct{}

func (b *sizeErrorBackend) Upload(ctx context.Context, blobID, blobPath string, blobSize int64, forcePush bool) (*ocispec.Descriptor, error) {
	return nil, nil
}
func (b *sizeErrorBackend) Finalize(cancel bool) error { return nil }
func (b *sizeErrorBackend) Check(blobID string) (bool, error) {
	return false, nil
}
func (b *sizeErrorBackend) Type() backend.Type { return 0 }
func (b *sizeErrorBackend) Reader(blobID string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (b *sizeErrorBackend) RangeReader(blobID string) (remotes.RangeReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (b *sizeErrorBackend) Size(blobID string) (int64, error) {
	return 0, fmt.Errorf("backend size error")
}
