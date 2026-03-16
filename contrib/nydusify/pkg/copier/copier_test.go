// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package copier

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// --- Mock content.Store ---

type mockContentStore struct {
	content.Store
	infoFunc     func(ctx context.Context, dgst digest.Digest) (content.Info, error)
	readerAtFunc func(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error)
}

func (m *mockContentStore) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	if m.infoFunc != nil {
		return m.infoFunc(ctx, dgst)
	}
	return content.Info{}, errdefs.ErrNotFound
}

func (m *mockContentStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	if m.readerAtFunc != nil {
		return m.readerAtFunc(ctx, desc)
	}
	return nil, errdefs.ErrNotFound
}

// --- Tests for getLocalPath ---

func TestGetLocalPath(t *testing.T) {
	tests := []struct {
		name        string
		ref         string
		wantIsLocal bool
		wantErr     bool
		wantNonEmpty bool
	}{
		{
			name:        "file:// prefix returns local path",
			ref:         "file:///tmp/image.tar",
			wantIsLocal: true,
			wantNonEmpty: true,
		},
		{
			name:        "file:// with relative path",
			ref:         "file://./relative/path",
			wantIsLocal: true,
			wantNonEmpty: true,
		},
		{
			name:        "docker reference is not local",
			ref:         "docker.io/library/alpine:latest",
			wantIsLocal: false,
		},
		{
			name:        "empty string is not local",
			ref:         "",
			wantIsLocal: false,
		},
		{
			name:        "http:// is not local",
			ref:         "http://example.com/image",
			wantIsLocal: false,
		},
		{
			name:        "file:// with just path",
			ref:         "file://myimage.tar",
			wantIsLocal: true,
			wantNonEmpty: true,
		},
		{
			name:        "FILE:// uppercase is not matched",
			ref:         "FILE:///tmp/image.tar",
			wantIsLocal: false,
		},
		{
			name:        "file:// with spaces in path",
			ref:         "file:///tmp/my image.tar",
			wantIsLocal: true,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isLocal, absPath, err := getLocalPath(tt.ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("getLocalPath(%q) error = %v, wantErr %v", tt.ref, err, tt.wantErr)
				return
			}
			if isLocal != tt.wantIsLocal {
				t.Errorf("getLocalPath(%q) isLocal = %v, want %v", tt.ref, isLocal, tt.wantIsLocal)
			}
			if tt.wantNonEmpty && absPath == "" {
				t.Errorf("getLocalPath(%q) absPath is empty, want non-empty", tt.ref)
			}
			if !tt.wantIsLocal && absPath != "" {
				t.Errorf("getLocalPath(%q) absPath = %q, want empty for non-local", tt.ref, absPath)
			}
		})
	}
}

// --- Tests for hosts ---

func TestHosts(t *testing.T) {
	tests := []struct {
		name           string
		opt            Opt
		queryRef       string
		wantInsecure   bool
	}{
		{
			name: "source ref returns source insecure setting",
			opt: Opt{
				Source:         "docker.io/library/alpine:latest",
				Target:         "myregistry.io/alpine:latest",
				SourceInsecure: true,
				TargetInsecure: false,
			},
			queryRef:     "docker.io/library/alpine:latest",
			wantInsecure: true,
		},
		{
			name: "target ref returns target insecure setting",
			opt: Opt{
				Source:         "docker.io/library/alpine:latest",
				Target:         "myregistry.io/alpine:latest",
				SourceInsecure: false,
				TargetInsecure: true,
			},
			queryRef:     "myregistry.io/alpine:latest",
			wantInsecure: true,
		},
		{
			name: "unknown ref returns false",
			opt: Opt{
				Source:         "docker.io/library/alpine:latest",
				Target:         "myregistry.io/alpine:latest",
				SourceInsecure: true,
				TargetInsecure: true,
			},
			queryRef:     "unknown.io/image:latest",
			wantInsecure: false,
		},
		{
			name: "both insecure false",
			opt: Opt{
				Source:         "docker.io/library/alpine:latest",
				Target:         "myregistry.io/alpine:latest",
				SourceInsecure: false,
				TargetInsecure: false,
			},
			queryRef:     "docker.io/library/alpine:latest",
			wantInsecure: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostFunc := hosts(tt.opt)
			_, insecure, err := hostFunc(tt.queryRef)
			if err != nil {
				t.Fatalf("hosts() returned error: %v", err)
			}
			if insecure != tt.wantInsecure {
				t.Errorf("hosts()(%q) insecure = %v, want %v", tt.queryRef, insecure, tt.wantInsecure)
			}
		})
	}
}

// --- Tests for getPlatform ---

func TestGetPlatform(t *testing.T) {
	tests := []struct {
		name     string
		platform *ocispec.Platform
		wantNonEmpty bool
	}{
		{
			name:     "nil platform returns default",
			platform: nil,
			wantNonEmpty: true,
		},
		{
			name: "linux/amd64",
			platform: &ocispec.Platform{
				OS:           "linux",
				Architecture: "amd64",
			},
			wantNonEmpty: true,
		},
		{
			name: "linux/arm64",
			platform: &ocispec.Platform{
				OS:           "linux",
				Architecture: "arm64",
			},
			wantNonEmpty: true,
		},
		{
			name: "with variant",
			platform: &ocispec.Platform{
				OS:           "linux",
				Architecture: "arm",
				Variant:      "v7",
			},
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getPlatform(tt.platform)
			if tt.wantNonEmpty && got == "" {
				t.Error("getPlatform() returned empty string")
			}
			if tt.platform != nil {
				// Check it contains OS and arch
				if tt.platform.OS != "" && !contains(got, tt.platform.OS) {
					t.Errorf("getPlatform() = %q, does not contain OS %q", got, tt.platform.OS)
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// --- Tests for store ---

func TestNewStore(t *testing.T) {
	base := &mockContentStore{}
	descs := []ocispec.Descriptor{
		{
			Digest: digest.FromString("test1"),
			Size:   100,
		},
		{
			Digest: digest.FromString("test2"),
			Size:   200,
		},
	}

	s := newStore(base, descs)
	if s == nil {
		t.Fatal("newStore() returned nil")
	}
	if len(s.remotes) != 2 {
		t.Errorf("newStore() remotes len = %d, want 2", len(s.remotes))
	}
}

func TestStoreInfo(t *testing.T) {
	dgst1 := digest.FromString("found-in-base")
	dgst2 := digest.FromString("found-in-remote")
	dgst3 := digest.FromString("not-found-anywhere")

	remoteDescs := []ocispec.Descriptor{
		{
			Digest: dgst2,
			Size:   200,
		},
	}

	tests := []struct {
		name       string
		dgst       digest.Digest
		baseInfo   *content.Info
		baseErr    error
		wantDigest digest.Digest
		wantSize   int64
		wantErr    bool
	}{
		{
			name: "found in base store",
			dgst: dgst1,
			baseInfo: &content.Info{
				Digest: dgst1,
				Size:   100,
			},
			wantDigest: dgst1,
			wantSize:   100,
		},
		{
			name:       "not in base but found in remotes",
			dgst:       dgst2,
			baseErr:    errdefs.ErrNotFound,
			wantDigest: dgst2,
			wantSize:   200,
		},
		{
			name:    "not found anywhere",
			dgst:    dgst3,
			baseErr: errdefs.ErrNotFound,
			wantErr: true,
		},
		{
			name:    "base returns non-NotFound error",
			dgst:    dgst1,
			baseErr: context.DeadlineExceeded,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &mockContentStore{
				infoFunc: func(ctx context.Context, dgst digest.Digest) (content.Info, error) {
					if tt.baseInfo != nil && dgst == tt.baseInfo.Digest {
						return *tt.baseInfo, nil
					}
					if tt.baseErr != nil {
						return content.Info{}, tt.baseErr
					}
					return content.Info{}, errdefs.ErrNotFound
				},
			}

			s := newStore(base, remoteDescs)
			ctx := context.Background()

			info, err := s.Info(ctx, tt.dgst)
			if (err != nil) != tt.wantErr {
				t.Errorf("store.Info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if info.Digest != tt.wantDigest {
					t.Errorf("store.Info().Digest = %v, want %v", info.Digest, tt.wantDigest)
				}
				if info.Size != tt.wantSize {
					t.Errorf("store.Info().Size = %d, want %d", info.Size, tt.wantSize)
				}
			}
		})
	}
}

func TestStoreInfoMultipleRemotes(t *testing.T) {
	dgstA := digest.FromString("remoteA")
	dgstB := digest.FromString("remoteB")

	remoteDescs := []ocispec.Descriptor{
		{Digest: dgstA, Size: 111},
		{Digest: dgstB, Size: 222},
	}

	base := &mockContentStore{
		infoFunc: func(ctx context.Context, dgst digest.Digest) (content.Info, error) {
			return content.Info{}, errdefs.ErrNotFound
		},
	}

	s := newStore(base, remoteDescs)
	ctx := context.Background()

	// Check first remote descriptor
	info, err := s.Info(ctx, dgstA)
	if err != nil {
		t.Fatalf("Info(dgstA) error = %v", err)
	}
	if info.Size != 111 {
		t.Errorf("Info(dgstA).Size = %d, want 111", info.Size)
	}

	// Check second remote descriptor
	info, err = s.Info(ctx, dgstB)
	if err != nil {
		t.Fatalf("Info(dgstB) error = %v", err)
	}
	if info.Size != 222 {
		t.Errorf("Info(dgstB).Size = %d, want 222", info.Size)
	}
}

// --- Tests for Opt struct ---

func TestOptDefaults(t *testing.T) {
	opt := Opt{}
	if opt.AllPlatforms {
		t.Error("default AllPlatforms should be false")
	}
	if opt.SourceInsecure {
		t.Error("default SourceInsecure should be false")
	}
	if opt.TargetInsecure {
		t.Error("default TargetInsecure should be false")
	}
	if opt.PushChunkSize != 0 {
		t.Errorf("default PushChunkSize = %d, want 0", opt.PushChunkSize)
	}
}

// --- Tests for output struct ---

func TestOutputStruct(t *testing.T) {
	o := output{
		Blobs: []string{"blob1", "blob2", "blob3"},
	}
	if len(o.Blobs) != 3 {
		t.Errorf("output.Blobs len = %d, want 3", len(o.Blobs))
	}
	if o.Blobs[0] != "blob1" {
		t.Errorf("output.Blobs[0] = %q, want %q", o.Blobs[0], "blob1")
	}
}

// --- Tests for Copy error paths ---

func TestCopyInvalidSource(t *testing.T) {
	ctx := context.Background()
	opt := Opt{
		Source:  "INVALID:://ref",
		Target:  "docker.io/library/target:latest",
		WorkDir: t.TempDir(),
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with invalid source should return error")
	}
}

func TestCopyInvalidTarget(t *testing.T) {
	ctx := context.Background()
	opt := Opt{
		Source:  "file:///nonexistent/path/image.tar",
		Target:  "INVALID:://ref",
		WorkDir: t.TempDir(),
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with invalid file source should return error")
	}
}

func TestCopyInvalidBackendType(t *testing.T) {
	ctx := context.Background()
	opt := Opt{
		Source:              "docker.io/library/alpine:latest",
		Target:              "docker.io/library/target:latest",
		WorkDir:             t.TempDir(),
		SourceBackendType:   "invalid_backend_type",
		SourceBackendConfig: "{}",
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with invalid backend type should return error")
	}
}

func TestCopyInvalidPlatforms(t *testing.T) {
	ctx := context.Background()
	opt := Opt{
		Source:    "docker.io/library/alpine:latest",
		Target:    "docker.io/library/target:latest",
		WorkDir:   t.TempDir(),
		Platforms: "invalid/platform/format/extra",
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with invalid platform should return error")
	}
}

func TestCopyLocalSourceNotExist(t *testing.T) {
	ctx := context.Background()
	opt := Opt{
		Source:  "file:///nonexistent/path/to/image.tar",
		Target:  "docker.io/library/target:latest",
		WorkDir: t.TempDir(),
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with nonexistent local source should return error")
	}
}

func TestCopyWorkDirCreation(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	workDir := tmpDir + "/subdir/work"

	opt := Opt{
		Source:  "file:///nonexistent",
		Target:  "docker.io/library/target:latest",
		WorkDir: workDir,
	}

	// This will fail on source, but workdir should be created
	_ = Copy(ctx, opt)
}

func TestCopyExistingWorkDir(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workDir := t.TempDir()

	opt := Opt{
		Source:  "docker.io/library/alpine:latest",
		Target:  "docker.io/library/target:latest",
		WorkDir: workDir,
	}

	// Uses existing workdir path (doesn't need to create it)
	// Will fail on pull, but exercises the existing workdir path
	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() should fail (can't reach registry)")
	}
}

func TestCopyLocalSourceInvalidTar(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()

	// Create a file that is not a valid tar
	invalidFile := filepath.Join(workDir, "invalid.tar")
	os.WriteFile(invalidFile, []byte("not a tar file"), 0644)

	opt := Opt{
		Source:  "file://" + invalidFile,
		Target:  "docker.io/library/target:latest",
		WorkDir: workDir,
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with invalid tar should return error")
	}
}

func TestCopyAllPlatforms(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opt := Opt{
		Source:       "docker.io/library/alpine:latest",
		Target:       "docker.io/library/target:latest",
		WorkDir:      t.TempDir(),
		AllPlatforms: true,
	}

	// Will fail on pull, but exercises the AllPlatforms code path
	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() should fail (can't reach registry)")
	}
}

func TestCopyWithSpecificPlatform(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opt := Opt{
		Source:    "docker.io/library/alpine:latest",
		Target:    "docker.io/library/target:latest",
		WorkDir:   t.TempDir(),
		Platforms: "linux/amd64",
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() should fail (can't reach registry)")
	}
}

func TestCopyLocalSourceToLocalTarget(t *testing.T) {
	ctx := context.Background()
	workDir := t.TempDir()

	// Create a file that's not a valid tar
	inputFile := filepath.Join(workDir, "input.tar")
	os.WriteFile(inputFile, []byte("data"), 0644)

	outputFile := filepath.Join(workDir, "output.tar")

	opt := Opt{
		Source:  "file://" + inputFile,
		Target:  "file://" + outputFile,
		WorkDir: workDir,
	}

	err := Copy(ctx, opt)
	// Will fail on import (invalid tar), but exercises local->local path
	if err == nil {
		t.Error("Copy() with invalid source tar should error")
	}
}

func TestGetLocalPathAbsoluteResolution(t *testing.T) {
	// Test that relative paths are resolved to absolute
	isLocal, absPath, err := getLocalPath("file://relative/path")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !isLocal {
		t.Error("should be local")
	}
	if !filepath.IsAbs(absPath) {
		t.Errorf("path %q should be absolute", absPath)
	}
}

func TestCopyContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	opt := Opt{
		Source:  "docker.io/library/alpine:latest",
		Target:  "docker.io/library/target:latest",
		WorkDir: t.TempDir(),
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() with cancelled context should fail")
	}
}

func TestCopyWithPushChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opt := Opt{
		Source:        "docker.io/library/alpine:latest",
		Target:        "docker.io/library/target:latest",
		WorkDir:       t.TempDir(),
		PushChunkSize: 1024 * 1024,
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() should fail (can't reach registry)")
	}
}

func TestCopyWithInsecureFlags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opt := Opt{
		Source:         "docker.io/library/alpine:latest",
		Target:         "docker.io/library/target:latest",
		WorkDir:        t.TempDir(),
		SourceInsecure: true,
		TargetInsecure: true,
	}

	err := Copy(ctx, opt)
	if err == nil {
		t.Error("Copy() should fail (can't reach registry)")
	}
}
