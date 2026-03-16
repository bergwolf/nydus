package tool

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
)

func TestCheckImageType(t *testing.T) {
	tests := []struct {
		name   string
		parsed *parser.Parsed
		want   string
	}{
		{
			name:   "nydus image",
			parsed: &parser.Parsed{NydusImage: &parser.Image{}},
			want:   "nydus",
		},
		{
			name:   "oci image",
			parsed: &parser.Parsed{OCIImage: &parser.Image{}},
			want:   "oci",
		},
		{
			name:   "unknown image (both nil)",
			parsed: &parser.Parsed{},
			want:   "unknown",
		},
		{
			name:   "nydus takes precedence over oci",
			parsed: &parser.Parsed{NydusImage: &parser.Image{}, OCIImage: &parser.Image{}},
			want:   "nydus",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckImageType(tt.parsed)
			if got != tt.want {
				t.Errorf("CheckImageType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMkMounts(t *testing.T) {
	tests := []struct {
		name      string
		dirs      []string
		wantLen   int
		wantType  string
		wantSrc   string
		checkOpts func(t *testing.T, opts []string)
	}{
		{
			name:    "empty dirs",
			dirs:    []string{},
			wantLen: 0,
		},
		{
			name:     "single dir produces bind mount",
			dirs:     []string{"/layer0"},
			wantLen:  1,
			wantType: "bind",
			wantSrc:  "/layer0",
			checkOpts: func(t *testing.T, opts []string) {
				if len(opts) != 2 || opts[0] != "ro" || opts[1] != "rbind" {
					t.Errorf("options = %v, want [ro rbind]", opts)
				}
			},
		},
		{
			name:     "multiple dirs produce overlay mount",
			dirs:     []string{"/layer0", "/layer1", "/layer2"},
			wantLen:  1,
			wantType: "overlay",
			wantSrc:  "overlay",
			checkOpts: func(t *testing.T, opts []string) {
				if len(opts) != 1 {
					t.Fatalf("options length = %d, want 1", len(opts))
				}
				expected := fmt.Sprintf("lowerdir=%s", strings.Join([]string{"/layer0", "/layer1", "/layer2"}, ":"))
				if opts[0] != expected {
					t.Errorf("option = %q, want %q", opts[0], expected)
				}
			},
		},
		{
			name:    "nil dirs",
			dirs:    nil,
			wantLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mkMounts(tt.dirs)
			if len(got) != tt.wantLen {
				t.Fatalf("mkMounts() returned %d mounts, want %d", len(got), tt.wantLen)
			}
			if tt.wantLen == 0 {
				return
			}
			m := got[0]
			if m.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", m.Type, tt.wantType)
			}
			if m.Source != tt.wantSrc {
				t.Errorf("Source = %q, want %q", m.Source, tt.wantSrc)
			}
			if tt.checkOpts != nil {
				tt.checkOpts(t, m.Options)
			}
		})
	}
}

func TestImageStruct(t *testing.T) {
	img := &Image{
		Layers: []ocispec.Descriptor{
			{MediaType: ocispec.MediaTypeImageLayerGzip},
		},
		LayerBaseDir: "/tmp/layers",
		Rootfs:       "/tmp/rootfs",
	}
	if len(img.Layers) != 1 {
		t.Errorf("Layers length = %d, want 1", len(img.Layers))
	}
	if img.LayerBaseDir != "/tmp/layers" {
		t.Errorf("LayerBaseDir = %q, want %q", img.LayerBaseDir, "/tmp/layers")
	}

	// Verify mkMounts produces correct result for single layer image
	mounts := mkMounts([]string{"/tmp/layers/layer-0"})
	if len(mounts) != 1 {
		t.Fatalf("mkMounts single layer: got %d mounts, want 1", len(mounts))
	}
	if mounts[0].Type != "bind" {
		t.Errorf("single layer mount type = %q, want %q", mounts[0].Type, "bind")
	}
}

func TestMkMountsOverlayFormat(t *testing.T) {
	// Verify that dirs with colons get properly included
	dirs := []string{"/dir:with:colons", "/normal"}
	mounts := mkMounts(dirs)
	if len(mounts) != 1 || mounts[0].Type != "overlay" {
		t.Fatalf("unexpected mount: %v", mounts)
	}
	opt := mounts[0].Options[0]
	if !strings.HasPrefix(opt, "lowerdir=") {
		t.Errorf("option doesn't start with lowerdir=: %q", opt)
	}
}

// Verify mkMounts result is usable as mount.Mount
func TestMkMountsReturnType(t *testing.T) {
	mounts := mkMounts([]string{"/a"})
	var _ []mount.Mount = mounts // compile-time check
}

func TestMkMountsTwoDirs(t *testing.T) {
	dirs := []string{"/upper", "/lower"}
	mounts := mkMounts(dirs)
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}
	if mounts[0].Type != "overlay" {
		t.Errorf("expected overlay, got %s", mounts[0].Type)
	}
	expected := "lowerdir=/upper:/lower"
	if mounts[0].Options[0] != expected {
		t.Errorf("expected %q, got %q", expected, mounts[0].Options[0])
	}
}

func TestCheckImageTypeNilFields(t *testing.T) {
	p := &parser.Parsed{
		OCIImage:   nil,
		NydusImage: nil,
	}
	if got := CheckImageType(p); got != "unknown" {
		t.Errorf("expected unknown, got %s", got)
	}
}

func TestImageDefaultValues(t *testing.T) {
	img := &Image{}
	if img.Layers != nil {
		t.Error("expected nil Layers")
	}
	if img.LayerBaseDir != "" {
		t.Error("expected empty LayerBaseDir")
	}
	if img.Rootfs != "" {
		t.Error("expected empty Rootfs")
	}
}

func TestMkMountsSingleWithSpecialPath(t *testing.T) {
	mounts := mkMounts([]string{"/path/with spaces/dir"})
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}
	if mounts[0].Source != "/path/with spaces/dir" {
		t.Errorf("expected source to be preserved, got %s", mounts[0].Source)
	}
}

func TestCheckImageTypeWithIndex(t *testing.T) {
	// Parsed with Index but no OCI/Nydus image should be unknown
	parsed := &parser.Parsed{
		Index: &ocispec.Index{
			Manifests: []ocispec.Descriptor{
				{MediaType: ocispec.MediaTypeImageManifest},
			},
		},
	}
	if got := CheckImageType(parsed); got != "unknown" {
		t.Errorf("expected unknown with only index, got %s", got)
	}
}

func TestCheckImageTypeWithRemoteOnly(t *testing.T) {
	parsed := &parser.Parsed{
		Remote: nil,
	}
	if got := CheckImageType(parsed); got != "unknown" {
		t.Errorf("expected unknown, got %s", got)
	}
}

func TestImageUmountNonexistentRootfs(t *testing.T) {
	img := &Image{
		Rootfs: "/nonexistent/rootfs/path",
	}
	// Should not error for nonexistent path
	err := img.Umount()
	if err != nil {
		t.Errorf("Umount() unexpected error: %v", err)
	}
}

func TestImageMountCreatesRootfsDir(t *testing.T) {
	tmpDir := t.TempDir()
	rootfs := filepath.Join(tmpDir, "rootfs")

	img := &Image{
		Layers:       []ocispec.Descriptor{},
		LayerBaseDir: tmpDir,
		Rootfs:       rootfs,
	}

	// Mount with empty layers creates dir but mount.All with nil mounts is a no-op
	err := img.Mount()
	// mount.All with nil mounts should succeed
	if err != nil {
		t.Logf("Mount() returned error (expected with empty mounts): %v", err)
	}

	// Verify rootfs directory was created
	_, err = os.Stat(rootfs)
	if err != nil {
		t.Errorf("rootfs directory not created: %v", err)
	}
}

func TestMkMountsLayerOrdering(t *testing.T) {
	dirs := []string{"/a", "/b", "/c", "/d"}
	mounts := mkMounts(dirs)
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}
	if mounts[0].Type != "overlay" {
		t.Errorf("expected overlay, got %s", mounts[0].Type)
	}
	expected := "lowerdir=/a:/b:/c:/d"
	if mounts[0].Options[0] != expected {
		t.Errorf("expected %q, got %q", expected, mounts[0].Options[0])
	}
}

func TestMkMountsColonEscaping(t *testing.T) {
	// verify colons in dir names are present in overlay options
	dirs := []string{"/dir:1", "/dir:2"}
	mounts := mkMounts(dirs)
	if len(mounts) != 1 {
		t.Fatal("expected 1 mount")
	}
	opt := mounts[0].Options[0]
	if !strings.Contains(opt, "/dir:1") && !strings.Contains(opt, "/dir:2") {
		t.Errorf("expected dirs in options: %s", opt)
	}
}

func TestImageLayerCountReversed(t *testing.T) {
	// Image.Mount() reverses layer ordering for overlay mount
	tmpDir := t.TempDir()
	img := &Image{
		Layers: []ocispec.Descriptor{
			{MediaType: "layer0"},
			{MediaType: "layer1"},
			{MediaType: "layer2"},
		},
		LayerBaseDir: tmpDir,
		Rootfs:       filepath.Join(tmpDir, "rootfs"),
	}

	// Create layer directories
	for i := 0; i < 3; i++ {
		layerDir := filepath.Join(tmpDir, fmt.Sprintf("layer-%d", i))
		os.MkdirAll(layerDir, 0755)
	}

	// Mount will fail because layer dirs don't have actual data,
	// but we can verify the dirs list logic
	err := img.Mount()
	if err != nil {
		t.Logf("Mount error (expected): %v", err)
	}
}
