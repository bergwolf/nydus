// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/continuity/fs"
)

// --- Additional GetUpperdir tests ---

func TestGetUpperdirAdditional(t *testing.T) {
	tests := []struct {
		name    string
		lower   []mount.Mount
		upper   []mount.Mount
		want    string
		wantErr bool
	}{
		{
			name: "layer count mismatch",
			lower: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/lower2:/lower1",
				}},
			},
			upper: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/lower3:/lower2:/lower1",
					"upperdir=/upper",
				}},
			},
			// upperlayers = [/lower1, /lower2, /lower3, /upper] = 4
			// lowerlayers = [/lower1, /lower2] = 2
			// 4 != 2+1, so error
			wantErr: true,
		},
		{
			name: "common layers diverge",
			lower: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/lower1",
				}},
			},
			upper: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/different",
					"upperdir=/upper",
				}},
			},
			wantErr: true,
		},
		{
			name:    "no lower, no upper",
			lower:   []mount.Mount{},
			upper:   []mount.Mount{},
			wantErr: true,
		},
		{
			name:  "empty bind source",
			lower: []mount.Mount{},
			upper: []mount.Mount{
				{Type: "bind", Source: ""},
			},
			wantErr: true, // upperdir is empty
		},
		{
			name: "three lower mounts unsupported",
			lower: []mount.Mount{
				{Type: "bind", Source: "/a"},
				{Type: "bind", Source: "/b"},
				{Type: "bind", Source: "/c"},
			},
			upper: []mount.Mount{
				{Type: "overlay"},
			},
			wantErr: true,
		},
		{
			name: "two uppers unsupported",
			lower: []mount.Mount{
				{Type: "bind", Source: "/lower"},
			},
			upper: []mount.Mount{
				{Type: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/a"}},
				{Type: "overlay", Options: []string{"lowerdir=/lower", "upperdir=/b"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetUpperdir(tt.lower, tt.upper)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUpperdir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("GetUpperdir() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Additional GetOverlayLayers tests ---

func TestGetOverlayLayersAdditional(t *testing.T) {
	tests := []struct {
		name    string
		mount   mount.Mount
		want    []string
		wantErr bool
	}{
		{
			name: "all known options",
			mount: mount.Mount{
				Options: []string{
					"workdir=/work",
					"lowerdir=/lower2:/lower1",
					"upperdir=/upper",
					"index=off",
					"userxattr",
					"redirect_dir=on",
				},
			},
			want: []string{"/lower1", "/lower2", "/upper"},
		},
		{
			name: "many lower layers",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/l5:/l4:/l3:/l2:/l1",
				},
			},
			want: []string{"/l1", "/l2", "/l3", "/l4", "/l5"},
		},
		{
			name: "upperdir without lowerdir",
			mount: mount.Mount{
				Options: []string{
					"upperdir=/upper",
				},
			},
			want: []string{"/upper"},
		},
		{
			name: "unknown option",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/lower",
					"metacopy=on",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetOverlayLayers(tt.mount)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetOverlayLayers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("GetOverlayLayers() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// --- cancellableWriter tests ---

func TestCancellableWriterCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf strings.Builder
	w := &cancellableWriter{ctx: ctx, w: &buf}
	_, err := w.Write([]byte("hello"))
	if err == nil {
		t.Error("Write() with cancelled context should return error")
	}
}

func TestCancellableWriterSuccess(t *testing.T) {
	var buf strings.Builder
	w := &cancellableWriter{ctx: context.Background(), w: &buf}
	n, err := w.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != 5 {
		t.Errorf("Write() = %d, want 5", n)
	}
	if buf.String() != "hello" {
		t.Errorf("buf = %q, want %q", buf.String(), "hello")
	}
}

func TestCancellableWriterMultipleWrites(t *testing.T) {
	var buf strings.Builder
	w := &cancellableWriter{ctx: context.Background(), w: &buf}

	for _, s := range []string{"hello", " ", "world"} {
		_, err := w.Write([]byte(s))
		if err != nil {
			t.Fatalf("Write(%q) error = %v", s, err)
		}
	}
	if buf.String() != "hello world" {
		t.Errorf("buf = %q, want %q", buf.String(), "hello world")
	}
}

// --- compareSysStat additional tests ---

func TestCompareSysStatAdditional(t *testing.T) {
	tests := []struct {
		name string
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{
			name: "nil interfaces",
			s1:   nil,
			s2:   nil,
			want: false,
		},
		{
			name: "zero stat",
			s1:   &syscall.Stat_t{},
			s2:   &syscall.Stat_t{},
			want: true,
		},
		{
			name: "all fields match",
			s1:   &syscall.Stat_t{Mode: 0100755, Uid: 0, Gid: 0, Rdev: 0},
			s2:   &syscall.Stat_t{Mode: 0100755, Uid: 0, Gid: 0, Rdev: 0},
			want: true,
		},
		{
			name: "only rdev differs",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 0},
			s2:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 5},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareSysStat(tt.s1, tt.s2)
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if got != tt.want {
				t.Errorf("got = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- compareFileContent tests ---

func TestCompareFileContent(t *testing.T) {
	tmpDir := t.TempDir()

	writeFile := func(name, content string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	tests := []struct {
		name     string
		content1 string
		content2 string
		want     bool
	}{
		{
			name:     "identical files",
			content1: "hello world",
			content2: "hello world",
			want:     true,
		},
		{
			name:     "different files",
			content1: "hello world",
			content2: "goodbye world",
			want:     false,
		},
		{
			name:     "empty files",
			content1: "",
			content2: "",
			want:     true,
		},
		{
			name:     "one empty one not",
			content1: "data",
			content2: "",
			want:     false,
		},
		{
			name:     "large identical files",
			content1: strings.Repeat("x", 100000),
			content2: strings.Repeat("x", 100000),
			want:     true,
		},
		{
			name:     "large different files",
			content1: strings.Repeat("x", 100000),
			content2: strings.Repeat("y", 100000),
			want:     false,
		},
		{
			name:     "differ at last byte",
			content1: strings.Repeat("a", 1000) + "x",
			content2: strings.Repeat("a", 1000) + "y",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f1 := writeFile(tt.name+"_1", tt.content1)
			f2 := writeFile(tt.name+"_2", tt.content2)

			got, err := compareFileContent(f1, f2)
			if err != nil {
				t.Fatalf("compareFileContent() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("compareFileContent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareFileContentNonexistent(t *testing.T) {
	_, err := compareFileContent("/nonexistent/file1", "/nonexistent/file2")
	if err == nil {
		t.Error("compareFileContent() with nonexistent files should error")
	}
}

// --- compareSymlinkTarget tests ---

func TestCompareSymlinkTarget(t *testing.T) {
	tmpDir := t.TempDir()

	// Create target files
	target1 := filepath.Join(tmpDir, "target1")
	target2 := filepath.Join(tmpDir, "target2")
	os.WriteFile(target1, []byte("a"), 0644)
	os.WriteFile(target2, []byte("b"), 0644)

	// Create symlinks
	link1 := filepath.Join(tmpDir, "link1")
	link2 := filepath.Join(tmpDir, "link2_same")
	link3 := filepath.Join(tmpDir, "link3_diff")

	os.Symlink(target1, link1)
	os.Symlink(target1, link2)
	os.Symlink(target2, link3)

	t.Run("same target", func(t *testing.T) {
		got, err := compareSymlinkTarget(link1, link2)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if !got {
			t.Error("same targets should be equal")
		}
	})

	t.Run("different target", func(t *testing.T) {
		got, err := compareSymlinkTarget(link1, link3)
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		if got {
			t.Error("different targets should not be equal")
		}
	})

	t.Run("nonexistent link", func(t *testing.T) {
		_, err := compareSymlinkTarget("/nonexistent", link1)
		if err == nil {
			t.Error("should error for nonexistent symlink")
		}
	})
}

// --- compareCapabilities tests ---

func TestCompareCapabilities(t *testing.T) {
	tmpDir := t.TempDir()

	f1 := filepath.Join(tmpDir, "file1")
	f2 := filepath.Join(tmpDir, "file2")
	os.WriteFile(f1, []byte("a"), 0644)
	os.WriteFile(f2, []byte("b"), 0644)

	// Both files with no capabilities should be equal
	got, err := compareCapabilities(f1, f2)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !got {
		t.Error("files without capabilities should be equal")
	}
}

// --- sameDirent tests ---

func TestSameDirentSameFile(t *testing.T) {
	tmpDir := t.TempDir()
	f := filepath.Join(tmpDir, "file")
	os.WriteFile(f, []byte("data"), 0644)

	fi1, _ := os.Lstat(f)
	fi2, _ := os.Lstat(f)

	got, err := sameDirent(fi1, fi2, f, f)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !got {
		t.Error("same file should be same dirent")
	}
}

// --- checkDelete tests ---

func TestCheckDelete(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a regular file
	regularFile := filepath.Join(tmpDir, "regular")
	os.WriteFile(regularFile, []byte("data"), 0644)
	fi, _ := os.Lstat(regularFile)

	// Regular files are not deletes
	isDelete, skip, err := checkDelete(tmpDir, "regular", tmpDir, fi)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if isDelete {
		t.Error("regular file should not be a delete")
	}
	if skip {
		t.Error("regular file should not be skipped")
	}
}

func TestCheckDeleteDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")
	os.Mkdir(subDir, 0755)
	fi, _ := os.Lstat(subDir)

	isDelete, skip, err := checkDelete(tmpDir, "subdir", tmpDir, fi)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if isDelete {
		t.Error("directory should not be a delete")
	}
	if skip {
		t.Error("directory should not be skipped")
	}
}

// --- Changes tests ---

func TestChangesEmptyUpperdir(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	var changes []fs.ChangeKind
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changes = append(changes, k)
		return nil
	}

	err := Changes(context.Background(), func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("expected 0 changes, got %d", len(changes))
	}
}

func TestChangesWithNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	// Add a new file in upper that's not in base
	os.WriteFile(filepath.Join(upperDir, "newfile"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "newfile"), []byte("data"), 0644)

	var changeKinds []fs.ChangeKind
	var changePaths []string
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changeKinds = append(changeKinds, k)
		changePaths = append(changePaths, p)
		return nil
	}

	err := Changes(context.Background(), func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}
	if len(changeKinds) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changeKinds))
	}
	if changeKinds[0] != fs.ChangeKindAdd {
		t.Errorf("expected ChangeKindAdd, got %v", changeKinds[0])
	}
	if changePaths[0] != "/newfile" {
		t.Errorf("expected /newfile, got %q", changePaths[0])
	}
}

func TestChangesWithModifiedFile(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	// Same file exists in base and upper
	os.WriteFile(filepath.Join(base, "existing"), []byte("old"), 0644)
	os.WriteFile(filepath.Join(upperDir, "existing"), []byte("new-content"), 0644)
	os.WriteFile(filepath.Join(upperView, "existing"), []byte("new-content"), 0644)

	var changeKinds []fs.ChangeKind
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changeKinds = append(changeKinds, k)
		return nil
	}

	err := Changes(context.Background(), func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}
	if len(changeKinds) == 0 {
		t.Fatal("expected at least 1 change")
	}
	// Should be a modify
	if changeKinds[0] != fs.ChangeKindModify {
		t.Errorf("expected ChangeKindModify, got %v", changeKinds[0])
	}
}

func TestChangesWithWithoutPaths(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	// Create files
	os.WriteFile(filepath.Join(upperDir, "keep"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperDir, "skip"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "keep"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "skip"), []byte("data"), 0644)

	var changePaths []string
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changePaths = append(changePaths, p)
		return nil
	}

	// Filter out /skip
	withoutPaths := []string{"/skip"}
	err := Changes(context.Background(), func(path string) {}, nil, withoutPaths, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}

	for _, p := range changePaths {
		if p == "/skip" {
			t.Error("filtered path /skip should not appear in changes")
		}
	}
}

func TestChangesWithPaths(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	var deleteChanges []string
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		if k == fs.ChangeKindDelete {
			deleteChanges = append(deleteChanges, p)
		}
		return nil
	}

	// withPaths should generate delete entries
	withPaths := []string{"/mnt/data"}
	err := Changes(context.Background(), func(path string) {}, withPaths, nil, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}

	found := false
	for _, p := range deleteChanges {
		if p == "/mnt/data" {
			found = true
			break
		}
	}
	if !found {
		t.Error("withPaths entry should generate a delete change")
	}
}

func TestChangesContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	// Create some files
	for i := 0; i < 10; i++ {
		name := filepath.Join(upperDir, strings.Repeat("f", i+1))
		os.WriteFile(name, []byte("data"), 0644)
		name2 := filepath.Join(upperView, strings.Repeat("f", i+1))
		os.WriteFile(name2, []byte("data"), 0644)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		return nil
	}

	err := Changes(ctx, func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err == nil {
		t.Error("Changes() with cancelled context should return error")
	}
}

func TestChangesChangeFnError(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	os.WriteFile(filepath.Join(upperDir, "file"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "file"), []byte("data"), 0644)

	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		return io.ErrUnexpectedEOF
	}

	err := Changes(context.Background(), func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err == nil {
		t.Error("Changes() should propagate changeFn error")
	}
}

// --- overlaySupportIndex tests ---

func TestOverlaySupportIndex(t *testing.T) {
	// Just verify it doesn't panic and returns a bool
	_ = overlaySupportIndex()
}

// --- Diff error paths ---

func TestDiffInvalidDirs(t *testing.T) {
	err := Diff(
		context.Background(),
		func(path string) {},
		nil,
		nil,
		io.Discard,
		"/nonexistent/lower",
		"/nonexistent/upper",
	)
	// Should error because mount operations will fail
	if err == nil {
		t.Error("Diff() with nonexistent dirs should error")
	}
}

// --- Test withoutPaths prefix filtering ---

func TestChangesWithoutPathsPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(upperDir, 0755)
	os.MkdirAll(upperView, 0755)
	os.MkdirAll(base, 0755)

	// Create nested files under /var
	os.MkdirAll(filepath.Join(upperDir, "var", "log"), 0755)
	os.WriteFile(filepath.Join(upperDir, "var", "log", "test.log"), []byte("log"), 0644)
	os.MkdirAll(filepath.Join(upperView, "var", "log"), 0755)
	os.WriteFile(filepath.Join(upperView, "var", "log", "test.log"), []byte("log"), 0644)

	// Also create file outside the filtered path
	os.WriteFile(filepath.Join(upperDir, "keepme"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "keepme"), []byte("data"), 0644)

	var changePaths []string
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changePaths = append(changePaths, p)
		return nil
	}

	// Filter out /var and all children
	withoutPaths := []string{"/var"}
	err := Changes(context.Background(), func(path string) {}, nil, withoutPaths, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}

	for _, p := range changePaths {
		if p == "/var" || strings.HasPrefix(p, "/var/") {
			t.Errorf("filtered path %q should not appear in changes", p)
		}
	}

	// keepme should be present
	foundKeep := false
	for _, p := range changePaths {
		if p == "/keepme" {
			foundKeep = true
		}
	}
	if !foundKeep {
		t.Error("/keepme should appear in changes")
	}
}

// --- Test new file in subdirectory ---

func TestChangesNewFileInSubdir(t *testing.T) {
	tmpDir := t.TempDir()
	upperDir := filepath.Join(tmpDir, "upper")
	upperView := filepath.Join(tmpDir, "upperview")
	base := filepath.Join(tmpDir, "base")

	os.MkdirAll(filepath.Join(upperDir, "subdir"), 0755)
	os.MkdirAll(filepath.Join(upperView, "subdir"), 0755)
	os.MkdirAll(base, 0755)

	os.WriteFile(filepath.Join(upperDir, "subdir", "newfile"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(upperView, "subdir", "newfile"), []byte("data"), 0644)

	var changePaths []string
	changeFn := func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
		changePaths = append(changePaths, p)
		return nil
	}

	err := Changes(context.Background(), func(path string) {}, nil, nil, changeFn, upperDir, upperView, base)
	if err != nil {
		t.Fatalf("Changes() error = %v", err)
	}
	if len(changePaths) == 0 {
		t.Fatal("expected changes for new files in subdirectory")
	}
}
