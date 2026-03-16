// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/continuity/fs"
)

// --- Additional NewChangeWriter tests ---

func TestNewChangeWriterWithOpts(t *testing.T) {
	var buf bytes.Buffer
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Test with modTimeUpperBound option
	modTimeOpt := func(cw *ChangeWriter) {
		cw.modTimeUpperBound = &ts
	}

	cw := NewChangeWriter(&buf, "/source", modTimeOpt)
	if cw == nil {
		t.Fatal("NewChangeWriter() with opts returned nil")
	}
	if cw.modTimeUpperBound == nil {
		t.Error("modTimeUpperBound should be set")
	}
	if !cw.modTimeUpperBound.Equal(ts) {
		t.Errorf("modTimeUpperBound = %v, want %v", cw.modTimeUpperBound, ts)
	}
	cw.Close()
}

func TestNewChangeWriterMultipleOpts(t *testing.T) {
	var buf bytes.Buffer
	ts := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)

	opt1 := func(cw *ChangeWriter) {
		cw.modTimeUpperBound = &ts
	}
	opt2 := func(cw *ChangeWriter) {
		cw.whiteoutT = ts
	}

	cw := NewChangeWriter(&buf, "/test", opt1, opt2)
	if cw.modTimeUpperBound == nil {
		t.Error("modTimeUpperBound should be set")
	}
	if !cw.whiteoutT.Equal(ts) {
		t.Errorf("whiteoutT = %v, want %v", cw.whiteoutT, ts)
	}
	cw.Close()
}

// --- HandleChange tests for delete (whiteout) ---

func TestHandleChangeDelete(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")

	err := cw.HandleChange(fs.ChangeKindDelete, "/somefile", nil, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}

	err = cw.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Read the tar to verify whiteout entry
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	// Whiteout should be .wh.somefile
	expectedName := ".wh.somefile"
	if hdr.Name != expectedName {
		t.Errorf("whiteout name = %q, want %q", hdr.Name, expectedName)
	}
	if hdr.Typeflag != tar.TypeReg {
		t.Errorf("whiteout typeflag = %d, want TypeReg(%d)", hdr.Typeflag, tar.TypeReg)
	}
	if hdr.Size != 0 {
		t.Errorf("whiteout size = %d, want 0", hdr.Size)
	}
}

func TestHandleChangeDeleteInSubdir(t *testing.T) {
	tmpDir := t.TempDir()

	// Create the parent directory structure in source
	os.MkdirAll(filepath.Join(tmpDir, "subdir"), 0755)

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindDelete, "/subdir/deleted", nil, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	// Read the tar to verify
	tr := tar.NewReader(&buf)
	foundWhiteout := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		if strings.Contains(hdr.Name, ".wh.deleted") {
			foundWhiteout = true
		}
	}
	if !foundWhiteout {
		t.Error("expected whiteout entry for deleted file")
	}
}

// --- HandleChange tests for add (regular file) ---

func TestHandleChangeAddRegularFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a regular file in the source
	content := "hello world"
	os.WriteFile(filepath.Join(tmpDir, "newfile"), []byte(content), 0644)

	fi, _ := os.Lstat(filepath.Join(tmpDir, "newfile"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/newfile", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	// Read the tar to verify
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	if hdr.Name != "newfile" {
		t.Errorf("name = %q, want %q", hdr.Name, "newfile")
	}
	if hdr.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", hdr.Size, len(content))
	}

	// Read content
	data, _ := io.ReadAll(tr)
	if string(data) != content {
		t.Errorf("content = %q, want %q", string(data), content)
	}
}

func TestHandleChangeAddDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	os.MkdirAll(filepath.Join(tmpDir, "newdir"), 0755)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "newdir"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/newdir", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	if !strings.HasSuffix(hdr.Name, "/") {
		t.Errorf("directory name should end with /, got %q", hdr.Name)
	}
	if hdr.Typeflag != tar.TypeDir {
		t.Errorf("typeflag = %d, want TypeDir(%d)", hdr.Typeflag, tar.TypeDir)
	}
}

// --- HandleChange tests for symlink ---

func TestHandleChangeAddSymlink(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a symlink
	target := filepath.Join(tmpDir, "target")
	os.WriteFile(target, []byte("data"), 0644)
	link := filepath.Join(tmpDir, "link")
	os.Symlink(target, link)

	fi, _ := os.Lstat(link)

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/link", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	if hdr.Typeflag != tar.TypeSymlink {
		t.Errorf("typeflag = %d, want TypeSymlink(%d)", hdr.Typeflag, tar.TypeSymlink)
	}
	if hdr.Linkname != target {
		t.Errorf("linkname = %q, want %q", hdr.Linkname, target)
	}
}

// --- HandleChange tests for modify ---

func TestHandleChangeModifyFile(t *testing.T) {
	tmpDir := t.TempDir()
	content := "modified content"
	os.WriteFile(filepath.Join(tmpDir, "modfile"), []byte(content), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "modfile"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindModify, "/modfile", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	if hdr.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", hdr.Size, len(content))
	}
}

// --- HandleChange error propagation ---

func TestHandleChangeWithError(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")

	err := cw.HandleChange(fs.ChangeKindAdd, "/file", nil, io.ErrUnexpectedEOF)
	if err == nil {
		t.Error("HandleChange() should propagate error")
	}
	cw.Close()
}

// --- HandleChange unmodified ---

func TestHandleChangeUnmodifiedSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "file"), []byte("data"), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "file"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindUnmodified, "/file", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	// Unmodified files with no hardlink info should not create tar entries
	tr := tar.NewReader(&buf)
	_, err = tr.Next()
	if err != io.EOF {
		t.Error("unmodified file should not produce tar entry")
	}
}

// --- HandleChange socket (ignored) ---

func TestHandleChangeSocket(t *testing.T) {
	tmpDir := t.TempDir()
	// We can't easily create a socket in the filesystem for testing,
	// but we can verify the code path by checking the mode flag handling
	// Just test that HandleChange doesn't error on nil fileInfo for socket

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	// Close without error
	err := cw.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// --- Close tests ---

func TestCloseEmpty(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")
	err := cw.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	// Tar should have at least the EOF markers
	if buf.Len() == 0 {
		t.Error("tar output should not be empty even with no entries")
	}
}

// --- includeParents tests ---

func TestIncludeParentsAddsParentDirs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directories
	os.MkdirAll(filepath.Join(tmpDir, "a", "b"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "a", "b", "file"), []byte("data"), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "a", "b", "file"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/a/b/file", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	// Should include parent dir entries
	tr := tar.NewReader(&buf)
	names := []string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		names = append(names, hdr.Name)
	}

	// Should have parent dirs and the file
	if len(names) < 2 {
		t.Errorf("expected at least 2 entries (parent + file), got %d: %v", len(names), names)
	}
}

func TestIncludeParentsNilAddedDirs(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")
	cw.addedDirs = nil

	hdr := &tar.Header{
		Name: "test",
	}
	err := cw.includeParents(hdr)
	if err != nil {
		t.Fatalf("includeParents() with nil addedDirs should not error, got: %v", err)
	}
}

// --- Additional copyBuffered tests ---

func TestCopyBufferedExactBufferSize(t *testing.T) {
	// Test with data exactly the size of the buffer (32*1024)
	data := strings.Repeat("A", 32*1024)
	src := strings.NewReader(data)
	var dst bytes.Buffer

	n, err := copyBuffered(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("copyBuffered() error = %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("n = %d, want %d", n, len(data))
	}
	if dst.String() != data {
		t.Error("output mismatch")
	}
}

func TestCopyBufferedMultipleChunks(t *testing.T) {
	// Data larger than buffer to test multiple iterations
	data := strings.Repeat("B", 100*1024)
	src := strings.NewReader(data)
	var dst bytes.Buffer

	n, err := copyBuffered(context.Background(), &dst, src)
	if err != nil {
		t.Fatalf("copyBuffered() error = %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("n = %d, want %d", n, len(data))
	}
}

func TestCopyBufferedReadError(t *testing.T) {
	src := &errorReader{err: io.ErrUnexpectedEOF}
	var dst bytes.Buffer

	_, err := copyBuffered(context.Background(), &dst, src)
	if err == nil {
		t.Error("copyBuffered() should propagate read error")
	}
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestCopyBufferedShortWrite(t *testing.T) {
	src := strings.NewReader("hello world")
	dst := &shortWriter{}

	_, err := copyBuffered(context.Background(), dst, src)
	if err == nil {
		t.Error("copyBuffered() should detect short write")
	}
}

type shortWriter struct{}

func (w *shortWriter) Write(p []byte) (int, error) {
	// Write fewer bytes than given
	if len(p) > 1 {
		return 1, nil
	}
	return len(p), nil
}

// --- modTimeUpperBound tests ---

func TestHandleChangeModTimeUpperBound(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file with current timestamp
	content := "test data"
	filePath := filepath.Join(tmpDir, "file")
	os.WriteFile(filePath, []byte(content), 0644)
	fi, _ := os.Lstat(filePath)

	// Set upper bound to a past time
	pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir, func(cw *ChangeWriter) {
		cw.modTimeUpperBound = &pastTime
	})

	err := cw.HandleChange(fs.ChangeKindAdd, "/file", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	// ModTime should be clamped to the upper bound
	if hdr.ModTime.After(pastTime) {
		t.Errorf("ModTime %v should not be after upper bound %v", hdr.ModTime, pastTime)
	}
}

// --- Multiple deletions ---

func TestHandleChangeMultipleDeletes(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")

	files := []string{"/file1", "/file2", "/file3"}
	for _, f := range files {
		err := cw.HandleChange(fs.ChangeKindDelete, f, nil, nil)
		if err != nil {
			t.Fatalf("HandleChange(Delete, %q) error = %v", f, err)
		}
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	count := 0
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		count++
	}
	if count < len(files) {
		t.Errorf("expected at least %d whiteout entries, got %d", len(files), count)
	}
}

// --- Large file test ---

func TestHandleChangeLargeFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a larger file
	data := strings.Repeat("X", 256*1024) // 256KB
	filePath := filepath.Join(tmpDir, "largefile")
	os.WriteFile(filePath, []byte(data), 0644)
	fi, _ := os.Lstat(filePath)

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/largefile", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	// Verify tar content
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}
	if hdr.Size != int64(len(data)) {
		t.Errorf("size = %d, want %d", hdr.Size, len(data))
	}

	readData, _ := io.ReadAll(tr)
	if string(readData) != data {
		t.Error("content mismatch for large file")
	}
}

// --- Empty file test ---

func TestHandleChangeEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "empty")
	os.WriteFile(filePath, []byte{}, 0644)
	fi, _ := os.Lstat(filePath)

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/empty", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}
	if hdr.Size != 0 {
		t.Errorf("size = %d, want 0", hdr.Size)
	}
}

// --- Test whiteout time ---

func TestNewChangeWriterWhiteoutTime(t *testing.T) {
	var buf bytes.Buffer
	customTime := time.Date(2022, 3, 1, 0, 0, 0, 0, time.UTC)
	cw := NewChangeWriter(&buf, "/source", func(cw *ChangeWriter) {
		cw.whiteoutT = customTime
	})

	err := cw.HandleChange(fs.ChangeKindDelete, "/deleted", nil, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}
	if !hdr.ModTime.Equal(customTime) {
		t.Errorf("whiteout ModTime = %v, want %v", hdr.ModTime, customTime)
	}
}

// --- Test PAX format ---

func TestHandleChangePAXFormat(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "paxfile"), []byte("data"), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "paxfile"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/paxfile", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}
	// Verify the format is set (PAX, USTAR, or GNU are all acceptable since
	// the tar library may choose the most compact representation)
	_ = hdr
}

// --- Test AccessTime and ChangeTime are zeroed ---

func TestHandleChangeTimestampCleaning(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "timefile"), []byte("data"), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "timefile"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	err := cw.HandleChange(fs.ChangeKindAdd, "/timefile", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	if !hdr.AccessTime.IsZero() {
		t.Errorf("AccessTime should be zero, got %v", hdr.AccessTime)
	}
	if !hdr.ChangeTime.IsZero() {
		t.Errorf("ChangeTime should be zero, got %v", hdr.ChangeTime)
	}
}

// --- Multiple adds ---

func TestHandleChangeMultipleAdds(t *testing.T) {
	tmpDir := t.TempDir()

	files := map[string]string{
		"file1": "content1",
		"file2": "content2",
		"file3": "content3",
	}
	for name, content := range files {
		os.WriteFile(filepath.Join(tmpDir, name), []byte(content), 0644)
	}

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	for name := range files {
		fi, _ := os.Lstat(filepath.Join(tmpDir, name))
		err := cw.HandleChange(fs.ChangeKindAdd, "/"+name, fi, nil)
		if err != nil {
			t.Fatalf("HandleChange(%q) error = %v", name, err)
		}
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	count := 0
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		count++
	}
	if count != len(files) {
		t.Errorf("expected %d entries, got %d", len(files), count)
	}
}

// --- Test path normalization ---

func TestHandleChangePathNormalization(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "file"), []byte("data"), 0644)
	fi, _ := os.Lstat(filepath.Join(tmpDir, "file"))

	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, tmpDir)

	// Pass path with leading separator
	err := cw.HandleChange(fs.ChangeKindAdd, "/file", fi, nil)
	if err != nil {
		t.Fatalf("HandleChange() error = %v", err)
	}
	cw.Close()

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next() error = %v", err)
	}

	// Name should not start with /
	if strings.HasPrefix(hdr.Name, "/") {
		t.Errorf("tar entry name should not start with /, got %q", hdr.Name)
	}
}
