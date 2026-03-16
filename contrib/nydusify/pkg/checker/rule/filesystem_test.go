// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package rule

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
)

func TestFilesystemRuleWalkSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "target.txt"), []byte("data"), 0644))
	require.NoError(t, os.Symlink("target.txt", filepath.Join(tmpDir, "link.txt")))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)

	linkNode, ok := nodes["/link.txt"]
	require.True(t, ok)
	require.Equal(t, "target.txt", linkNode.Symlink)
}

func TestFilesystemRuleWalkNestedDirs(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "a", "b", "c"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "a", "b", "c", "deep.txt"), []byte("deep"), 0644))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)
	require.Contains(t, nodes, "/a/b/c/deep.txt")
	require.Equal(t, int64(4), nodes["/a/b/c/deep.txt"].Size)
}

func TestFilesystemRuleWalkEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "empty.txt"), []byte{}, 0644))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)
	require.Contains(t, nodes, "/empty.txt")
	require.Equal(t, int64(0), nodes["/empty.txt"].Size)
	// Empty file should still have a hash
	require.NotNil(t, nodes["/empty.txt"].Hash)
}

func TestFilesystemRuleWalkFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "exec.sh"), []byte("#!/bin/sh"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "read.txt"), []byte("data"), 0444))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)

	execNode := nodes["/exec.sh"]
	readNode := nodes["/read.txt"]

	require.NotEqual(t, execNode.Mode, readNode.Mode)
}

func TestFilesystemRuleWalkOwnership(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "file.txt"), []byte("data"), 0644))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)

	node := nodes["/file.txt"]
	// UID/GID should be set (current user)
	require.True(t, node.UID >= 0)
	require.True(t, node.GID >= 0)
}

func TestFilesystemRuleVerifySizeMismatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir1, "a.txt"), []byte("short"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "a.txt"), []byte("longer content"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not match")
}

func TestFilesystemRuleVerifyMultipleFiles(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	for i := 0; i < 10; i++ {
		name := filepath.Join(dir1, "file"+string(rune('0'+i))+".txt")
		require.NoError(t, os.WriteFile(name, []byte("content"), 0644))
		name2 := filepath.Join(dir2, "file"+string(rune('0'+i))+".txt")
		require.NoError(t, os.WriteFile(name2, []byte("content"), 0644))
	}

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.NoError(t, err)
}

func TestFilesystemRuleVerifySubdirectoryMismatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir1, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "sub", "file.txt"), []byte("v1"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir2, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "sub", "file.txt"), []byte("v2"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
}

func TestFilesystemRuleVerifyExtraInTarget(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir1, "common.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "common.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "extra.txt"), []byte("extra"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not found in source image")
}

func TestFilesystemRuleMountImageInvalid(t *testing.T) {
	rule := FilesystemRule{}
	img := &Image{
		Parsed: &parser.Parsed{},
	}
	_, err := rule.mountImage(img, "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid image for mounting")
}

func TestNodeStringSymlink(t *testing.T) {
	node := Node{
		Path:    "/link",
		Symlink: "/target",
		Mode:    os.ModeSymlink,
	}
	str := node.String()
	require.Contains(t, str, "symink: /target")
}

func TestNodeEquality(t *testing.T) {
	n1 := Node{
		Path: "/test",
		Size: 100,
		Mode: 0644,
		UID:  1000,
		GID:  1000,
		Hash: []byte{0x01, 0x02},
	}
	n2 := n1 // copy

	require.Equal(t, n1.Path, n2.Path)
	require.Equal(t, n1.Size, n2.Size)
	require.Equal(t, n1.Mode, n2.Mode)
	require.Equal(t, n1.UID, n2.UID)
	require.Equal(t, n1.Hash, n2.Hash)
}

func TestFilesystemRuleWalkLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	// 64KB file
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "large.bin"), data, 0644))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)
	require.Contains(t, nodes, "/large.bin")
	require.Equal(t, int64(64*1024), nodes["/large.bin"].Size)
	require.NotNil(t, nodes["/large.bin"].Hash)
}

func TestFilesystemRuleVerifyPermissionMismatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir1, "a.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "a.txt"), []byte("data"), 0755))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not match")
}

func TestFilesystemRuleWalkDirSizeZero(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "subdir"), 0755))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)

	// Directory sizes are ignored (set to 0)
	require.Equal(t, int64(0), nodes["/"].Size)
	require.Equal(t, int64(0), nodes["/subdir"].Size)
}

func TestFilesystemRuleWalkNonexistentDir(t *testing.T) {
	rule := FilesystemRule{}
	_, err := rule.walk("/nonexistent/directory")
	require.Error(t, err)
}
