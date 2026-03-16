// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package compactor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactConfigFields(t *testing.T) {
	cfg := CompactConfig{
		MinUsedRatio:    "10",
		CompactBlobSize: "20971520",
		MaxCompactSize:  "209715200",
		LayersToCompact: "64",
		BlobsDir:        "/tmp/blobs",
	}

	require.Equal(t, "10", cfg.MinUsedRatio)
	require.Equal(t, "20971520", cfg.CompactBlobSize)
	require.Equal(t, "209715200", cfg.MaxCompactSize)
	require.Equal(t, "64", cfg.LayersToCompact)
	require.Equal(t, "/tmp/blobs", cfg.BlobsDir)
}

func TestDefaultCompactConfig(t *testing.T) {
	require.Equal(t, "5", defaultCompactConfig.MinUsedRatio)
	require.Equal(t, "10485760", defaultCompactConfig.CompactBlobSize)
	require.Equal(t, "104857600", defaultCompactConfig.MaxCompactSize)
	require.Equal(t, "32", defaultCompactConfig.LayersToCompact)
}

func TestCompactConfigDumps(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.json")

	cfg := CompactConfig{
		MinUsedRatio:    "5",
		CompactBlobSize: "10485760",
		MaxCompactSize:  "104857600",
		LayersToCompact: "32",
		BlobsDir:        "/tmp/blobs",
	}

	err := cfg.Dumps(filePath)
	require.NoError(t, err)

	// Verify file was created and is valid JSON
	content, err := os.ReadFile(filePath)
	require.NoError(t, err)

	var loaded CompactConfig
	err = json.Unmarshal(content, &loaded)
	require.NoError(t, err)
	require.Equal(t, cfg.MinUsedRatio, loaded.MinUsedRatio)
	require.Equal(t, cfg.CompactBlobSize, loaded.CompactBlobSize)
	require.Equal(t, cfg.MaxCompactSize, loaded.MaxCompactSize)
	require.Equal(t, cfg.LayersToCompact, loaded.LayersToCompact)
	require.Equal(t, cfg.BlobsDir, loaded.BlobsDir)
}

func TestCompactConfigDumpsInvalidPath(t *testing.T) {
	cfg := CompactConfig{}
	err := cfg.Dumps("/nonexistent/dir/config.json")
	require.Error(t, err)
}

func TestLoadCompactConfig(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.json")

	original := CompactConfig{
		MinUsedRatio:    "10",
		CompactBlobSize: "20971520",
		MaxCompactSize:  "209715200",
		LayersToCompact: "16",
		BlobsDir:        "/blobs",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)
	err = os.WriteFile(filePath, data, 0644)
	require.NoError(t, err)

	loaded, err := loadCompactConfig(filePath)
	require.NoError(t, err)
	require.Equal(t, original.MinUsedRatio, loaded.MinUsedRatio)
	require.Equal(t, original.CompactBlobSize, loaded.CompactBlobSize)
	require.Equal(t, original.MaxCompactSize, loaded.MaxCompactSize)
	require.Equal(t, original.LayersToCompact, loaded.LayersToCompact)
	require.Equal(t, original.BlobsDir, loaded.BlobsDir)
}

func TestLoadCompactConfigMissingFile(t *testing.T) {
	_, err := loadCompactConfig("/nonexistent/config.json")
	require.Error(t, err)
}

func TestLoadCompactConfigInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.json")

	err := os.WriteFile(filePath, []byte("not valid json"), 0644)
	require.NoError(t, err)

	_, err = loadCompactConfig(filePath)
	require.Error(t, err)
}

func TestNewCompactorWithDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	compactor, err := NewCompactor("/usr/bin/nydus-image", tmpDir, "")
	require.NoError(t, err)
	require.NotNil(t, compactor)
	require.Equal(t, tmpDir, compactor.cfg.BlobsDir)
	require.Equal(t, defaultCompactConfig.MinUsedRatio, compactor.cfg.MinUsedRatio)
	require.Equal(t, defaultCompactConfig.CompactBlobSize, compactor.cfg.CompactBlobSize)
	require.Equal(t, defaultCompactConfig.MaxCompactSize, compactor.cfg.MaxCompactSize)
	require.Equal(t, defaultCompactConfig.LayersToCompact, compactor.cfg.LayersToCompact)
}

func TestNewCompactorWithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	cfg := CompactConfig{
		MinUsedRatio:    "15",
		CompactBlobSize: "5242880",
		MaxCompactSize:  "52428800",
		LayersToCompact: "8",
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	err = os.WriteFile(configPath, data, 0644)
	require.NoError(t, err)

	workDir := filepath.Join(tmpDir, "work")
	err = os.MkdirAll(workDir, 0755)
	require.NoError(t, err)

	compactor, err := NewCompactor("/usr/bin/nydus-image", workDir, configPath)
	require.NoError(t, err)
	require.NotNil(t, compactor)
	require.Equal(t, "15", compactor.cfg.MinUsedRatio)
	require.Equal(t, "5242880", compactor.cfg.CompactBlobSize)
	require.Equal(t, workDir, compactor.cfg.BlobsDir)
}

func TestNewCompactorInvalidConfigPath(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := NewCompactor("/usr/bin/nydus-image", tmpDir, "/nonexistent/config.json")
	require.Error(t, err)
}

func TestCompactConfigDumpsOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.json")

	cfg1 := CompactConfig{MinUsedRatio: "5"}
	err := cfg1.Dumps(filePath)
	require.NoError(t, err)

	cfg2 := CompactConfig{MinUsedRatio: "10"}
	err = cfg2.Dumps(filePath)
	require.NoError(t, err)

	loaded, err := loadCompactConfig(filePath)
	require.NoError(t, err)
	require.Equal(t, "10", loaded.MinUsedRatio)
}
