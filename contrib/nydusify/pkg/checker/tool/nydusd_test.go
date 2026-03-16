// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package tool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNydusdConfigDefaults(t *testing.T) {
	conf := NydusdConfig{}
	if conf.EnablePrefetch {
		t.Error("expected false EnablePrefetch")
	}
	if conf.NydusdPath != "" {
		t.Error("expected empty NydusdPath")
	}
	if conf.BootstrapPath != "" {
		t.Error("expected empty BootstrapPath")
	}
	if conf.DigestValidate {
		t.Error("expected false DigestValidate")
	}
	if conf.Mode != "" {
		t.Error("expected empty Mode")
	}
}

func TestNydusdConfigFields(t *testing.T) {
	conf := NydusdConfig{
		EnablePrefetch:            true,
		NydusdPath:                "/usr/bin/nydusd",
		BootstrapPath:             "/tmp/bootstrap",
		ConfigPath:                "/tmp/config.json",
		BackendType:               "registry",
		BackendConfig:             `{"host":"reg.io"}`,
		ExternalBackendConfigPath: "/tmp/external.json",
		BlobCacheDir:              "/tmp/cache",
		APISockPath:               "/tmp/api.sock",
		MountPath:                 "/tmp/mnt",
		Mode:                      "direct",
		DigestValidate:            true,
	}
	if conf.NydusdPath != "/usr/bin/nydusd" {
		t.Errorf("NydusdPath = %q", conf.NydusdPath)
	}
	if conf.BackendType != "registry" {
		t.Errorf("BackendType = %q", conf.BackendType)
	}
	if !conf.EnablePrefetch {
		t.Error("expected true EnablePrefetch")
	}
	if !conf.DigestValidate {
		t.Error("expected true DigestValidate")
	}
}

func TestMakeConfigDefaultBackend(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		ConfigPath:   configPath,
		BlobCacheDir: tmpDir + "/cache",
		Mode:         "direct",
	}

	err := makeConfig(conf)
	if err != nil {
		t.Fatalf("makeConfig() error: %v", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, `"localfs"`) {
		t.Error("expected default backend type 'localfs'")
	}
	if !strings.Contains(content, `"direct"`) {
		t.Error("expected mode 'direct'")
	}
	if !strings.Contains(content, `"blobcache"`) {
		t.Error("expected cache type 'blobcache'")
	}
}

func TestMakeConfigWithBackend(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		ConfigPath:     configPath,
		BackendType:    "registry",
		BackendConfig:  `{"host": "registry.io", "repo": "test"}`,
		BlobCacheDir:   tmpDir + "/cache",
		Mode:           "direct",
		EnablePrefetch: true,
		DigestValidate: true,
	}

	err := makeConfig(conf)
	if err != nil {
		t.Fatalf("makeConfig() error: %v", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, `"registry"`) {
		t.Error("expected backend type 'registry'")
	}
	if !strings.Contains(content, "registry.io") {
		t.Error("expected host in backend config")
	}
	if !strings.Contains(content, "true") {
		t.Error("expected prefetch enabled")
	}
}

func TestMakeConfigEmptyBackendConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		ConfigPath:  configPath,
		BackendType: "registry",
		// BackendConfig intentionally empty
	}

	err := makeConfig(conf)
	if err == nil {
		t.Error("expected error for empty backend config with non-empty backend type")
	}
}

func TestMakeConfigInvalidPath(t *testing.T) {
	conf := NydusdConfig{
		ConfigPath: "/nonexistent/dir/config.json",
	}

	err := makeConfig(conf)
	if err == nil {
		t.Error("expected error for invalid config path")
	}
}

func TestNewNydusdSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		ConfigPath:   configPath,
		BlobCacheDir: tmpDir + "/cache",
		Mode:         "direct",
	}

	nydusd, err := NewNydusd(conf)
	if err != nil {
		t.Fatalf("NewNydusd() error: %v", err)
	}
	if nydusd == nil {
		t.Fatal("NewNydusd() returned nil")
	}
	if nydusd.ConfigPath != configPath {
		t.Errorf("ConfigPath = %q, want %q", nydusd.ConfigPath, configPath)
	}
}

func TestNewNydusdFailure(t *testing.T) {
	conf := NydusdConfig{
		ConfigPath:  "/nonexistent/dir/config.json",
		BackendType: "registry",
		// empty backend config should cause error
	}

	nydusd, err := NewNydusd(conf)
	if err == nil {
		t.Error("expected error from NewNydusd")
	}
	if nydusd != nil {
		t.Error("expected nil nydusd on error")
	}
}

func TestNydusdMountInvalidBinary(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		NydusdPath:    "/nonexistent/nydusd",
		ConfigPath:    configPath,
		MountPath:     tmpDir + "/mnt",
		BootstrapPath: "/dev/null",
		APISockPath:   tmpDir + "/api.sock",
		BlobCacheDir:  tmpDir + "/cache",
		Mode:          "direct",
	}

	nydusd, err := NewNydusd(conf)
	if err != nil {
		t.Fatalf("NewNydusd() error: %v", err)
	}

	err = nydusd.Mount()
	if err == nil {
		t.Error("expected error for invalid nydusd binary")
	}
}

func TestNydusdUmountNonexistentPath(t *testing.T) {
	nydusd := &Nydusd{
		NydusdConfig: NydusdConfig{
			MountPath: "/nonexistent/mount/path",
		},
	}

	err := nydusd.Umount(false)
	if err != nil {
		t.Errorf("Umount() unexpected error: %v", err)
	}
}

func TestNydusdUmountSilent(t *testing.T) {
	nydusd := &Nydusd{
		NydusdConfig: NydusdConfig{
			MountPath: "/nonexistent/mount/path",
		},
	}

	err := nydusd.Umount(true)
	if err != nil {
		t.Errorf("Umount(true) unexpected error: %v", err)
	}
}

func TestDaemonInfoJSON(t *testing.T) {
	info := daemonInfo{State: "RUNNING"}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	var decoded daemonInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.State != "RUNNING" {
		t.Errorf("State = %q, want %q", decoded.State, "RUNNING")
	}
}

func TestDaemonInfoStates(t *testing.T) {
	tests := []struct {
		state string
	}{
		{"RUNNING"},
		{"INIT"},
		{"READY"},
		{"STOPPED"},
	}
	for _, tt := range tests {
		info := daemonInfo{State: tt.state}
		if info.State != tt.state {
			t.Errorf("State = %q, want %q", info.State, tt.state)
		}
	}
}

func TestMakeConfigPrefetchSettings(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	tests := []struct {
		name     string
		prefetch bool
	}{
		{"prefetch enabled", true},
		{"prefetch disabled", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NydusdConfig{
				ConfigPath:     configPath,
				EnablePrefetch: tt.prefetch,
				BlobCacheDir:   tmpDir + "/cache",
				Mode:           "direct",
			}
			if err := makeConfig(conf); err != nil {
				t.Fatalf("makeConfig error: %v", err)
			}

			data, _ := os.ReadFile(configPath)
			content := string(data)
			if tt.prefetch {
				if !strings.Contains(content, `"enable": true`) {
					t.Error("expected prefetch enabled in config")
				}
			} else {
				if !strings.Contains(content, `"enable": false`) {
					t.Error("expected prefetch disabled in config")
				}
			}
		})
	}
}

func TestMakeConfigExternalBackend(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	conf := NydusdConfig{
		ConfigPath:                configPath,
		ExternalBackendConfigPath: "/tmp/external.json",
		BlobCacheDir:              tmpDir + "/cache",
		Mode:                      "direct",
	}

	if err := makeConfig(conf); err != nil {
		t.Fatalf("makeConfig error: %v", err)
	}

	data, _ := os.ReadFile(configPath)
	content := string(data)
	if !strings.Contains(content, "/tmp/external.json") {
		t.Error("expected external backend config path in config")
	}
}
