// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package committer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	parserPkg "github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
)

// --- Additional ValidateRef tests ---

func TestValidateRefAdditional(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		want    string
		wantErr bool
	}{
		{
			name: "image with port in registry",
			ref:  "localhost:5000/myimage:v1",
			want: "localhost:5000/myimage:v1",
		},
		{
			name: "deeply nested path",
			ref:  "registry.io/org/team/project/image:latest",
			want: "registry.io/org/team/project/image:latest",
		},
		{
			name:    "digest only is unsupported",
			ref:     "alpine@sha256:e7d88de73db3d3fd9b2d63aa7f447a10fd0220b7cbf39803c803f2af9ba256b3",
			wantErr: true,
		},
		{
			name:    "tag and digest is unsupported",
			ref:     "alpine:latest@sha256:e7d88de73db3d3fd9b2d63aa7f447a10fd0220b7cbf39803c803f2af9ba256b3",
			wantErr: true,
		},
		{
			name: "uppercase in image name is normalized",
			ref:  "docker.io/library/ALPINE:latest",
			// Docker ref parsing lowercases names
			wantErr: true,
		},
		{
			name: "with explicit latest tag",
			ref:  "myregistry.io/myimage:latest",
			want: "myregistry.io/myimage:latest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateRef(tt.ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRef(%q) error = %v, wantErr %v", tt.ref, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ValidateRef(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

// --- Additional withRetry tests ---

func TestWithRetryErrorPreserved(t *testing.T) {
	expectedErr := errors.New("specific error message")
	err := withRetry(func() error {
		return expectedErr
	}, 1)
	if err == nil {
		t.Fatal("withRetry() should return error")
	}
	if err.Error() != expectedErr.Error() {
		t.Errorf("withRetry() error = %q, want %q", err.Error(), expectedErr.Error())
	}
}

func TestWithRetryCallCount(t *testing.T) {
	tests := []struct {
		name      string
		total     int
		wantCalls int
	}{
		{name: "5 retries all fail", total: 5, wantCalls: 5},
		{name: "1 retry fails", total: 1, wantCalls: 1},
		{name: "10 retries all fail", total: 10, wantCalls: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			withRetry(func() error {
				calls++
				return errors.New("fail")
			}, tt.total)
			if calls != tt.wantCalls {
				t.Errorf("withRetry() called %d times, want %d", calls, tt.wantCalls)
			}
		})
	}
}

func TestWithRetrySuccessOnSecondToLast(t *testing.T) {
	calls := 0
	err := withRetry(func() error {
		calls++
		if calls < 4 {
			return errors.New("fail")
		}
		return nil
	}, 5)
	if err != nil {
		t.Errorf("withRetry() error = %v, want nil", err)
	}
	if calls != 4 {
		t.Errorf("expected 4 calls, got %d", calls)
	}
}

// --- MountList additional tests ---

func TestMountListAddOrder(t *testing.T) {
	ml := NewMountList()
	paths := []string{"/z/path", "/a/path", "/m/path"}
	for _, p := range paths {
		ml.Add(p)
	}
	for i, p := range paths {
		if ml.paths[i] != p {
			t.Errorf("path[%d] = %q, want %q", i, ml.paths[i], p)
		}
	}
}

func TestMountListConcurrentAddsVerifyCount(t *testing.T) {
	ml := NewMountList()
	var wg sync.WaitGroup
	count := 200
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(idx int) {
			defer wg.Done()
			ml.Add(fmt.Sprintf("/path/%d", idx))
		}(i)
	}
	wg.Wait()
	if len(ml.paths) != count {
		t.Errorf("expected %d paths, got %d", count, len(ml.paths))
	}
}

// --- Manager tests ---

func TestNewManager(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{
			name: "valid address",
			addr: "/run/containerd/containerd.sock",
		},
		{
			name: "empty address",
			addr: "",
		},
		{
			name: "tcp address",
			addr: "tcp://localhost:2375",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewManager(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewManager(%q) error = %v, wantErr %v", tt.addr, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if m == nil {
					t.Fatal("NewManager() returned nil")
				}
				if m.address != tt.addr {
					t.Errorf("Manager.address = %q, want %q", m.address, tt.addr)
				}
			}
		})
	}
}

// --- InspectResult and Mount tests ---

func TestInspectResultFields(t *testing.T) {
	result := &InspectResult{
		LowerDirs: "/lower1:/lower2",
		UpperDir:  "/upper",
		Image:     "docker.io/library/alpine:latest",
		Mounts: []Mount{
			{Destination: "/data", Source: "/host/data"},
		},
		Pid: 12345,
	}

	if result.LowerDirs != "/lower1:/lower2" {
		t.Errorf("LowerDirs = %q", result.LowerDirs)
	}
	if result.UpperDir != "/upper" {
		t.Errorf("UpperDir = %q", result.UpperDir)
	}
	if result.Image != "docker.io/library/alpine:latest" {
		t.Errorf("Image = %q", result.Image)
	}
	if len(result.Mounts) != 1 {
		t.Fatalf("Mounts len = %d", len(result.Mounts))
	}
	if result.Mounts[0].Destination != "/data" {
		t.Errorf("Mount.Destination = %q", result.Mounts[0].Destination)
	}
	if result.Mounts[0].Source != "/host/data" {
		t.Errorf("Mount.Source = %q", result.Mounts[0].Source)
	}
	if result.Pid != 12345 {
		t.Errorf("Pid = %d", result.Pid)
	}
}

// --- nsenter Config tests ---

func TestConfigBuildCommand(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		wantArgs []string
	}{
		{
			name:    "no target returns error",
			config:  Config{},
			wantErr: true,
		},
		{
			name: "target only",
			config: Config{
				Target: 1234,
			},
			wantArgs: []string{"--target", "1234"},
		},
		{
			name: "mount and pid",
			config: Config{
				Target: 1234,
				Mount:  true,
				PID:    true,
			},
			wantArgs: []string{"--target", "1234", "--mount", "--pid"},
		},
		{
			name: "with custom mount file",
			config: Config{
				Target:    1234,
				Mount:     true,
				MountFile: "/proc/1234/ns/mnt",
			},
			wantArgs: []string{"--target", "1234", "--mount=/proc/1234/ns/mnt"},
		},
		{
			name: "with net",
			config: Config{
				Target: 1234,
				Net:    true,
			},
			wantArgs: []string{"--target", "1234", "--net"},
		},
		{
			name: "with custom net file",
			config: Config{
				Target:  1234,
				Net:     true,
				NetFile: "/proc/1234/ns/net",
			},
			wantArgs: []string{"--target", "1234", "--net=/proc/1234/ns/net"},
		},
		{
			name: "with ipc",
			config: Config{
				Target: 1234,
				IPC:    true,
			},
			wantArgs: []string{"--target", "1234", "--ipc"},
		},
		{
			name: "with cgroup",
			config: Config{
				Target: 1234,
				Cgroup: true,
			},
			wantArgs: []string{"--target", "1234", "--cgroup"},
		},
		{
			name: "with cgroup file",
			config: Config{
				Target:     1234,
				Cgroup:     true,
				CgroupFile: "/proc/1234/ns/cgroup",
			},
			wantArgs: []string{"--target", "1234", "--cgroup=/proc/1234/ns/cgroup"},
		},
		{
			name: "with user namespace",
			config: Config{
				Target: 1234,
				User:   true,
			},
			wantArgs: []string{"--target", "1234", "--user"},
		},
		{
			name: "with UTS namespace",
			config: Config{
				Target: 1234,
				UTS:    true,
			},
			wantArgs: []string{"--target", "1234", "--uts"},
		},
		{
			name: "with UID and GID",
			config: Config{
				Target: 1234,
				UID:    1000,
				GID:    1000,
			},
			wantArgs: []string{"--target", "1234", "--setgid", "1000", "--setuid", "1000"},
		},
		{
			name: "with follow-context",
			config: Config{
				Target:        1234,
				FollowContext: true,
			},
			wantArgs: []string{"--target", "1234", "--follow-context"},
		},
		{
			name: "with no-fork",
			config: Config{
				Target: 1234,
				NoFork: true,
			},
			wantArgs: []string{"--target", "1234", "--no-fork"},
		},
		{
			name: "with preserve-credentials",
			config: Config{
				Target:              1234,
				PreserveCredentials: true,
			},
			wantArgs: []string{"--target", "1234", "--preserve-credentials"},
		},
		{
			name: "with root directory",
			config: Config{
				Target:        1234,
				RootDirectory: "/new/root",
			},
			wantArgs: []string{"--target", "1234", "--root", "/new/root"},
		},
		{
			name: "with working directory",
			config: Config{
				Target:           1234,
				WorkingDirectory: "/work/dir",
			},
			wantArgs: []string{"--target", "1234", "--wd", "/work/dir"},
		},
		{
			name: "all options combined",
			config: Config{
				Target:              1234,
				Mount:               true,
				PID:                 true,
				Net:                 true,
				IPC:                 true,
				User:                true,
				UTS:                 true,
				Cgroup:              true,
				FollowContext:       true,
				NoFork:              true,
				PreserveCredentials: true,
				UID:                 1000,
				GID:                 1000,
				RootDirectory:       "/root",
				WorkingDirectory:    "/work",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := tt.config.buildCommand(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("buildCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if cmd == nil {
				t.Fatal("buildCommand() returned nil cmd")
			}

			// Verify expected args are present
			args := strings.Join(cmd.Args, " ")
			for _, wantArg := range tt.wantArgs {
				if !strings.Contains(args, wantArg) {
					t.Errorf("command args %q missing expected %q", args, wantArg)
				}
			}
		})
	}
}

// --- ContainerWalker tests ---

func TestNewContainerWalker(t *testing.T) {
	called := false
	onFound := func(ctx context.Context, found Found) error {
		called = true
		return nil
	}

	// We can't create a real client, but we can test the constructor
	walker := NewContainerWalker(nil, onFound)
	if walker == nil {
		t.Fatal("NewContainerWalker() returned nil")
	}
	if walker.Client != nil {
		t.Error("Client should be nil")
	}
	if walker.OnFound == nil {
		t.Error("OnFound should not be nil")
	}
	_ = called
}

// --- Counter additional tests ---

func TestCounterLargeWrites(t *testing.T) {
	c := &Counter{}
	data := make([]byte, 1024*1024) // 1MB
	n, err := c.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}
	if c.Size() != int64(len(data)) {
		t.Errorf("Size() = %d, want %d", c.Size(), len(data))
	}
}

func TestCounterMultipleSequentialWrites(t *testing.T) {
	c := &Counter{}
	total := int64(0)
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		n, err := c.Write(data)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		total += int64(n)
	}
	if c.Size() != total {
		t.Errorf("Size() = %d, want %d", c.Size(), total)
	}
}

// --- makeDesc tests ---

func TestMakeDesc(t *testing.T) {
	cm := &Committer{
		workDir: t.TempDir(),
	}

	t.Run("simple struct", func(t *testing.T) {
		input := map[string]string{"key": "value"}
		oldDesc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageConfig,
			Size:      999,
		}

		data, desc, err := cm.makeDesc(input, oldDesc)
		if err != nil {
			t.Fatalf("makeDesc() error = %v", err)
		}
		if desc == nil {
			t.Fatal("makeDesc() returned nil desc")
		}
		if len(data) == 0 {
			t.Error("makeDesc() returned empty data")
		}
		if desc.MediaType != ocispec.MediaTypeImageConfig {
			t.Errorf("desc.MediaType = %q, want %q", desc.MediaType, ocispec.MediaTypeImageConfig)
		}
		if desc.Size != int64(len(data)) {
			t.Errorf("desc.Size = %d, want %d", desc.Size, len(data))
		}
		if desc.Digest.String() == "" {
			t.Error("desc.Digest should not be empty")
		}

		// Verify the data can be unmarshaled back
		var result map[string]string
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("failed to unmarshal result: %v", err)
		}
		if result["key"] != "value" {
			t.Errorf("result[\"key\"] = %q, want %q", result["key"], "value")
		}
	})

	t.Run("preserves media type from old desc", func(t *testing.T) {
		oldDesc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
		}
		_, desc, err := cm.makeDesc(struct{}{}, oldDesc)
		if err != nil {
			t.Fatalf("makeDesc() error = %v", err)
		}
		if desc.MediaType != ocispec.MediaTypeImageManifest {
			t.Errorf("desc.MediaType = %q, want %q", desc.MediaType, ocispec.MediaTypeImageManifest)
		}
	})

	t.Run("different inputs produce different digests", func(t *testing.T) {
		oldDesc := ocispec.Descriptor{}
		_, desc1, _ := cm.makeDesc(map[string]string{"a": "1"}, oldDesc)
		_, desc2, _ := cm.makeDesc(map[string]string{"b": "2"}, oldDesc)
		if desc1.Digest == desc2.Digest {
			t.Error("different inputs should produce different digests")
		}
	})
}

// --- getDistributionSourceLabel tests ---

func TestGetDistributionSourceLabel(t *testing.T) {
	tests := []struct {
		name       string
		sourceRef  string
		wantKey    string
		wantValue  string
		wantEmpty  bool
	}{
		{
			name:      "docker hub image",
			sourceRef: "docker.io/library/alpine:latest",
			wantKey:   "containerd.io/distribution.source.docker.io",
			wantValue: "library/alpine",
		},
		{
			name:      "custom registry",
			sourceRef: "myregistry.io/myimage:v1",
			wantKey:   "containerd.io/distribution.source.myregistry.io",
			wantValue: "myimage",
		},
		{
			name:      "invalid ref returns empty",
			sourceRef: "INVALID:://ref",
			wantEmpty: true,
		},
		{
			name:      "registry with port",
			sourceRef: "localhost:5000/myimage:v1",
			wantKey:   "containerd.io/distribution.source.localhost:5000",
			wantValue: "myimage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, value := getDistributionSourceLabel(tt.sourceRef)
			if tt.wantEmpty {
				if key != "" || value != "" {
					t.Errorf("getDistributionSourceLabel(%q) = (%q, %q), want empty", tt.sourceRef, key, value)
				}
				return
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
			if value != tt.wantValue {
				t.Errorf("value = %q, want %q", value, tt.wantValue)
			}
		})
	}
}

// --- outputJSON tests ---

func TestOutputJSON(t *testing.T) {
	tests := []struct {
		name       string
		jsonStr    string
		wantFsVer  string
		wantCompr  string
		wantErr    bool
	}{
		{
			name:      "valid json",
			jsonStr:   `{"fs_version":"6","compressor":"zstd"}`,
			wantFsVer: "6",
			wantCompr: "zstd",
		},
		{
			name:      "v5 with lz4",
			jsonStr:   `{"fs_version":"5","compressor":"lz4_block"}`,
			wantFsVer: "5",
			wantCompr: "lz4_block",
		},
		{
			name:    "invalid json",
			jsonStr: `{invalid}`,
			wantErr: true,
		},
		{
			name:      "empty fields",
			jsonStr:   `{"fs_version":"","compressor":""}`,
			wantFsVer: "",
			wantCompr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output outputJSON
			err := json.Unmarshal([]byte(tt.jsonStr), &output)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if output.FsVersion != tt.wantFsVer {
					t.Errorf("FsVersion = %q, want %q", output.FsVersion, tt.wantFsVer)
				}
				if output.Compressor != tt.wantCompr {
					t.Errorf("Compressor = %q, want %q", output.Compressor, tt.wantCompr)
				}
			}
		})
	}
}

// --- Committer field tests ---

func TestCommitterFields(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/test/socket")

	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
		manager: m,
	}

	if cm.workDir != tmpDir {
		t.Errorf("workDir = %q, want %q", cm.workDir, tmpDir)
	}
	if cm.builder != "/usr/bin/nydus-image" {
		t.Errorf("builder = %q", cm.builder)
	}
	if cm.manager == nil {
		t.Error("manager should not be nil")
	}
}

// --- resolveContainerID edge cases ---

func TestResolveContainerIDFullID(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		manager: m,
	}

	// A 64-char ID should be returned immediately without contacting containerd
	fullID := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	opt := &Opt{
		ContainerID: fullID,
	}

	err := cm.resolveContainerID(context.Background(), opt)
	if err != nil {
		t.Errorf("resolveContainerID() with full ID should not error, got: %v", err)
	}
	if opt.ContainerID != fullID {
		t.Errorf("ContainerID = %q, want %q", opt.ContainerID, fullID)
	}
}

func TestResolveContainerIDShortIDFailsWithoutContainerd(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		manager: m,
	}

	opt := &Opt{
		ContainerID: "abc123", // Short ID, will try to connect to containerd
		Namespace:   "default",
	}

	err := cm.resolveContainerID(context.Background(), opt)
	if err == nil {
		t.Error("resolveContainerID() with short ID and no containerd should error")
	}
}

// --- Found struct tests ---

func TestFoundStruct(t *testing.T) {
	f := Found{
		Req:        "abc",
		MatchIndex: 0,
		MatchCount: 3,
	}
	if f.Req != "abc" {
		t.Errorf("Req = %q", f.Req)
	}
	if f.MatchIndex != 0 {
		t.Errorf("MatchIndex = %d", f.MatchIndex)
	}
	if f.MatchCount != 3 {
		t.Errorf("MatchCount = %d", f.MatchCount)
	}
}

// --- Opt struct tests ---

func TestOptFields(t *testing.T) {
	opt := Opt{
		WorkDir:           "/work",
		ContainerdAddress: "/run/containerd.sock",
		NydusImagePath:    "/usr/bin/nydus-image",
		Namespace:         "default",
		ContainerID:       "abc123",
		SourceInsecure:    true,
		TargetRef:         "myregistry.io/image:v1",
		TargetInsecure:    false,
		MaximumTimes:      5,
		FsVersion:         "6",
		Compressor:        "zstd",
		WithPaths:         []string{"/data", "/config"},
		WithoutPaths:      []string{"/tmp", "/var/log"},
	}

	if opt.WorkDir != "/work" {
		t.Errorf("WorkDir = %q", opt.WorkDir)
	}
	if len(opt.WithPaths) != 2 {
		t.Errorf("WithPaths len = %d", len(opt.WithPaths))
	}
	if len(opt.WithoutPaths) != 2 {
		t.Errorf("WithoutPaths len = %d", len(opt.WithoutPaths))
	}
	if opt.MaximumTimes != 5 {
		t.Errorf("MaximumTimes = %d", opt.MaximumTimes)
	}
}

// --- copyFromContainer test (will fail because nsenter is not available, testing error path) ---

func TestCopyFromContainerInvalidPID(t *testing.T) {
	err := copyFromContainer(context.Background(), 0, "/source", io.Discard)
	if err == nil {
		t.Error("copyFromContainer() with PID 0 should error (nsenter requires target)")
	}
}

// --- Config Execute tests ---

func TestConfigExecuteNoTarget(t *testing.T) {
	c := &Config{}
	_, err := c.Execute(io.Discard, "echo", "hello")
	if err == nil {
		t.Error("Execute() without target should return error")
	}
}

// --- obtainBootStrapInfo tests (error paths) ---

func TestObtainBootStrapInfoNoFile(t *testing.T) {
	tmpDir := t.TempDir()
	cm := &Committer{
		workDir: tmpDir,
		builder: "/nonexistent/nydus-image",
	}

	_, _, err := cm.obtainBootStrapInfo(context.Background(), "nonexistent-bootstrap")
	if err == nil {
		t.Error("obtainBootStrapInfo() with nonexistent builder should error")
	}
}

// --- commitUpperByDiff error paths ---

func TestCommitUpperByDiffCreateBlobError(t *testing.T) {
	// Use a path that doesn't exist to cause os.Create to fail
	cm := &Committer{
		workDir: "/nonexistent/path/that/doesnt/exist",
		builder: "/usr/bin/nydus-image",
	}

	appendMount := func(path string) {}
	_, err := cm.commitUpperByDiff(
		context.Background(),
		appendMount,
		nil,
		nil,
		"/lower",
		"/upper",
		"test-blob",
		"6",
		"zstd",
	)
	if err == nil {
		t.Error("commitUpperByDiff() with invalid workDir should error")
	}
}

// --- mergeBootstrap error paths ---

func TestMergeBootstrapMissingFiles(t *testing.T) {
	tmpDir := t.TempDir()
	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
	}

	upperBlob := Blob{
		Name: "nonexistent-blob",
	}

	_, _, err := cm.mergeBootstrap(
		context.Background(),
		upperBlob,
		nil,
		"nonexistent-bootstrap",
		"merged-bootstrap.tar",
	)
	if err == nil {
		t.Error("mergeBootstrap() with missing files should error")
	}
}

// --- Blob struct tests ---

func TestBlobStructDescriptor(t *testing.T) {
	b := Blob{
		Name:          "test-blob",
		BootstrapName: "test-bootstrap",
		Desc: ocispec.Descriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Size:      1024,
		},
	}

	if b.Desc.Size != 1024 {
		t.Errorf("Desc.Size = %d, want 1024", b.Desc.Size)
	}
	if b.Desc.MediaType != "application/vnd.oci.image.layer.v1.tar" {
		t.Errorf("Desc.MediaType = %q", b.Desc.MediaType)
	}
}

// --- pushBlob error paths ---

func TestPushBlobInvalidWorkDir(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
		manager: m,
	}

	image := &parserPkg.Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{},
		},
	}

	// Local blob (not a mount blob) that doesn't exist on disk
	_, err := cm.pushBlob(
		context.Background(),
		"blob-upper",
		"sha256:abc123",
		"docker.io/library/alpine:latest",
		"myregistry.io/image:v1",
		false,
		image,
	)
	if err == nil {
		t.Error("pushBlob() with nonexistent local blob should error")
	}
}

func TestPushBlobLowerBlobNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
		manager: m,
	}

	image := &parserPkg.Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				// Layer with different digest
				{
					Digest:    "sha256:different",
					MediaType: "application/vnd.oci.image.layer.nydus.blob.v1",
				},
			},
		},
	}

	// Lower blob (blob-mount-*) with digest not in manifest
	_, err := cm.pushBlob(
		context.Background(),
		"blob-mount-0",
		"sha256:notfound",
		"docker.io/library/alpine:latest",
		"myregistry.io/image:v1",
		false,
		image,
	)
	if err == nil {
		t.Error("pushBlob() with unfound lower blob should error")
	}
}

// --- commitMountByNSEnter error paths ---

func TestCommitMountByNSEnterInvalidWorkDir(t *testing.T) {
	cm := &Committer{
		workDir: "/nonexistent/path/that/doesnt/exist",
		builder: "/usr/bin/nydus-image",
	}

	_, err := cm.commitMountByNSEnter(
		context.Background(),
		12345,
		"/source",
		"test-blob",
		"6",
		"zstd",
	)
	if err == nil {
		t.Error("commitMountByNSEnter() with invalid workDir should error")
	}
}

// --- pushManifest error paths ---

func TestPushManifestInvalidBootstrap(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
		manager: m,
	}

	nydusImage := parserPkg.Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{},
			Config: ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageConfig,
			},
		},
		Config: ocispec.Image{},
	}

	upperBlob := &Blob{
		Name: "blob-upper",
		Desc: ocispec.Descriptor{},
	}

	err := cm.pushManifest(
		context.Background(),
		nydusImage,
		"sha256:abc",
		"myregistry.io/image:v1",
		"nonexistent-bootstrap.tar",
		"6",
		upperBlob,
		nil,
		false,
	)
	if err == nil {
		t.Error("pushManifest() with nonexistent bootstrap should error")
	}
}

// --- Commit error paths ---

func TestCommitInvalidTarget(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		builder: "/usr/bin/nydus-image",
		manager: m,
	}

	opt := Opt{
		ContainerID: strings.Repeat("a", 64),
		Namespace:   "default",
		TargetRef:   "INVALID:://ref",
	}

	err := cm.Commit(context.Background(), opt)
	if err == nil {
		t.Error("Commit() with invalid target should error")
	}
}

// --- pause error paths ---

func TestPauseFails(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		manager: m,
	}

	err := cm.pause(context.Background(), "test-container", func() error {
		return nil
	})
	// Will fail because the manager can't connect to containerd
	if err == nil {
		t.Error("pause() without containerd should error")
	}
}

// --- syncFilesystem error paths ---

func TestSyncFilesystemFails(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := NewManager("/nonexistent/socket")
	cm := &Committer{
		workDir: tmpDir,
		manager: m,
	}

	err := cm.syncFilesystem(context.Background(), "nonexistent-container")
	if err == nil {
		t.Error("syncFilesystem() without containerd should error")
	}
}

// --- NewCommitter tests ---

func TestNewCommitterCreatesWorkDir(t *testing.T) {
	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "test-workdir")

	// This will fail because there's no containerd socket, but we can
	// verify the work dir handling
	_, err := NewCommitter(Opt{
		WorkDir:           workDir,
		ContainerdAddress: "/nonexistent/socket",
	})
	// NewCommitter calls NewManager which just stores address, so no error for address
	if err != nil {
		t.Logf("NewCommitter() error (expected if no containerd): %v", err)
	}
	// Verify the work directory was created
	if _, statErr := os.Stat(workDir); statErr != nil {
		t.Errorf("work directory %q was not created", workDir)
	}
}

func TestNewCommitterTempDir(t *testing.T) {
	tmpDir := t.TempDir()

	cm, err := NewCommitter(Opt{
		WorkDir:           tmpDir,
		ContainerdAddress: "/nonexistent/socket",
		NydusImagePath:    "/usr/bin/nydus-image",
	})
	if err != nil {
		t.Fatalf("NewCommitter() error = %v", err)
	}
	if cm == nil {
		t.Fatal("NewCommitter() returned nil")
	}
	if cm.builder != "/usr/bin/nydus-image" {
		t.Errorf("builder = %q, want /usr/bin/nydus-image", cm.builder)
	}
	if cm.workDir == "" {
		t.Error("workDir should not be empty")
	}
	if !strings.HasPrefix(cm.workDir, tmpDir) {
		t.Errorf("workDir %q should start with %q", cm.workDir, tmpDir)
	}
}
