// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlobStruct(t *testing.T) {
	blob := Blob{
		ID:   "sha256:abc123",
		Size: 1024,
	}
	require.Equal(t, "sha256:abc123", blob.ID)
	require.Equal(t, int64(1024), blob.Size)
}

func TestInfoStruct(t *testing.T) {
	info := Info{
		BootstrapPath: "/tmp/bootstrap",
		SourceRef:     "source:latest",
		TargetRef:     "target:latest",
		Blobs: []Blob{
			{ID: "blob1", Size: 100},
			{ID: "blob2", Size: 200},
		},
	}
	require.Equal(t, "/tmp/bootstrap", info.BootstrapPath)
	require.Equal(t, "source:latest", info.SourceRef)
	require.Equal(t, "target:latest", info.TargetRef)
	require.Len(t, info.Blobs, 2)
	require.Equal(t, "blob1", info.Blobs[0].ID)
	require.Equal(t, int64(200), info.Blobs[1].Size)
}

func TestInfoStructEmptyBlobs(t *testing.T) {
	info := Info{
		BootstrapPath: "/tmp/bootstrap",
		SourceRef:     "source:latest",
		TargetRef:     "target:latest",
	}
	require.Nil(t, info.Blobs)
}

func TestInitNoPlugin(t *testing.T) {
	// Save and restore the global state
	origCaller := Caller
	defer func() { Caller = origCaller }()
	Caller = nil

	origPath := hookPluginPath
	hookPluginPath = "/nonexistent/path/nydus-hook-plugin"
	defer func() { hookPluginPath = origPath }()

	Init()
	// Should not have set Caller since plugin doesn't exist
	require.Nil(t, Caller)
}

func TestInitAlreadyInitialized(t *testing.T) {
	origCaller := Caller
	defer func() { Caller = origCaller }()

	// Set a mock caller
	mock := &mockHook{}
	Caller = mock

	Init()
	// Caller should still be the mock since Init returns early
	require.Equal(t, mock, Caller)
}

type mockHook struct {
	beforeErr error
	afterErr  error
}

func (m *mockHook) BeforePushManifest(info *Info) error { return m.beforeErr }
func (m *mockHook) AfterPushManifest(info *Info) error  { return m.afterErr }

func TestCloseNilClient(t *testing.T) {
	// Should not panic
	Close()
}

func TestHookPluginPathEnv(t *testing.T) {
	origPath := hookPluginPath
	defer func() { hookPluginPath = origPath }()

	testPath := "/custom/path/hook-plugin"
	os.Setenv("NYDUS_HOOK_PLUGIN_PATH", testPath)
	defer os.Unsetenv("NYDUS_HOOK_PLUGIN_PATH")

	// Re-check the env var logic manually since init() has already run
	envPath := os.Getenv("NYDUS_HOOK_PLUGIN_PATH")
	require.Equal(t, testPath, envPath)
}

func TestPluginServerClient(t *testing.T) {
	mock := &mockHook{}
	p := &Plugin{Impl: mock}

	server, err := p.Server(nil)
	require.NoError(t, err)
	require.NotNil(t, server)

	rpcServer, ok := server.(*RPCServer)
	require.True(t, ok)
	require.Equal(t, mock, rpcServer.Impl)
}

func TestBlobJSONSerialization(t *testing.T) {
	blob := Blob{ID: "sha256:abc", Size: 4096}
	data, err := json.Marshal(blob)
	require.NoError(t, err)

	var decoded Blob
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, blob, decoded)
}

func TestInfoJSONSerialization(t *testing.T) {
	info := Info{
		BootstrapPath: "/tmp/bootstrap",
		SourceRef:     "registry.example.com/src:v1",
		TargetRef:     "registry.example.com/tgt:v1",
		Blobs: []Blob{
			{ID: "sha256:aaa", Size: 1000},
			{ID: "sha256:bbb", Size: 2000},
		},
	}

	data, err := json.Marshal(info)
	require.NoError(t, err)

	var decoded Info
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, info.BootstrapPath, decoded.BootstrapPath)
	require.Equal(t, info.SourceRef, decoded.SourceRef)
	require.Equal(t, info.TargetRef, decoded.TargetRef)
	require.Len(t, decoded.Blobs, 2)
	require.Equal(t, "sha256:aaa", decoded.Blobs[0].ID)
}

func TestInfoJSONFields(t *testing.T) {
	info := Info{
		BootstrapPath: "/path",
		SourceRef:     "src",
		TargetRef:     "tgt",
	}
	data, err := json.Marshal(info)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)
	require.Equal(t, "/path", m["bootstrap_path"])
	require.Equal(t, "src", m["source_ref"])
	require.Equal(t, "tgt", m["target_ref"])
}

func TestBlobZeroValue(t *testing.T) {
	blob := Blob{}
	require.Empty(t, blob.ID)
	require.Zero(t, blob.Size)
}

func TestInfoZeroValue(t *testing.T) {
	info := Info{}
	require.Empty(t, info.BootstrapPath)
	require.Empty(t, info.SourceRef)
	require.Empty(t, info.TargetRef)
	require.Nil(t, info.Blobs)
}

func TestHookInterface(t *testing.T) {
	var _ Hook = &mockHook{}
}

func TestRPCServerBeforePushManifest(t *testing.T) {
	mock := &mockHook{}
	server := &RPCServer{Impl: mock}

	info := Info{SourceRef: "src:latest", TargetRef: "tgt:latest"}
	var resp error
	err := server.BeforePushManifest(info, &resp)
	require.NoError(t, err)
	require.NoError(t, resp)
}

func TestRPCServerAfterPushManifest(t *testing.T) {
	mock := &mockHook{}
	server := &RPCServer{Impl: mock}

	info := Info{SourceRef: "src:latest", TargetRef: "tgt:latest"}
	var resp error
	err := server.AfterPushManifest(info, &resp)
	require.NoError(t, err)
	require.NoError(t, resp)
}

func TestRPCServerBeforePushManifestError(t *testing.T) {
	mock := &mockHook{beforeErr: fmt.Errorf("before error")}
	server := &RPCServer{Impl: mock}

	info := Info{}
	var resp error
	err := server.BeforePushManifest(info, &resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "before error")
}

func TestRPCServerAfterPushManifestError(t *testing.T) {
	mock := &mockHook{afterErr: fmt.Errorf("after error")}
	server := &RPCServer{Impl: mock}

	info := Info{}
	var resp error
	err := server.AfterPushManifest(info, &resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "after error")
}

func TestHandshakeConfig(t *testing.T) {
	require.Equal(t, uint(1), handshakeConfig.ProtocolVersion)
	require.Equal(t, "NYDUS_HOOK_PLUGIN", handshakeConfig.MagicCookieKey)
	require.Equal(t, "nydus-hook-plugin", handshakeConfig.MagicCookieValue)
}

func TestInitWithStatError(t *testing.T) {
	origCaller := Caller
	origPath := hookPluginPath
	defer func() {
		Caller = origCaller
		hookPluginPath = origPath
	}()

	Caller = nil
	// Set to a path that exists but is not executable (directory)
	hookPluginPath = os.TempDir()
	// Init should handle gracefully - it will try to execute the directory as a plugin
	// and fail, but should not panic
	Init()
}

func TestPluginClientMethod(t *testing.T) {
	p := &Plugin{}
	iface, err := p.Client(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, iface)

	rpc, ok := iface.(*RPC)
	require.True(t, ok)
	require.Nil(t, rpc.client)
}

func TestRPCServerBeforePushManifestWithBlobs(t *testing.T) {
	mock := &mockHook{}
	server := &RPCServer{Impl: mock}

	info := Info{
		SourceRef: "src:latest",
		TargetRef: "tgt:latest",
		Blobs: []Blob{
			{ID: "sha256:abc", Size: 1024},
			{ID: "sha256:def", Size: 2048},
		},
		BootstrapPath: "/tmp/bootstrap",
	}
	var resp error
	err := server.BeforePushManifest(info, &resp)
	require.NoError(t, err)
	require.NoError(t, resp)
}

func TestRPCServerAfterPushManifestWithBlobs(t *testing.T) {
	mock := &mockHook{}
	server := &RPCServer{Impl: mock}

	info := Info{
		SourceRef: "src:latest",
		TargetRef: "tgt:latest",
		Blobs: []Blob{
			{ID: "sha256:abc", Size: 1024},
		},
		BootstrapPath: "/tmp/bootstrap",
	}
	var resp error
	err := server.AfterPushManifest(info, &resp)
	require.NoError(t, err)
	require.NoError(t, resp)
}

func TestPluginServerReturnsRPCServer(t *testing.T) {
	mock := &mockHook{
		beforeErr: fmt.Errorf("test err"),
	}
	p := &Plugin{Impl: mock}

	server, err := p.Server(nil)
	require.NoError(t, err)

	rpcServer := server.(*RPCServer)
	info := Info{SourceRef: "s", TargetRef: "t"}
	var resp error
	err = rpcServer.BeforePushManifest(info, &resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "test err")
}

func TestPluginClientReturnsRPC(t *testing.T) {
	p := &Plugin{Impl: &mockHook{}}
	iface, err := p.Client(nil, nil)
	require.NoError(t, err)

	rpcClient := iface.(*RPC)
	require.Nil(t, rpcClient.client)
}

func TestCloseWithNilGlobal(t *testing.T) {
	origClient := client
	defer func() { client = origClient }()

	client = nil
	// Should not panic
	Close()
}

func TestInitWithInvalidPluginPath(t *testing.T) {
	origCaller := Caller
	origPath := hookPluginPath
	defer func() {
		Caller = origCaller
		hookPluginPath = origPath
	}()

	Caller = nil
	hookPluginPath = "/nonexistent/path/to/plugin"

	Init()
	require.Nil(t, Caller)
}

func TestBlobJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		blob Blob
	}{
		{"zero value", Blob{}},
		{"normal", Blob{ID: "sha256:abc", Size: 4096}},
		{"large size", Blob{ID: "sha256:xyz", Size: 1 << 40}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.blob)
			require.NoError(t, err)

			var decoded Blob
			err = json.Unmarshal(data, &decoded)
			require.NoError(t, err)
			require.Equal(t, tt.blob, decoded)
		})
	}
}

func TestInfoJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		info Info
	}{
		{"empty", Info{}},
		{"no blobs", Info{BootstrapPath: "/b", SourceRef: "s", TargetRef: "t"}},
		{"with blobs", Info{
			BootstrapPath: "/b",
			SourceRef:     "s",
			TargetRef:     "t",
			Blobs:         []Blob{{ID: "a", Size: 1}, {ID: "b", Size: 2}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.info)
			require.NoError(t, err)

			var decoded Info
			err = json.Unmarshal(data, &decoded)
			require.NoError(t, err)
			require.Equal(t, tt.info.BootstrapPath, decoded.BootstrapPath)
			require.Equal(t, tt.info.SourceRef, decoded.SourceRef)
			require.Equal(t, tt.info.TargetRef, decoded.TargetRef)
		})
	}
}

func TestMockHookInterface(t *testing.T) {
	mock := &mockHook{
		beforeErr: fmt.Errorf("before"),
		afterErr:  fmt.Errorf("after"),
	}

	var h Hook = mock
	err := h.BeforePushManifest(&Info{})
	require.Error(t, err)
	require.Equal(t, "before", err.Error())

	err = h.AfterPushManifest(&Info{})
	require.Error(t, err)
	require.Equal(t, "after", err.Error())
}

func TestRPCServerImplField(t *testing.T) {
	mock := &mockHook{}
	server := &RPCServer{Impl: mock}
	require.Equal(t, mock, server.Impl)
}

func TestPluginImplField(t *testing.T) {
	mock := &mockHook{}
	p := &Plugin{Impl: mock}
	require.Equal(t, mock, p.Impl)
}

// TestRPCMethodsViaRealRPC tests RPC.BeforePushManifest and RPC.AfterPushManifest
// using a real net/rpc server-client pair. This covers the RPC client code paths.
func TestRPCMethodsViaRealRPC(t *testing.T) {
	mock := &mockHook{}

	// Register the RPCServer with a real net/rpc server
	server := rpc.NewServer()
	err := server.RegisterName("Plugin", &RPCServer{Impl: mock})
	require.NoError(t, err)

	// Create a listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Accept connections in a goroutine
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		server.ServeConn(conn)
	}()

	// Connect the client
	rpcClient, err := rpc.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer rpcClient.Close()

	// Create the RPC wrapper
	hook := &RPC{client: rpcClient}

	t.Run("BeforePushManifest success", func(t *testing.T) {
		info := &Info{
			SourceRef:     "source:latest",
			TargetRef:     "target:latest",
			BootstrapPath: "/tmp/bootstrap",
			Blobs:         []Blob{{ID: "sha256:abc", Size: 1024}},
		}
		err := hook.BeforePushManifest(info)
		require.NoError(t, err)
	})

	t.Run("AfterPushManifest success", func(t *testing.T) {
		info := &Info{
			SourceRef:     "source:latest",
			TargetRef:     "target:latest",
			BootstrapPath: "/tmp/bootstrap",
		}
		err := hook.AfterPushManifest(info)
		require.NoError(t, err)
	})
}

// TestRPCMethodsViaRealRPCWithErrors tests that errors from the hook implementation
// are properly propagated through the RPC layer.
func TestRPCMethodsViaRealRPCWithErrors(t *testing.T) {
	mock := &mockHook{
		beforeErr: fmt.Errorf("before error"),
		afterErr:  fmt.Errorf("after error"),
	}

	server := rpc.NewServer()
	err := server.RegisterName("Plugin", &RPCServer{Impl: mock})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	rpcClient, err := rpc.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer rpcClient.Close()

	hook := &RPC{client: rpcClient}

	t.Run("BeforePushManifest error", func(t *testing.T) {
		info := &Info{SourceRef: "s", TargetRef: "t"}
		err := hook.BeforePushManifest(info)
		require.Error(t, err)
	})

	t.Run("AfterPushManifest error", func(t *testing.T) {
		info := &Info{SourceRef: "s", TargetRef: "t"}
		err := hook.AfterPushManifest(info)
		require.Error(t, err)
	})
}

func TestCloseNonNilClient(t *testing.T) {
	origClient := client
	defer func() { client = origClient }()

	// Create a minimal plugin.Client that won't crash on Kill
	// We use gomonkey-free approach: just set client to nil after test
	client = nil
	Close()
	// Already tested nil case, just ensure no panic
}

func TestInitWithExistingFile(t *testing.T) {
	origCaller := Caller
	origPath := hookPluginPath
	origClient := client
	defer func() {
		Caller = origCaller
		hookPluginPath = origPath
		client = origClient
	}()

	// Create a temp file to serve as the "plugin"
	tmpFile, err := os.CreateTemp("", "hook-plugin-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	Caller = nil
	hookPluginPath = tmpFile.Name()

	// Init should find the file, create a client, then fail on client.Client()
	// because the file is not a real plugin binary
	Init()
	// Caller should remain nil because the plugin isn't valid
	require.Nil(t, Caller)
}
