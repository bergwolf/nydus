package viewer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/checker/tool"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/snapshotter/external/backend"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFsViewer(t *testing.T) {
	var remoter = remote.Remote{}
	defaultRemotePatches := gomonkey.ApplyFunc(provider.DefaultRemote, func(string, bool) (*remote.Remote, error) {
		return &remoter, nil
	})
	defer defaultRemotePatches.Reset()

	var targetParser = parser.Parser{}
	parserNewPatches := gomonkey.ApplyFunc(parser.New, func(*remote.Remote, string) (*parser.Parser, error) {
		return &targetParser, nil
	})
	defer parserNewPatches.Reset()
	opt := Opt{
		Target: "test",
	}
	fsViewer, err := New(opt)
	assert.NoError(t, err)
	assert.NotNil(t, fsViewer)
}

func TestPullBootstrap(t *testing.T) {
	opt := Opt{
		WorkDir: "/tmp/nydusify/fsviwer",
	}
	fsViwer := FsViewer{
		Opt: opt,
		NydusdConfig: tool.NydusdConfig{
			ExternalBackendConfigPath: "/tmp/backend.json",
		},
	}
	os.MkdirAll(fsViwer.WorkDir, 0755)
	defer os.RemoveAll(fsViwer.WorkDir)
	targetParsed := &parser.Parsed{
		NydusImage: &parser.Image{},
	}
	err := fsViwer.PullBootstrap(context.Background(), targetParsed)
	assert.Error(t, err)
	callCount := 0
	getBootstrapPatches := gomonkey.ApplyPrivateMethod(&fsViwer, "getBootstrapFile", func(context.Context, *parser.Image, string, string) error {
		if callCount == 0 {
			callCount++
			return nil
		}
		return errors.New("failed to pull Nydus bootstrap layer mock error")
	})
	defer getBootstrapPatches.Reset()
	err = fsViwer.PullBootstrap(context.Background(), targetParsed)
	assert.Error(t, err)
}

func TestGetBootstrapFile(t *testing.T) {
	opt := Opt{
		WorkDir: "/tmp/nydusify/fsviwer",
	}
	fsViwer := FsViewer{
		Opt:    opt,
		Parser: &parser.Parser{},
	}
	t.Run("Run pull bootstrap failed", func(t *testing.T) {
		pullNydusBootstrapPatches := gomonkey.ApplyMethod(fsViwer.Parser, "PullNydusBootstrap", func(*parser.Parser, context.Context, *parser.Image) (io.ReadCloser, error) {
			return nil, errors.New("failed to pull Nydus bootstrap layer mock error")
		})
		defer pullNydusBootstrapPatches.Reset()
		image := &parser.Image{}
		err := fsViwer.getBootstrapFile(context.Background(), image, "", "")
		assert.Error(t, err)
	})

	t.Run("Run unpack failed", func(t *testing.T) {
		var buf bytes.Buffer
		pullNydusBootstrapPatches := gomonkey.ApplyMethod(fsViwer.Parser, "PullNydusBootstrap", func(*parser.Parser, context.Context, *parser.Image) (io.ReadCloser, error) {
			return io.NopCloser(&buf), nil
		})
		defer pullNydusBootstrapPatches.Reset()
		image := &parser.Image{}
		err := fsViwer.getBootstrapFile(context.Background(), image, "", "")
		assert.Error(t, err)
	})

	t.Run("Run normal", func(t *testing.T) {
		var buf bytes.Buffer
		pullNydusBootstrapPatches := gomonkey.ApplyMethod(fsViwer.Parser, "PullNydusBootstrap", func(*parser.Parser, context.Context, *parser.Image) (io.ReadCloser, error) {
			return io.NopCloser(&buf), nil
		})
		defer pullNydusBootstrapPatches.Reset()

		unpackPatches := gomonkey.ApplyFunc(utils.UnpackFile, func(io.Reader, string, string) error {
			return nil
		})
		defer unpackPatches.Reset()
		image := &parser.Image{}
		err := fsViwer.getBootstrapFile(context.Background(), image, "", "")
		assert.NoError(t, err)
	})
}

func TestHandleExternalBackendConfig(t *testing.T) {
	backend := &backend.Backend{
		Backends: []backend.Config{
			{
				Type: "registry",
			},
		},
	}
	bkdConfig, err := json.Marshal(backend)
	require.NoError(t, err)
	opt := Opt{
		WorkDir:       "/tmp/nydusify/fsviwer",
		BackendConfig: string(bkdConfig),
	}
	fsViwer := FsViewer{
		Opt:    opt,
		Parser: &parser.Parser{},
	}
	t.Run("Run not exist", func(t *testing.T) {
		err := fsViwer.handleExternalBackendConfig()
		assert.NoError(t, err)
	})

	t.Run("Run normal", func(t *testing.T) {
		osStatPatches := gomonkey.ApplyFunc(os.Stat, func(string) (os.FileInfo, error) {
			return nil, nil
		})
		defer osStatPatches.Reset()

		buildExternalConfigPatches := gomonkey.ApplyFunc(utils.BuildRuntimeExternalBackendConfig, func(string, string) error {
			return nil
		})
		defer buildExternalConfigPatches.Reset()
		err := fsViwer.handleExternalBackendConfig()
		assert.NoError(t, err)
	})
}

func TestPrettyDump(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("dump simple object", func(t *testing.T) {
		obj := map[string]string{"key": "value"}
		path := tmpDir + "/test1.json"
		err := prettyDump(obj, path)
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Contains(t, string(data), "key")
		assert.Contains(t, string(data), "value")

		// Verify it's valid indented JSON
		var decoded map[string]string
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, "value", decoded["key"])
	})

	t.Run("dump complex object", func(t *testing.T) {
		obj := struct {
			Name  string   `json:"name"`
			Items []string `json:"items"`
		}{
			Name:  "test",
			Items: []string{"a", "b", "c"},
		}
		path := tmpDir + "/test2.json"
		err := prettyDump(obj, path)
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Contains(t, string(data), "  ")
	})

	t.Run("dump to invalid path", func(t *testing.T) {
		obj := map[string]string{"key": "value"}
		err := prettyDump(obj, "/nonexistent/dir/file.json")
		assert.Error(t, err)
	})

	t.Run("dump nil object", func(t *testing.T) {
		path := tmpDir + "/test_nil.json"
		err := prettyDump(nil, path)
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		assert.Equal(t, "null", string(data))
	})

	t.Run("dump unmarshalable object", func(t *testing.T) {
		// Channels cannot be marshaled to JSON
		ch := make(chan int)
		err := prettyDump(ch, tmpDir+"/test_fail.json")
		assert.Error(t, err)
	})
}

func TestOptStructDefaults(t *testing.T) {
	opt := Opt{}
	assert.Empty(t, opt.WorkDir)
	assert.Empty(t, opt.Target)
	assert.False(t, opt.TargetInsecure)
	assert.Empty(t, opt.MountPath)
	assert.Empty(t, opt.NydusdPath)
	assert.Empty(t, opt.BackendType)
	assert.Empty(t, opt.BackendConfig)
	assert.Empty(t, opt.ExpectedArch)
	assert.Empty(t, opt.FsVersion)
	assert.False(t, opt.Prefetch)
}

func TestOptStructFilled(t *testing.T) {
	opt := Opt{
		WorkDir:        "/work",
		Target:         "registry.example.com/image:v1",
		TargetInsecure: true,
		MountPath:      "/mnt",
		NydusdPath:     "/usr/bin/nydusd",
		BackendType:    "registry",
		BackendConfig:  `{"key":"val"}`,
		ExpectedArch:   "amd64",
		FsVersion:      "6",
		Prefetch:       true,
	}
	assert.Equal(t, "/work", opt.WorkDir)
	assert.Equal(t, "registry.example.com/image:v1", opt.Target)
	assert.True(t, opt.TargetInsecure)
	assert.Equal(t, "/mnt", opt.MountPath)
	assert.Equal(t, "/usr/bin/nydusd", opt.NydusdPath)
	assert.Equal(t, "registry", opt.BackendType)
	assert.Equal(t, "amd64", opt.ExpectedArch)
	assert.Equal(t, "6", opt.FsVersion)
	assert.True(t, opt.Prefetch)
}

func TestFsViewerStructConstruction(t *testing.T) {
	opt := Opt{
		WorkDir: "/work",
		Target:  "test:latest",
	}
	viewer := FsViewer{
		Opt: opt,
	}
	assert.Equal(t, "/work", viewer.WorkDir)
	assert.Equal(t, "test:latest", viewer.Target)
	assert.Nil(t, viewer.Parser)
}

func TestNewFsViewerEmptyTarget(t *testing.T) {
	opt := Opt{
		Target: "",
	}
	fsViewer, err := New(opt)
	assert.Error(t, err)
	assert.Nil(t, fsViewer)
	assert.Contains(t, err.Error(), "missing target image reference")
}

func TestPullBootstrapNilNydusImage(t *testing.T) {
	tmpDir := t.TempDir()
	opt := Opt{WorkDir: tmpDir}
	viewer := FsViewer{Opt: opt}

	targetParsed := &parser.Parsed{
		NydusImage: nil,
	}
	err := viewer.PullBootstrap(context.Background(), targetParsed)
	assert.NoError(t, err)
}

func TestPullBootstrapCleanupError(t *testing.T) {
	opt := Opt{WorkDir: "/nonexistent/\x00invalid"}
	viewer := FsViewer{Opt: opt}

	targetParsed := &parser.Parsed{}
	err := viewer.PullBootstrap(context.Background(), targetParsed)
	assert.Error(t, err)
}

func TestHandleExternalBackendConfigNotExist(t *testing.T) {
	viewer := FsViewer{
		NydusdConfig: tool.NydusdConfig{
			ExternalBackendConfigPath: "/nonexistent/path/config.json",
		},
	}
	err := viewer.handleExternalBackendConfig()
	assert.NoError(t, err)
}

func TestHandleExternalBackendConfigEmptyPath(t *testing.T) {
	viewer := FsViewer{
		NydusdConfig: tool.NydusdConfig{
			ExternalBackendConfigPath: "",
		},
	}
	err := viewer.handleExternalBackendConfig()
	assert.NoError(t, err)
}

func TestNewFsViewerErrors(t *testing.T) {
	var remoteErr error
	var parserErr error
	var parserResult *parser.Parser

	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyFunc(provider.DefaultRemote, func(string, bool) (*remote.Remote, error) {
		if remoteErr != nil {
			return nil, remoteErr
		}
		return &remote.Remote{}, nil
	})
	patches.ApplyFunc(parser.New, func(r *remote.Remote, arch string) (*parser.Parser, error) {
		if parserErr != nil {
			return nil, parserErr
		}
		if parserResult != nil {
			return parserResult, nil
		}
		return &parser.Parser{Remote: r}, nil
	})

	t.Run("DefaultRemoteError", func(t *testing.T) {
		remoteErr = errors.New("remote error")
		parserErr = nil

		opt := Opt{Target: "test:latest"}
		fsViewer, err := New(opt)
		assert.Error(t, err)
		assert.Nil(t, fsViewer)
		assert.Contains(t, err.Error(), "failed to create image provider")
	})

	t.Run("ParserError", func(t *testing.T) {
		remoteErr = nil
		parserErr = errors.New("parser error")

		opt := Opt{Target: "test:latest"}
		fsViewer, err := New(opt)
		assert.Error(t, err)
		assert.Nil(t, fsViewer)
		assert.Contains(t, err.Error(), "failed to create image reference parser")
	})

	t.Run("WithOptions", func(t *testing.T) {
		remoteErr = nil
		parserErr = nil

		opt := Opt{
			WorkDir:        "/work",
			Target:         "registry.io/image:v1",
			TargetInsecure: true,
			MountPath:      "/mnt",
			NydusdPath:     "/usr/bin/nydusd",
			BackendType:    "registry",
			BackendConfig:  `{"host":"reg.io"}`,
			ExpectedArch:   "amd64",
			FsVersion:      "6",
			Prefetch:       true,
		}
		fsViewer, err := New(opt)
		assert.NoError(t, err)
		assert.NotNil(t, fsViewer)
		assert.Equal(t, "/work", fsViewer.WorkDir)
		assert.Equal(t, "registry.io/image:v1", fsViewer.Target)
		assert.True(t, fsViewer.TargetInsecure)
		assert.True(t, fsViewer.Prefetch)
	})
}

func TestPullBootstrapWithNydusImage(t *testing.T) {
	tmpDir := t.TempDir()
	opt := Opt{WorkDir: tmpDir}
	fsViewer := FsViewer{
		Opt: opt,
		NydusdConfig: tool.NydusdConfig{
			BootstrapPath: tmpDir + "/bootstrap",
		},
	}

	targetParsed := &parser.Parsed{
		NydusImage: &parser.Image{},
	}

	// Mock getBootstrapFile to succeed
	patches := gomonkey.ApplyPrivateMethod(&fsViewer, "getBootstrapFile", func(context.Context, *parser.Image, string, string) error {
		return nil
	})
	defer patches.Reset()

	err := fsViewer.PullBootstrap(context.Background(), targetParsed)
	assert.NoError(t, err)

	// Verify working directory was re-created
	_, err = os.Stat(tmpDir)
	assert.NoError(t, err)
}

func TestPullBootstrapExternalBackendNotFoundIsOk(t *testing.T) {
	tmpDir := t.TempDir()
	opt := Opt{WorkDir: tmpDir}
	fsViewer := FsViewer{
		Opt: opt,
		NydusdConfig: tool.NydusdConfig{
			BootstrapPath:            tmpDir + "/bootstrap",
			ExternalBackendConfigPath: tmpDir + "/external.json",
		},
	}

	targetParsed := &parser.Parsed{
		NydusImage: &parser.Image{},
	}

	callCount := 0
	patches := gomonkey.ApplyPrivateMethod(&fsViewer, "getBootstrapFile", func(ctx context.Context, img *parser.Image, source, target string) error {
		callCount++
		if callCount == 1 {
			return nil // bootstrap pull ok
		}
		return errors.New("Not found file backend.json")
	})
	defer patches.Reset()

	err := fsViewer.PullBootstrap(context.Background(), targetParsed)
	assert.NoError(t, err) // "Not found" error is acceptable
}

func TestPullBootstrapExternalBackendRealError(t *testing.T) {
	tmpDir := t.TempDir()
	opt := Opt{WorkDir: tmpDir}
	fsViewer := FsViewer{
		Opt: opt,
		NydusdConfig: tool.NydusdConfig{
			BootstrapPath:            tmpDir + "/bootstrap",
			ExternalBackendConfigPath: tmpDir + "/external.json",
		},
	}

	targetParsed := &parser.Parsed{
		NydusImage: &parser.Image{},
	}

	callCount := 0
	patches := gomonkey.ApplyPrivateMethod(&fsViewer, "getBootstrapFile", func(ctx context.Context, img *parser.Image, source, target string) error {
		callCount++
		if callCount == 1 {
			return nil // bootstrap pull ok
		}
		return errors.New("network timeout")
	})
	defer patches.Reset()

	err := fsViewer.PullBootstrap(context.Background(), targetParsed)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unpack Nydus external backend layer")
}

func TestPrettyDumpEmptyMap(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/empty.json"
	err := prettyDump(map[string]string{}, path)
	assert.NoError(t, err)

	data, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestPrettyDumpArray(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/array.json"
	err := prettyDump([]int{1, 2, 3}, path)
	assert.NoError(t, err)

	data, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "1")
	assert.Contains(t, string(data), "2")
	assert.Contains(t, string(data), "3")
}

func TestPrettyDumpOverwritesExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/overwrite.json"
	// Write initial
	prettyDump(map[string]string{"old": "data"}, path)
	// Overwrite
	prettyDump(map[string]string{"new": "data"}, path)

	data, _ := os.ReadFile(path)
	assert.NotContains(t, string(data), "old")
	assert.Contains(t, string(data), "new")
}

func TestHandleExternalBackendConfigError(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := tmpDir + "/backend.json"
	// Create an existing file so os.Stat doesn't return NotExist
	os.WriteFile(cfgPath, []byte("invalid"), 0644)

	fsViewer := FsViewer{
		Opt: Opt{BackendConfig: `{"host":"test"}`},
		NydusdConfig: tool.NydusdConfig{
			ExternalBackendConfigPath: cfgPath,
		},
	}

	// Patch BuildRuntimeExternalBackendConfig to return error
	patches := gomonkey.ApplyFunc(utils.BuildRuntimeExternalBackendConfig, func(string, string) error {
		return errors.New("build config error")
	})
	defer patches.Reset()

	err := fsViewer.handleExternalBackendConfig()
	assert.Error(t, err)
}

func TestMountImageCreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	fsViewer := FsViewer{
		NydusdConfig: tool.NydusdConfig{
			BlobCacheDir: tmpDir + "/cache",
			MountPath:    tmpDir + "/mnt",
			NydusdPath:   "/nonexistent/nydusd",
			ConfigPath:   tmpDir + "/config.json",
		},
	}

	// NewNydusd will fail because the binary doesn't exist,
	// but directories should be created before that
	patches := gomonkey.ApplyFunc(tool.NewNydusd, func(conf tool.NydusdConfig) (*tool.Nydusd, error) {
		return nil, errors.New("mock error")
	})
	defer patches.Reset()

	err := fsViewer.MountImage()
	assert.Error(t, err)

	// Verify directories were created
	_, err = os.Stat(tmpDir + "/cache")
	assert.NoError(t, err)
	_, err = os.Stat(tmpDir + "/mnt")
	assert.NoError(t, err)
}
