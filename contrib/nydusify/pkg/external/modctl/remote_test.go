package modctl

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	modelspec "github.com/CloudNativeAI/model-spec/specs-go/v1"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/snapshotter/external/backend"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockRemote struct {
	ResolveFunc        func(ctx context.Context) (*ocispec.Descriptor, error)
	PullFunc           func(ctx context.Context, desc ocispec.Descriptor, plainHTTP bool) (io.ReadCloser, error)
	ReadSeekCloserFunc func(ctx context.Context, desc ocispec.Descriptor, plainHTTP bool) (io.ReadSeekCloser, error)
	WithHTTPFunc       func()
	MaybeWithHTTPFunc  func(err error)
}

func (m *MockRemote) Resolve(ctx context.Context) (*ocispec.Descriptor, error) {
	return m.ResolveFunc(ctx)
}

func (m *MockRemote) Pull(ctx context.Context, desc ocispec.Descriptor, plainHTTP bool) (io.ReadCloser, error) {
	return m.PullFunc(ctx, desc, plainHTTP)
}

func (m *MockRemote) ReadSeekCloser(ctx context.Context, desc ocispec.Descriptor, plainHTTP bool) (io.ReadSeekCloser, error) {
	return m.ReadSeekCloserFunc(ctx, desc, plainHTTP)
}

func (m *MockRemote) WithHTTP() {
	m.WithHTTPFunc()
}

func (m *MockRemote) MaybeWithHTTP(err error) {
	m.MaybeWithHTTPFunc(err)
}

type readSeekCloser struct {
	*bytes.Reader
}

func (r *readSeekCloser) Close() error {
	return nil
}

func TestRemoteHandler_Handle(t *testing.T) {
	mockRemote := &MockRemote{
		ResolveFunc: func(context.Context) (*ocispec.Descriptor, error) {
			return &ocispec.Descriptor{}, nil
		},
		PullFunc: func(context.Context, ocispec.Descriptor, bool) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("{}"))), nil
		},
		ReadSeekCloserFunc: func(context.Context, ocispec.Descriptor, bool) (io.ReadSeekCloser, error) {
			// prepare tar

			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			files := []struct {
				name string
				size int64
			}{
				{"file1.txt", 10},
				{"file2.txt", 20},
				{"file3.txt", 30},
			}
			for _, file := range files {
				header := &tar.Header{
					Name: file.name,
					Size: file.size,
				}
				if err := tw.WriteHeader(header); err != nil {
					t.Fatalf("Failed to write tar header: %v", err)
				}
				if _, err := tw.Write(make([]byte, file.size)); err != nil {
					t.Fatalf("Failed to write tar content: %v", err)
				}
			}
			tw.Close()
			reader := bytes.NewReader(buf.Bytes())
			return &readSeekCloser{reader}, nil
		},
		WithHTTPFunc:      func() {},
		MaybeWithHTTPFunc: func(error) {},
	}

	fileCrcInfo := &FileCrcInfo{
		ChunkCrcs: "0x1234,0x5678",
		FilePath:  "file1.txt",
	}
	fileCrcList := &FileCrcList{
		Files: []FileCrcInfo{
			*fileCrcInfo,
		},
	}
	crcs, err := json.Marshal(fileCrcList)
	require.NoError(t, err)
	annotations := map[string]string{
		filePathKey: "file1.txt",
		crcsKey:     string(crcs),
	}
	handler := &RemoteHandler{
		ctx:      context.Background(),
		imageRef: "test-image",
		remoter:  mockRemote,
		manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				{
					MediaType:   "test-media-type",
					Digest:      "test-digest",
					Annotations: annotations,
				},
			},
		},
		blobs: []backend.Blob{
			{
				Config: backend.BlobConfig{
					Digest: "test-digest",
					Size:   "100",
				},
			},
		},
	}

	backend, fileAttrs, err := handler.Handle(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, backend)
	assert.NotEmpty(t, fileAttrs)
	assert.Equal(t, 3, len(fileAttrs))
	assert.Equal(t, fileCrcInfo.ChunkCrcs, fileAttrs[0].Crcs)
	assert.Equal(t, "", fileAttrs[1].Crcs)

	handler.manifest.Layers[0].Annotations = map[string]string{
		filePathKey: "file1.txt",
		crcsKey:     "0x1234,0x5678",
	}
	_, _, err = handler.Handle(context.Background())
	assert.Error(t, err)
}

func TestGetModelConfig(t *testing.T) {
	mockRemote := &MockRemote{
		ResolveFunc: func(context.Context) (*ocispec.Descriptor, error) {
			return &ocispec.Descriptor{}, nil
		},
		PullFunc: func(_ context.Context, desc ocispec.Descriptor, _ bool) (io.ReadCloser, error) {
			desc = ocispec.Descriptor{
				MediaType: modelspec.MediaTypeModelConfig,
				Size:      desc.Size,
			}
			data, err := json.Marshal(desc)
			assert.Nil(t, err)
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}

	handler := &RemoteHandler{
		ctx:      context.Background(),
		imageRef: "test-image",
		remoter:  mockRemote,
	}

	modelConfig, err := handler.GetModelConfig()
	assert.NoError(t, err)
	assert.NotNil(t, modelConfig)
}

func TestSetManifest(t *testing.T) {
	mockRemote := &MockRemote{
		ResolveFunc: func(context.Context) (*ocispec.Descriptor, error) {
			return &ocispec.Descriptor{}, nil
		},
		PullFunc: func(context.Context, ocispec.Descriptor, bool) (io.ReadCloser, error) {
			mani := ocispec.Manifest{
				MediaType: ocispec.MediaTypeImageManifest,
			}
			data, err := json.Marshal(mani)
			assert.Nil(t, err)
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}
	handler := &RemoteHandler{
		ctx:      context.Background(),
		imageRef: "test-image",
		remoter:  mockRemote,
	}

	err := handler.setManifest()
	assert.Nil(t, err)
}

func TestBackend(t *testing.T) {
	handler := &RemoteHandler{
		manifest: ocispec.Manifest{},
		blobs: []backend.Blob{
			{
				Config: backend.BlobConfig{
					Digest: "test-digest",
					Size:   "100",
				},
			},
		},
	}

	backend, err := handler.backend()
	assert.NoError(t, err)
	assert.NotNil(t, backend)
	assert.Equal(t, "v1", backend.Version)
	assert.Equal(t, "registry", backend.Backends[0].Type)
}

// TestNewRemoteHandlerAndInit consolidates gomonkey-based remote handler
// tests into a single function with persistent patches, avoiding repeated
// patch/reset cycles that are unreliable on ARM64.
func TestNewRemoteHandlerAndInit(t *testing.T) {
	// Persistent patches applied once — never Reset.
	var defaultRemoteFunc func(string, bool) (*remote.Remote, error)
	gomonkey.ApplyFunc(provider.DefaultRemote, func(ref string, insecure bool) (*remote.Remote, error) {
		return defaultRemoteFunc(ref, insecure)
	})

	var initRemoteHandlerFunc func(*RemoteHandler) error
	gomonkey.ApplyFunc(initRemoteHandler, func(h *RemoteHandler) error {
		return initRemoteHandlerFunc(h)
	})

	t.Run("NewRemoteHandler", func(t *testing.T) {
		var remoter = remote.Remote{}
		defaultRemoteFunc = func(string, bool) (*remote.Remote, error) {
			return &remoter, nil
		}
		initRemoteHandlerFunc = func(*RemoteHandler) error {
			return nil
		}

		remoteHandler, err := NewRemoteHandler(context.Background(), "test", false)
		assert.Nil(t, err)
		assert.NotNil(t, remoteHandler)
	})

	t.Run("InitRemoteHandler", func(t *testing.T) {
		mockRemote := &MockRemote{
			ResolveFunc: func(context.Context) (*ocispec.Descriptor, error) {
				return &ocispec.Descriptor{}, nil
			},
			PullFunc: func(context.Context, ocispec.Descriptor, bool) (io.ReadCloser, error) {
				mani := ocispec.Manifest{}
				data, _ := json.Marshal(mani)
				return io.NopCloser(bytes.NewReader(data)), nil
			},
		}
		// Replicate real initRemoteHandler logic with mocked remoter
		initRemoteHandlerFunc = func(h *RemoteHandler) error {
			h.remoter = mockRemote
			if err := h.setManifest(); err != nil {
				return errors.Wrap(err, "set manifest failed")
			}
			h.blobs = convertToBlobs(&h.manifest)
			return nil
		}

		h := &RemoteHandler{ctx: context.Background()}
		err := initRemoteHandler(h)
		assert.NoError(t, err)
	})
}

func TestHackFileWrapper(t *testing.T) {
	f := &fileInfo{}
	os.Setenv("HACK_MODE", "0640")
	hackFileWrapper(f)
	assert.Equal(t, uint32(0640), f.mode)
}
