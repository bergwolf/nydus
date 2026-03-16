package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/remotes"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockNamed struct {
	mockName   string
	mockString string
}

func (m *MockNamed) Name() string {
	return m.mockName
}
func (m *MockNamed) String() string {
	return m.mockString
}

// MockResolver implements the Resolver interface for testing purposes.
type MockResolver struct {
	ResolveFunc         func(ctx context.Context, ref string) (string, ocispec.Descriptor, error)
	FetcherFunc         func(ctx context.Context, ref string) (remotes.Fetcher, error)
	PusherFunc          func(ctx context.Context, ref string) (remotes.Pusher, error)
	PusherInChunkedFunc func(ctx context.Context, ref string) (remotes.PusherInChunked, error)
}

// Resolve implements the Resolver.Resolve method.
func (m *MockResolver) Resolve(ctx context.Context, ref string) (string, ocispec.Descriptor, error) {
	if m.ResolveFunc != nil {
		return m.ResolveFunc(ctx, ref)
	}
	return "", ocispec.Descriptor{}, errors.New("ResolveFunc not implemented")
}

// Fetcher implements the Resolver.Fetcher method.
func (m *MockResolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	if m.FetcherFunc != nil {
		return m.FetcherFunc(ctx, ref)
	}
	return nil, errors.New("FetcherFunc not implemented")
}

// Pusher implements the Resolver.Pusher method.
func (m *MockResolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	if m.PusherFunc != nil {
		return m.PusherFunc(ctx, ref)
	}
	return nil, errors.New("PusherFunc not implemented")
}

// PusherInChunked implements the Resolver.PusherInChunked method.
func (m *MockResolver) PusherInChunked(ctx context.Context, ref string) (remotes.PusherInChunked, error) {
	if m.PusherInChunkedFunc != nil {
		return m.PusherInChunkedFunc(ctx, ref)
	}
	return nil, errors.New("PusherInChunkedFunc not implemented")
}

type mockReadSeekCloeser struct {
	buf bytes.Buffer
}

func (m *mockReadSeekCloeser) Read(p []byte) (n int, err error) {
	return m.buf.Read(p)
}

func (m *mockReadSeekCloeser) Seek(int64, int) (int64, error) {
	return 0, nil
}

func (m *mockReadSeekCloeser) Close() error {
	return nil
}

func TestReadSeekCloser(t *testing.T) {
	remote := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox:latest",
			mockString: "docker.io/library/busybox:latest",
		},
	}
	t.Run("Run not ReadSeekCloser", func(t *testing.T) {
		remote.resolverFunc = func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					var buf bytes.Buffer
					return remotes.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
						// return io.ReadSeekCloser
						return &readerAt{
							Reader: &buf,
							Closer: io.NopCloser(&buf),
						}, nil
					}), nil
				},
			}
		}
		_, err := remote.ReadSeekCloser(context.Background(), ocispec.Descriptor{}, false)
		assert.Error(t, err)
	})

	t.Run("Run Normal", func(t *testing.T) {
		// mock io.ReadSeekCloser
		remote.resolverFunc = func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					var buf bytes.Buffer
					return remotes.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
						return &mockReadSeekCloeser{
							buf: buf,
						}, nil
					}), nil
				},
			}
		}
		rsc, err := remote.ReadSeekCloser(context.Background(), ocispec.Descriptor{}, false)
		assert.NoError(t, err)
		assert.NotNil(t, rsc)
	})
}

// Tests for New
func TestNewValidRef(t *testing.T) {
	tests := []struct {
		name string
		ref  string
	}{
		{"docker hub short", "docker.io/library/alpine:latest"},
		{"docker hub full", "docker.io/library/nginx:1.25"},
		{"custom registry", "registry.example.com/myapp:v1.0"},
		{"with port", "localhost:5000/myimage:latest"},
		{"no tag", "docker.io/library/alpine"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := New(tt.ref, nil)
			require.NoError(t, err)
			require.NotNil(t, r)
			require.Equal(t, tt.ref, r.Ref)
		})
	}
}

func TestNewInvalidRef(t *testing.T) {
	tests := []struct {
		name string
		ref  string
	}{
		{"empty", ""},
		{"just colon", ":"},
		{"invalid chars", "INVALID@@@"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := New(tt.ref, nil)
			require.Error(t, err)
			require.Nil(t, r)
		})
	}
}

// Tests for MaybeWithHTTP
func TestMaybeWithHTTP(t *testing.T) {
	r, err := New("localhost:5000/myimage:latest", nil)
	require.NoError(t, err)
	require.False(t, r.IsWithHTTP())

	// Error containing the registry host should trigger HTTP
	r.MaybeWithHTTP(fmt.Errorf("failed to connect to /localhost:5000/ via HTTPS"))
	require.True(t, r.IsWithHTTP())
}

func TestMaybeWithHTTPNoMatch(t *testing.T) {
	r, err := New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)
	require.False(t, r.IsWithHTTP())

	// Error not containing the host should not trigger HTTP
	r.MaybeWithHTTP(fmt.Errorf("some random error"))
	require.False(t, r.IsWithHTTP())
}

func TestWithHTTP(t *testing.T) {
	r, err := New("docker.io/library/alpine:latest", nil)
	require.NoError(t, err)
	require.False(t, r.IsWithHTTP())

	r.WithHTTP()
	require.True(t, r.IsWithHTTP())
}

// Tests for readerAt
func TestReaderAtReadAtSequential(t *testing.T) {
	data := []byte("hello world test data")
	ra := &readerAt{
		Reader: bytes.NewReader(data),
		Closer: io.NopCloser(bytes.NewReader(data)),
		size:   int64(len(data)),
		offset: 0,
	}

	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))
}

func TestReaderAtReadAtWithSeek(t *testing.T) {
	data := []byte("hello world test data")
	reader := bytes.NewReader(data)
	ra := &readerAt{
		Reader: reader,
		Closer: io.NopCloser(reader),
		size:   int64(len(data)),
		offset: 0,
	}

	// Read from offset 6 (seeks to "world")
	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 6)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "world", string(buf))
}

func TestReaderAtReadAtNoSeekerFails(t *testing.T) {
	data := []byte("hello world")
	// Use a pipe reader which doesn't support seeking
	ra := &readerAt{
		Reader: strings.NewReader(string(data)),
		Closer: io.NopCloser(strings.NewReader(string(data))),
		size:   int64(len(data)),
		offset: 0,
	}

	// First read at 0 should work
	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	// Second read at non-sequential offset with non-seekable reader
	// strings.Reader supports Seek, so use a custom non-seekable reader
	ra2 := &readerAt{
		Reader: &nonSeekableReader{data: data, pos: 0},
		Closer: io.NopCloser(&nonSeekableReader{data: data, pos: 0}),
		size:   int64(len(data)),
		offset: 5, // offset mismatch
	}

	_, err = ra2.ReadAt(buf, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reader does not support seeking")
}

type nonSeekableReader struct {
	data []byte
	pos  int
}

func (r *nonSeekableReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestReaderAtSize(t *testing.T) {
	ra := &readerAt{
		size: 12345,
	}
	require.Equal(t, int64(12345), ra.Size())
}

func TestReaderAtSizeZero(t *testing.T) {
	ra := &readerAt{
		size: 0,
	}
	require.Equal(t, int64(0), ra.Size())
}

// Tests for FromFetcher
func TestFromFetcher(t *testing.T) {
	data := []byte("test content")
	fetcher := remotes.FetcherFunc(func(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})

	provider := FromFetcher(fetcher)
	require.NotNil(t, provider)

	ra, err := provider.ReaderAt(context.Background(), ocispec.Descriptor{Size: int64(len(data))})
	require.NoError(t, err)
	require.NotNil(t, ra)
	require.Equal(t, int64(len(data)), ra.Size())

	buf := make([]byte, len(data))
	n, err := ra.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)
}

func TestFromFetcherError(t *testing.T) {
	fetcher := remotes.FetcherFunc(func(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
		return nil, fmt.Errorf("fetch failed")
	})

	provider := FromFetcher(fetcher)
	_, err := provider.ReaderAt(context.Background(), ocispec.Descriptor{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch failed")
}

// Tests for Remote.Pull
func TestRemotePullFetcherError(t *testing.T) {
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					return nil, fmt.Errorf("fetcher creation failed")
				},
			}
		},
	}

	_, err := r.Pull(context.Background(), ocispec.Descriptor{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetcher creation failed")
}

func TestRemotePullSuccess(t *testing.T) {
	data := []byte("layer data")
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					return remotes.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
						return io.NopCloser(bytes.NewReader(data)), nil
					}), nil
				},
			}
		},
	}

	reader, err := r.Pull(context.Background(), ocispec.Descriptor{}, true)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, data, result)
}

// Tests for Remote.Resolve
func TestRemoteResolveSuccess(t *testing.T) {
	expectedDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Size:      1234,
	}

	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				ResolveFunc: func(context.Context, string) (string, ocispec.Descriptor, error) {
					return "docker.io/library/busybox:latest", expectedDesc, nil
				},
			}
		},
	}

	desc, err := r.Resolve(context.Background())
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Equal(t, ocispec.MediaTypeImageManifest, desc.MediaType)
	require.Equal(t, int64(1234), desc.Size)
}

func TestRemoteResolveError(t *testing.T) {
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				ResolveFunc: func(context.Context, string) (string, ocispec.Descriptor, error) {
					return "", ocispec.Descriptor{}, fmt.Errorf("resolve failed")
				},
			}
		},
	}

	_, err := r.Resolve(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve failed")
}

// Tests for Remote.ReaderAt
func TestRemoteReaderAtSuccess(t *testing.T) {
	data := []byte("blob content here")
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					return remotes.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
						return io.NopCloser(bytes.NewReader(data)), nil
					}), nil
				},
			}
		},
	}

	ra, err := r.ReaderAt(context.Background(), ocispec.Descriptor{Size: int64(len(data))}, true)
	require.NoError(t, err)
	require.NotNil(t, ra)
	require.Equal(t, int64(len(data)), ra.Size())
}

func TestRemoteReaderAtFetcherError(t *testing.T) {
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					return nil, fmt.Errorf("fetcher error")
				},
			}
		},
	}

	_, err := r.ReaderAt(context.Background(), ocispec.Descriptor{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetcher error")
}

func TestReadSeekCloserByDigest(t *testing.T) {
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(_ context.Context, ref string) (remotes.Fetcher, error) {
					// When byDigest=true, ref should be just the name (no tag)
					require.Equal(t, "docker.io/library/busybox", ref)
					return remotes.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
						return &mockReadSeekCloeser{}, nil
					}), nil
				},
			}
		},
	}

	rsc, err := r.ReadSeekCloser(context.Background(), ocispec.Descriptor{}, true)
	require.NoError(t, err)
	require.NotNil(t, rsc)
}

func TestReadSeekCloserFetcherError(t *testing.T) {
	r := &Remote{
		parsed: &MockNamed{
			mockName:   "docker.io/library/busybox",
			mockString: "docker.io/library/busybox:latest",
		},
		resolverFunc: func(bool) remotes.Resolver {
			return &MockResolver{
				FetcherFunc: func(context.Context, string) (remotes.Fetcher, error) {
					return nil, fmt.Errorf("cannot create fetcher")
				},
			}
		},
	}

	_, err := r.ReadSeekCloser(context.Background(), ocispec.Descriptor{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot create fetcher")
}

func TestReaderAtReadAtEOF(t *testing.T) {
	data := []byte("short")
	ra := &readerAt{
		Reader: bytes.NewReader(data),
		Closer: io.NopCloser(bytes.NewReader(data)),
		size:   int64(len(data)),
		offset: 0,
	}

	// Read exactly the size of the data
	buf := make([]byte, len(data))
	n, err := ra.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	// Read beyond EOF
	buf2 := make([]byte, 10)
	_, err = ra.ReadAt(buf2, 0)
	require.Error(t, err) // EOF since reader is exhausted
}
