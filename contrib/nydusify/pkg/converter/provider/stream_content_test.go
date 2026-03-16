package provider

import (
	"bytes"
	"context"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	ctrcontent "github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/errdefs"
)

func TestIsFetchRef(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{"manifest-sha256:abc", true},
		{"index-sha256:abc", true},
		{"layer-sha256:abc", true},
		{"config-sha256:abc", true},
		{"attestation-sha256:abc", true},
		{"manifest-", true},
		{"custom-ref", false},
		{"docker.io/library/alpine:latest", false},
		{"", false},
		{"manifesto-something", false},
		{"MANIFEST-sha256:abc", false},
		{"layer", false},
	}
	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := isFetchRef(tt.ref)
			if got != tt.want {
				t.Errorf("isFetchRef(%q) = %v, want %v", tt.ref, got, tt.want)
			}
		})
	}
}

func TestHasPrefix(t *testing.T) {
	tests := []struct {
		s, p string
		want bool
	}{
		{"hello world", "hello", true},
		{"hello", "hello", true},
		{"hell", "hello", false},
		{"", "hello", false},
		{"hello", "", true},
		{"", "", true},
		{"abc", "abcdef", false},
	}
	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.p, func(t *testing.T) {
			got := hasPrefix(tt.s, tt.p)
			if got != tt.want {
				t.Errorf("hasPrefix(%q, %q) = %v, want %v", tt.s, tt.p, got, tt.want)
			}
		})
	}
}

func TestCopyMap(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]string
	}{
		{"nil map", nil},
		{"empty map", map[string]string{}},
		{"single entry", map[string]string{"key": "value"}},
		{"multiple entries", map[string]string{"a": "1", "b": "2", "c": "3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := copyMap(tt.input)
			if tt.input == nil {
				if got != nil {
					t.Error("copyMap(nil) should return nil")
				}
				return
			}
			if len(got) != len(tt.input) {
				t.Errorf("len = %d, want %d", len(got), len(tt.input))
			}
			for k, v := range tt.input {
				if got[k] != v {
					t.Errorf("got[%q] = %q, want %q", k, got[k], v)
				}
			}
			// Verify it's a real copy by modifying the copy
			if len(got) > 0 {
				for k := range got {
					got[k] = "modified"
					break
				}
				for k, v := range tt.input {
					if v == "modified" {
						t.Errorf("original map was modified at key %q", k)
					}
				}
			}
		})
	}
}

func TestMemWriter(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Size:      5,
	}

	w := newMemWriter(sc, desc)

	t.Run("write and digest", func(t *testing.T) {
		n, err := w.Write([]byte("hello"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if n != 5 {
			t.Errorf("Write() = %d, want 5", n)
		}

		dgst := w.Digest()
		expected := digest.SHA256.FromBytes([]byte("hello"))
		if dgst != expected {
			t.Errorf("Digest() = %v, want %v", dgst, expected)
		}
	})

	t.Run("status", func(t *testing.T) {
		status, err := w.Status()
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		if status.Offset != 5 {
			t.Errorf("Status().Offset = %d, want 5", status.Offset)
		}
		if status.Total != 5 {
			t.Errorf("Status().Total = %d, want 5", status.Total)
		}
	})

	t.Run("close is no-op", func(t *testing.T) {
		if err := w.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
}

func TestMemWriterTruncate(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	w := newMemWriter(sc, ocispec.Descriptor{})

	w.Write([]byte("hello world"))

	t.Run("truncate to shorter", func(t *testing.T) {
		err := w.Truncate(5)
		if err != nil {
			t.Fatalf("Truncate(5) error = %v", err)
		}
		status, _ := w.Status()
		if status.Offset != 5 {
			t.Errorf("after truncate: Offset = %d, want 5", status.Offset)
		}
	})

	t.Run("truncate to same size", func(t *testing.T) {
		err := w.Truncate(5)
		if err != nil {
			t.Errorf("Truncate(same size) error = %v", err)
		}
	})

	t.Run("truncate to zero", func(t *testing.T) {
		err := w.Truncate(0)
		if err != nil {
			t.Fatalf("Truncate(0) error = %v", err)
		}
		status, _ := w.Status()
		if status.Offset != 0 {
			t.Errorf("after Truncate(0): Offset = %d, want 0", status.Offset)
		}
	})

	t.Run("truncate negative", func(t *testing.T) {
		err := w.Truncate(-1)
		if err == nil {
			t.Error("Truncate(-1) should return error")
		}
	})
}

func TestMemWriterCommit(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	w := newMemWriter(sc, ocispec.Descriptor{})

	data := []byte("test data")
	w.Write(data)
	dgst := digest.SHA256.FromBytes(data)

	err := w.Commit(context.Background(), int64(len(data)), dgst)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Verify data was stored in StreamContent
	sc.mu.RLock()
	stored, ok := sc.blobs[dgst]
	sc.mu.RUnlock()
	if !ok {
		t.Fatal("blob not found in StreamContent after Commit")
	}
	if !bytes.Equal(stored, data) {
		t.Errorf("stored data = %q, want %q", stored, data)
	}
}

func TestMemWriterDigestEmpty(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	w := newMemWriter(sc, ocispec.Descriptor{})

	// Digest with no writes should use FromBytes on empty buffer
	dgst := w.Digest()
	expected := digest.FromBytes([]byte{})
	if dgst != expected {
		t.Errorf("empty Digest() = %v, want %v", dgst, expected)
	}
}

func TestBytesReaderAt(t *testing.T) {
	data := []byte("hello world")
	br := &bytesReaderAt{r: bytes.NewReader(data)}

	t.Run("ReadAt beginning", func(t *testing.T) {
		buf := make([]byte, 5)
		n, err := br.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt() error = %v", err)
		}
		if n != 5 || string(buf) != "hello" {
			t.Errorf("ReadAt(0) = %q, want %q", string(buf[:n]), "hello")
		}
	})

	t.Run("ReadAt offset", func(t *testing.T) {
		buf := make([]byte, 5)
		n, err := br.ReadAt(buf, 6)
		if err != nil {
			t.Fatalf("ReadAt() error = %v", err)
		}
		if n != 5 || string(buf) != "world" {
			t.Errorf("ReadAt(6) = %q, want %q", string(buf[:n]), "world")
		}
	})

	t.Run("Size", func(t *testing.T) {
		if br.Size() != 11 {
			t.Errorf("Size() = %d, want 11", br.Size())
		}
	})

	t.Run("Close", func(t *testing.T) {
		if err := br.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
}

func TestStreamContentInfoAndUpdate(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	ctx := context.Background()
	dgst := digest.SHA256.FromBytes([]byte("test"))

	// Initial Info should return empty labels
	info, err := sc.Info(ctx, dgst)
	if err != nil {
		t.Fatalf("Info() error = %v", err)
	}
	if info.Labels != nil {
		t.Errorf("initial labels should be nil, got %v", info.Labels)
	}

	// Update labels
	_, err = sc.Update(ctx, ctrcontent.Info{
		Digest: dgst,
		Labels: map[string]string{"key1": "val1"},
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	// Read back
	info, err = sc.Info(ctx, dgst)
	if err != nil {
		t.Fatalf("Info() after update error = %v", err)
	}
	if info.Labels["key1"] != "val1" {
		t.Errorf("Labels[key1] = %q, want %q", info.Labels["key1"], "val1")
	}

	// Update again with additional label
	_, err = sc.Update(ctx, ctrcontent.Info{
		Digest: dgst,
		Labels: map[string]string{"key2": "val2"},
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	info, _ = sc.Info(ctx, dgst)
	if info.Labels["key1"] != "val1" || info.Labels["key2"] != "val2" {
		t.Errorf("Labels = %v, want key1=val1, key2=val2", info.Labels)
	}
}

func TestStreamContentDelete(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	ctx := context.Background()
	dgst := digest.SHA256.FromBytes([]byte("test"))

	// Set up some data
	sc.mu.Lock()
	sc.labels[dgst] = map[string]string{"a": "b"}
	sc.blobs[dgst] = []byte("data")
	sc.mu.Unlock()

	err := sc.Delete(ctx, dgst)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	sc.mu.RLock()
	_, hasLabels := sc.labels[dgst]
	_, hasBlobs := sc.blobs[dgst]
	sc.mu.RUnlock()

	if hasLabels {
		t.Error("labels not deleted")
	}
	if hasBlobs {
		t.Error("blobs not deleted")
	}
}

func TestStreamContentWalk(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	err := sc.Walk(context.Background(), nil)
	if err != nil {
		t.Errorf("Walk() error = %v", err)
	}
}

func TestStreamContentStatus(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	_, err := sc.Status(context.Background(), "ref")
	if !errdefs.IsNotFound(err) {
		t.Errorf("Status() error = %v, want NotFound", err)
	}
}

func TestStreamContentListStatuses(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	statuses, err := sc.ListStatuses(context.Background())
	if err != nil {
		t.Errorf("ListStatuses() error = %v", err)
	}
	if statuses != nil {
		t.Errorf("ListStatuses() = %v, want nil", statuses)
	}
}

func TestStreamContentAbort(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	err := sc.Abort(context.Background(), "ref")
	if err != nil {
		t.Errorf("Abort() error = %v", err)
	}
}

func TestStreamContentWriterFetchRef(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	_, err := sc.Writer(context.Background(), ctrcontent.WithRef("layer-sha256:abc"))
	if !errdefs.IsAlreadyExists(err) {
		t.Errorf("Writer() for fetch ref: error = %v, want AlreadyExists", err)
	}
}

func TestStreamContentWriterNonFetchRef(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	w, err := sc.Writer(context.Background(), ctrcontent.WithRef("custom-ref"))
	if err != nil {
		t.Fatalf("Writer() for non-fetch ref: error = %v", err)
	}
	if w == nil {
		t.Error("Writer() returned nil for non-fetch ref")
	}
}

func TestStreamContentReaderAtFromBlob(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	data := []byte("test content")
	dgst := digest.SHA256.FromBytes(data)

	sc.mu.Lock()
	sc.blobs[dgst] = data
	sc.mu.Unlock()

	ra, err := sc.ReaderAt(context.Background(), ocispec.Descriptor{Digest: dgst})
	if err != nil {
		t.Fatalf("ReaderAt() error = %v", err)
	}
	defer ra.Close()

	buf := make([]byte, len(data))
	n, err := ra.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Errorf("read data = %q, want %q", buf[:n], data)
	}
}

func TestStreamContentSetDefaultRef(t *testing.T) {
	sc := NewStreamContent(nil, nil)
	sc.SetDefaultRef("docker.io/library/alpine:latest")

	sc.mu.RLock()
	ref := sc.defaultRef
	sc.mu.RUnlock()

	if ref != "docker.io/library/alpine:latest" {
		t.Errorf("defaultRef = %q, want %q", ref, "docker.io/library/alpine:latest")
	}
}
