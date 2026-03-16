package archive

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestNewChangeWriter(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChangeWriter(&buf, "/source")
	if cw == nil {
		t.Fatal("NewChangeWriter() returned nil")
	}
	if cw.source != "/source" {
		t.Errorf("source = %q, want %q", cw.source, "/source")
	}
	if cw.tw == nil {
		t.Error("tar.Writer should not be nil")
	}
	if cw.whiteoutT.IsZero() {
		t.Error("whiteoutT should not be zero")
	}
	if cw.inodeSrc == nil {
		t.Error("inodeSrc should not be nil")
	}
	if cw.inodeRefs == nil {
		t.Error("inodeRefs should not be nil")
	}
	if cw.addedDirs == nil {
		t.Error("addedDirs should not be nil")
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestCopyBuffered(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantN   int64
		wantErr bool
	}{
		{
			name:  "empty input",
			input: "",
			wantN: 0,
		},
		{
			name:  "small input",
			input: "hello world",
			wantN: 11,
		},
		{
			name:  "larger than buffer input",
			input: strings.Repeat("x", 64*1024),
			wantN: 64 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := strings.NewReader(tt.input)
			var dst bytes.Buffer
			n, err := copyBuffered(context.Background(), &dst, src)
			if (err != nil) != tt.wantErr {
				t.Errorf("copyBuffered() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if n != tt.wantN {
				t.Errorf("copyBuffered() = %d, want %d", n, tt.wantN)
			}
			if dst.String() != tt.input {
				t.Errorf("output mismatch: got len %d, want len %d", dst.Len(), len(tt.input))
			}
		})
	}
}

func TestCopyBufferedCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	src := strings.NewReader("data that should not be fully copied")
	var dst bytes.Buffer
	_, err := copyBuffered(ctx, &dst, src)
	if err == nil {
		t.Error("copyBuffered() with cancelled context should return error")
	}
}

func TestCopyBufferedWriteError(t *testing.T) {
	src := strings.NewReader("some data")
	dst := &errorWriter{err: io.ErrShortWrite}
	_, err := copyBuffered(context.Background(), dst, src)
	if err == nil {
		t.Error("copyBuffered() with write error should return error")
	}
}

type errorWriter struct{ err error }

func (e *errorWriter) Write(_ []byte) (int, error) { return 0, e.err }
