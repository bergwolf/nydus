package committer

import (
	"errors"
	"sync"
	"testing"
)

func TestValidateRef(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		want    string
		wantErr bool
	}{
		{
			name: "simple image with tag",
			ref:  "docker.io/library/alpine:3.18",
			want: "docker.io/library/alpine:3.18",
		},
		{
			name: "image without tag defaults to latest",
			ref:  "docker.io/library/alpine",
			want: "docker.io/library/alpine:latest",
		},
		{
			name: "short name gets normalized",
			ref:  "alpine",
			want: "docker.io/library/alpine:latest",
		},
		{
			name: "short name with tag",
			ref:  "alpine:3.18",
			want: "docker.io/library/alpine:3.18",
		},
		{
			name: "custom registry",
			ref:  "myregistry.io/myimage:v1",
			want: "myregistry.io/myimage:v1",
		},
		{
			name:    "digest reference is unsupported",
			ref:     "alpine@sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			wantErr: true,
		},
		{
			name:    "invalid reference",
			ref:     "INVALID:://image",
			wantErr: true,
		},
		{
			name:    "empty reference",
			ref:     "",
			wantErr: true,
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

func TestWithRetry(t *testing.T) {
	tests := []struct {
		name       string
		failTimes  int
		totalTries int
		wantErr    bool
	}{
		{
			name:       "succeeds on first try",
			failTimes:  0,
			totalTries: 3,
			wantErr:    false,
		},
		{
			name:       "succeeds after one retry",
			failTimes:  1,
			totalTries: 3,
			wantErr:    false,
		},
		{
			name:       "succeeds on last try",
			failTimes:  2,
			totalTries: 3,
			wantErr:    false,
		},
		{
			name:       "fails when all retries exhausted",
			failTimes:  3,
			totalTries: 3,
			wantErr:    true,
		},
		{
			name:       "single try succeeds",
			failTimes:  0,
			totalTries: 1,
			wantErr:    false,
		},
		{
			name:       "single try fails",
			failTimes:  1,
			totalTries: 1,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			err := withRetry(func() error {
				callCount++
				if callCount <= tt.failTimes {
					return errors.New("temporary error")
				}
				return nil
			}, tt.totalTries)
			if (err != nil) != tt.wantErr {
				t.Errorf("withRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMountList(t *testing.T) {
	t.Run("new mount list is empty", func(t *testing.T) {
		ml := NewMountList()
		if len(ml.paths) != 0 {
			t.Errorf("new MountList has %d paths, want 0", len(ml.paths))
		}
	})

	t.Run("add paths", func(t *testing.T) {
		ml := NewMountList()
		ml.Add("/path/a")
		ml.Add("/path/b")
		ml.Add("/path/c")
		if len(ml.paths) != 3 {
			t.Fatalf("MountList has %d paths, want 3", len(ml.paths))
		}
		if ml.paths[0] != "/path/a" || ml.paths[1] != "/path/b" || ml.paths[2] != "/path/c" {
			t.Errorf("paths = %v, want [/path/a /path/b /path/c]", ml.paths)
		}
	})

	t.Run("concurrent adds", func(t *testing.T) {
		ml := NewMountList()
		var wg sync.WaitGroup
		n := 50
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ml.Add("/some/path")
			}()
		}
		wg.Wait()
		if len(ml.paths) != n {
			t.Errorf("after %d concurrent adds: len(paths) = %d", n, len(ml.paths))
		}
	})
}

func TestBlobStruct(t *testing.T) {
	b := Blob{
		Name:          "test-blob",
		BootstrapName: "test-bootstrap",
	}
	if b.Name != "test-blob" {
		t.Errorf("Blob.Name = %q, want %q", b.Name, "test-blob")
	}
	if b.BootstrapName != "test-bootstrap" {
		t.Errorf("Blob.BootstrapName = %q, want %q", b.BootstrapName, "test-bootstrap")
	}
}
