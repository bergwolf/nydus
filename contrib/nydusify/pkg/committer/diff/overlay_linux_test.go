package diff

import (
	"syscall"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
)

func TestGetOverlayLayers(t *testing.T) {
	tests := []struct {
		name    string
		mount   mount.Mount
		want    []string
		wantErr bool
	}{
		{
			name: "single lowerdir only",
			mount: mount.Mount{
				Options: []string{"lowerdir=/lower1"},
			},
			want: []string{"/lower1"},
		},
		{
			name: "multiple lowerdirs reversed",
			mount: mount.Mount{
				Options: []string{"lowerdir=/lower3:/lower2:/lower1"},
			},
			want: []string{"/lower1", "/lower2", "/lower3"},
		},
		{
			name: "lowerdir and upperdir",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/lower2:/lower1",
					"upperdir=/upper",
				},
			},
			want: []string{"/lower1", "/lower2", "/upper"},
		},
		{
			name: "upperdir only",
			mount: mount.Mount{
				Options: []string{"upperdir=/upper"},
			},
			want: []string{"/upper"},
		},
		{
			name: "with workdir and index=off",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/lower1",
					"upperdir=/upper",
					"workdir=/work",
					"index=off",
				},
			},
			want: []string{"/lower1", "/upper"},
		},
		{
			name: "with userxattr",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/lower1",
					"userxattr",
				},
			},
			want: []string{"/lower1"},
		},
		{
			name: "with redirect_dir",
			mount: mount.Mount{
				Options: []string{
					"lowerdir=/lower1",
					"redirect_dir=on",
				},
			},
			want: []string{"/lower1"},
		},
		{
			name:  "empty options",
			mount: mount.Mount{Options: []string{}},
			want:  nil,
		},
		{
			name: "unknown option causes error",
			mount: mount.Mount{
				Options: []string{"lowerdir=/lower1", "nosuid"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetOverlayLayers(tt.mount)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetOverlayLayers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("GetOverlayLayers() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("GetOverlayLayers()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestGetUpperdir(t *testing.T) {
	tests := []struct {
		name    string
		lower   []mount.Mount
		upper   []mount.Mount
		want    string
		wantErr bool
	}{
		{
			name:  "bottommost snapshot (bind mount)",
			lower: []mount.Mount{},
			upper: []mount.Mount{
				{Type: "bind", Source: "/snapshots/1/fs"},
			},
			want: "/snapshots/1/fs",
		},
		{
			name: "single lower bind, upper overlay",
			lower: []mount.Mount{
				{Type: "bind", Source: "/snapshots/1/fs"},
			},
			upper: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/snapshots/1/fs",
					"upperdir=/snapshots/2/fs",
				}},
			},
			want: "/snapshots/2/fs",
		},
		{
			name: "lower overlay, upper overlay",
			lower: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/snapshots/2/fs:/snapshots/1/fs",
				}},
			},
			upper: []mount.Mount{
				{Type: "overlay", Options: []string{
					"lowerdir=/snapshots/2/fs:/snapshots/1/fs",
					"upperdir=/snapshots/3/fs",
				}},
			},
			want: "/snapshots/3/fs",
		},
		{
			name:    "upper is not bind for bottommost",
			lower:   []mount.Mount{},
			upper:   []mount.Mount{{Type: "overlay"}},
			wantErr: true,
		},
		{
			name:    "multiple lowers unsupported",
			lower:   []mount.Mount{{}, {}},
			upper:   []mount.Mount{{}},
			wantErr: true,
		},
		{
			name: "upper is not overlay for non-bottommost",
			lower: []mount.Mount{
				{Type: "bind", Source: "/lower"},
			},
			upper: []mount.Mount{
				{Type: "bind", Source: "/upper"},
			},
			wantErr: true,
		},
		{
			name: "lower type unsupported",
			lower: []mount.Mount{
				{Type: "tmpfs"},
			},
			upper: []mount.Mount{
				{Type: "overlay"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetUpperdir(tt.lower, tt.upper)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUpperdir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("GetUpperdir() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompareSysStat(t *testing.T) {
	tests := []struct {
		name string
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{
			name: "identical stats",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 0},
			s2:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 0},
			want: true,
		},
		{
			name: "different mode",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000},
			s2:   &syscall.Stat_t{Mode: 0755, Uid: 1000, Gid: 1000},
			want: false,
		},
		{
			name: "different uid",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000},
			s2:   &syscall.Stat_t{Mode: 0644, Uid: 2000, Gid: 1000},
			want: false,
		},
		{
			name: "different gid",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000},
			s2:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 2000},
			want: false,
		},
		{
			name: "different rdev",
			s1:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 1},
			s2:   &syscall.Stat_t{Mode: 0644, Uid: 1000, Gid: 1000, Rdev: 2},
			want: false,
		},
		{
			name: "first not Stat_t",
			s1:   "not a stat",
			s2:   &syscall.Stat_t{},
			want: false,
		},
		{
			name: "second not Stat_t",
			s1:   &syscall.Stat_t{},
			s2:   "not a stat",
			want: false,
		},
		{
			name: "both not Stat_t",
			s1:   42,
			s2:   "string",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareSysStat(tt.s1, tt.s2)
			if err != nil {
				t.Fatalf("compareSysStat() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("compareSysStat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCancellableWriter(t *testing.T) {
	t.Run("writes to underlying writer", func(t *testing.T) {
		var buf []byte
		w := &cancellableWriter{
			ctx: t.Context(),
			w: writerFunc(func(p []byte) (int, error) {
				buf = append(buf, p...)
				return len(p), nil
			}),
		}
		n, err := w.Write([]byte("hello"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if n != 5 {
			t.Errorf("Write() = %d, want 5", n)
		}
		if string(buf) != "hello" {
			t.Errorf("buf = %q, want %q", string(buf), "hello")
		}
	})
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
