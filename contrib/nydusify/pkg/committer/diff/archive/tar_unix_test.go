//go:build !windows

package archive

import (
	"os"
	"testing"
)

func TestChmodTarEntry(t *testing.T) {
	tests := []struct {
		name string
		perm os.FileMode
		want os.FileMode
	}{
		{"regular file", 0644, 0644},
		{"executable", 0755, 0755},
		{"read only", 0444, 0444},
		{"all perms", 0777, 0777},
		{"no perms", 0000, 0000},
		{"setuid", os.ModeSetuid | 0755, os.ModeSetuid | 0755},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chmodTarEntry(tt.perm)
			if got != tt.want {
				t.Errorf("chmodTarEntry(%o) = %o, want %o", tt.perm, got, tt.want)
			}
		})
	}
}
