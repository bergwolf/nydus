package provider

import (
	"testing"

	"github.com/containerd/containerd/v2/core/images"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestImageName(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		cleanup     func(string) string
		want        string
	}{
		{
			name: "image name annotation present",
			annotations: map[string]string{
				images.AnnotationImageName: "docker.io/library/alpine:latest",
			},
			want: "docker.io/library/alpine:latest",
		},
		{
			name: "image name takes precedence over ref name",
			annotations: map[string]string{
				images.AnnotationImageName:   "docker.io/library/alpine:latest",
				ocispec.AnnotationRefName: "3.18",
			},
			want: "docker.io/library/alpine:latest",
		},
		{
			name: "ref name annotation without cleanup",
			annotations: map[string]string{
				ocispec.AnnotationRefName: "3.18",
			},
			cleanup: nil,
			want:    "3.18",
		},
		{
			name: "ref name annotation with cleanup",
			annotations: map[string]string{
				ocispec.AnnotationRefName: "3.18",
			},
			cleanup: func(s string) string { return "cleaned:" + s },
			want:    "cleaned:3.18",
		},
		{
			name:        "no annotations",
			annotations: map[string]string{},
			want:        "",
		},
		{
			name:        "nil annotations",
			annotations: nil,
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := imageName(tt.annotations, tt.cleanup)
			if got != tt.want {
				t.Errorf("imageName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestImportOptsStruct(t *testing.T) {
	opts := importOpts{
		indexName:     "test-index",
		compress:      true,
		discardLayers: false,
		skipMissing:   true,
		imageLabels:   map[string]string{"env": "test"},
	}
	if opts.indexName != "test-index" {
		t.Errorf("indexName = %q, want %q", opts.indexName, "test-index")
	}
	if !opts.compress {
		t.Error("compress should be true")
	}
	if !opts.skipMissing {
		t.Error("skipMissing should be true")
	}
}
