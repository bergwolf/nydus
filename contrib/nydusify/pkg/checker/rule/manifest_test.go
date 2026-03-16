// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package rule

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/remote"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestManifestName(t *testing.T) {
	rule := ManifestRule{}
	require.Equal(t, "manifest", rule.Name())
}

func TestManifestRuleValidate_IgnoreDeprecatedField(t *testing.T) {
	source := &parser.Parsed{
		Remote: &remote.Remote{},
		OCIImage: &parser.Image{
			Config: ocispec.Image{
				Config: ocispec.ImageConfig{
					ArgsEscaped: true, // deprecated field
				},
			},
		},
	}
	target := &parser.Parsed{
		Remote: &remote.Remote{},
		NydusImage: &parser.Image{
			Config: ocispec.Image{
				Config: ocispec.ImageConfig{
					ArgsEscaped: false,
				},
			},
		},
	}

	rule := ManifestRule{
		SourceParsed: source,
		TargetParsed: target,
	}

	require.Nil(t, rule.Validate())
}

func TestManifestRuleValidate_TargetLayer(t *testing.T) {
	rule := ManifestRule{}

	rule.TargetParsed = &parser.Parsed{
		Remote: &remote.Remote{},
		NydusImage: &parser.Image{
			Manifest: ocispec.Manifest{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Config: ocispec.Descriptor{
					MediaType: "application/vnd.oci.image.config.v1+json",
					Digest:    "sha256:563fad1f51cec2ee4c972af4bfd7275914061e2f73770585cfb04309cb5e0d6b",
					Size:      523,
				},
				Layers: []ocispec.Descriptor{
					{
						MediaType: "application / vnd.oci.image.layer.v1.tar",
						Digest:    "sha256:09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059",
						Size:      83528010,
						Annotations: map[string]string{
							"containerd.io/snapshot/nydus-blob": "true",
						},
					},
					{
						MediaType: "application/vnd.oci.image.layer.nydus.blob.v1",
						Digest:    "sha256:09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059",
						Size:      83528010,
						Annotations: map[string]string{
							"containerd.io/snapshot/nydus-blob": "true",
						},
					},
				},
			},
		},
	}
	require.Error(t, rule.Validate())
	require.Contains(t, rule.Validate().Error(), "invalid blob layer in nydus image manifest")

	rule.TargetParsed.NydusImage.Manifest.Layers = []ocispec.Descriptor{
		{
			MediaType: "application/vnd.oci.image.layer.nydus.blob.v1",
			Digest:    "sha256:09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059",
			Size:      83528010,
			Annotations: map[string]string{
				"containerd.io/snapshot/nydus-blob": "true",
			},
		},
	}
	require.Error(t, rule.Validate())
	require.Contains(t, rule.Validate().Error(), "invalid bootstrap layer in nydus image manifest")

	rule.TargetParsed.NydusImage.Config.RootFS.DiffIDs = []digest.Digest{
		"sha256:09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059",
		"sha256:bec98c9e3dce739877b8f5fe1cddd339de1db2b36c20995d76f6265056dbdb08",
	}

	rule.TargetParsed.NydusImage.Manifest.Layers = []ocispec.Descriptor{
		{
			MediaType: "application/vnd.oci.image.layer.nydus.blob.v1",
			Digest:    "sha256:09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059",
			Size:      83528010,
			Annotations: map[string]string{
				"containerd.io/snapshot/nydus-blob": "true",
			},
		},
		{
			MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
			Digest:    "sha256:aec98c9e3dce739877b8f5fe1cddd339de1db2b36c20995d76f6265056dbdb08",
			Size:      273320,
			Annotations: map[string]string{
				"containerd.io/snapshot/nydus-bootstrap":          "true",
				"containerd.io/snapshot/nydus-reference-blob-ids": "[\"09845cce1d983b158d4865fc37c23bbfb892d4775c786e8114d3cf868975c059\"]",
			},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateNilParsed(t *testing.T) {
	rule := ManifestRule{
		SourceParsed: nil,
		TargetParsed: nil,
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateSourceNilTargetValid(t *testing.T) {
	rule := ManifestRule{
		SourceParsed: nil,
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			OCIImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{MediaType: ocispec.MediaTypeImageLayerGzip},
					},
				},
				Config: ocispec.Image{
					RootFS: ocispec.RootFS{
						DiffIDs: []digest.Digest{"sha256:abc"},
					},
				},
			},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateOCIDiffIDMismatch(t *testing.T) {
	rule := ManifestRule{
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			OCIImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{MediaType: ocispec.MediaTypeImageLayerGzip},
						{MediaType: ocispec.MediaTypeImageLayerGzip},
					},
				},
				Config: ocispec.Image{
					RootFS: ocispec.RootFS{
						DiffIDs: []digest.Digest{"sha256:abc"},
					},
				},
			},
		},
	}
	err := rule.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid diff ids")
}

func TestManifestRuleValidateNydusValidLayers(t *testing.T) {
	rule := ManifestRule{
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			NydusImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{
							MediaType: utils.MediaTypeNydusBlob,
							Annotations: map[string]string{
								utils.LayerAnnotationNydusBlob: "true",
							},
						},
						{
							MediaType: ocispec.MediaTypeImageLayerGzip,
							Annotations: map[string]string{
								utils.LayerAnnotationNydusBootstrap: "true",
							},
						},
					},
				},
				Config: ocispec.Image{
					RootFS: ocispec.RootFS{
						DiffIDs: []digest.Digest{"sha256:abc", "sha256:def"},
					},
				},
			},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateNydusDiffIDMismatch(t *testing.T) {
	rule := ManifestRule{
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			NydusImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{
							MediaType: ocispec.MediaTypeImageLayerGzip,
							Annotations: map[string]string{
								utils.LayerAnnotationNydusBootstrap: "true",
							},
						},
					},
				},
				Config: ocispec.Image{
					RootFS: ocispec.RootFS{
						DiffIDs: []digest.Digest{"sha256:abc", "sha256:def"},
					},
				},
			},
		},
	}
	err := rule.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid diff ids")
}

func TestManifestRuleValidateNoImage(t *testing.T) {
	rule := ManifestRule{
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
		},
	}
	err := rule.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found valid image")
}

func TestManifestRuleValidateConfigMismatch(t *testing.T) {
	rule := ManifestRule{
		SourceParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			OCIImage: &parser.Image{
				Config: ocispec.Image{
					Config: ocispec.ImageConfig{
						Cmd: []string{"/bin/sh"},
					},
				},
			},
		},
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			NydusImage: &parser.Image{
				Config: ocispec.Image{
					Config: ocispec.ImageConfig{
						Cmd: []string{"/bin/bash"},
					},
				},
			},
		},
	}
	err := rule.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "source image config should be equal with target image config")
}

func TestManifestRuleValidateConfigMatch(t *testing.T) {
	cfg := ocispec.ImageConfig{
		Cmd:        []string{"/bin/sh"},
		Env:        []string{"PATH=/usr/bin"},
		WorkingDir: "/app",
	}
	rule := ManifestRule{
		SourceParsed: &parser.Parsed{
			Remote:   &remote.Remote{},
			OCIImage: &parser.Image{Config: ocispec.Image{Config: cfg}},
		},
		TargetParsed: &parser.Parsed{
			Remote:     &remote.Remote{},
			NydusImage: &parser.Image{Config: ocispec.Image{Config: cfg}},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateSourceNydusTargetNydus(t *testing.T) {
	cfg := ocispec.ImageConfig{Cmd: []string{"/bin/sh"}}
	rule := ManifestRule{
		SourceParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			NydusImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{
							MediaType: ocispec.MediaTypeImageLayerGzip,
							Annotations: map[string]string{
								utils.LayerAnnotationNydusBootstrap: "true",
							},
						},
					},
				},
				Config: ocispec.Image{
					Config: cfg,
					RootFS: ocispec.RootFS{DiffIDs: []digest.Digest{"sha256:abc"}},
				},
			},
		},
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			NydusImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{
							MediaType: ocispec.MediaTypeImageLayerGzip,
							Annotations: map[string]string{
								utils.LayerAnnotationNydusBootstrap: "true",
							},
						},
					},
				},
				Config: ocispec.Image{
					Config: cfg,
					RootFS: ocispec.RootFS{DiffIDs: []digest.Digest{"sha256:def"}},
				},
			},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestManifestRuleValidateOCIEmptyLayers(t *testing.T) {
	rule := ManifestRule{
		TargetParsed: &parser.Parsed{
			Remote: &remote.Remote{},
			OCIImage: &parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{},
				},
				Config: ocispec.Image{
					RootFS: ocispec.RootFS{
						DiffIDs: []digest.Digest{},
					},
				},
			},
		},
	}
	require.NoError(t, rule.Validate())
}

// Tests for BootstrapRule
func TestBootstrapRuleName(t *testing.T) {
	rule := BootstrapRule{}
	require.Equal(t, "bootstrap", rule.Name())
}

func TestBootstrapRuleValidateNilParsed(t *testing.T) {
	rule := BootstrapRule{
		SourceParsed: nil,
		TargetParsed: nil,
	}
	require.NoError(t, rule.Validate())
}

func TestBootstrapRuleValidateNilNydusImage(t *testing.T) {
	rule := BootstrapRule{
		SourceParsed: &parser.Parsed{
			Remote:   &remote.Remote{},
			OCIImage: &parser.Image{},
		},
		TargetParsed: &parser.Parsed{
			Remote:   &remote.Remote{},
			OCIImage: &parser.Image{},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestBootstrapRuleValidateSourceNil(t *testing.T) {
	rule := BootstrapRule{
		SourceParsed: nil,
		TargetParsed: &parser.Parsed{
			Remote:   &remote.Remote{},
			OCIImage: &parser.Image{},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestBootstrapRuleValidateTargetNil(t *testing.T) {
	rule := BootstrapRule{
		SourceParsed: &parser.Parsed{
			Remote:   &remote.Remote{},
			OCIImage: &parser.Image{},
		},
		TargetParsed: nil,
	}
	require.NoError(t, rule.Validate())
}

func TestBootstrapRuleFields(t *testing.T) {
	rule := BootstrapRule{
		WorkDir:             "/tmp/workdir",
		NydusImagePath:      "/usr/bin/nydus-image",
		SourceBackendType:   "oss",
		SourceBackendConfig: `{"bucket":"src"}`,
		TargetBackendType:   "registry",
		TargetBackendConfig: `{"host":"reg.io"}`,
	}
	require.Equal(t, "/tmp/workdir", rule.WorkDir)
	require.Equal(t, "/usr/bin/nydus-image", rule.NydusImagePath)
	require.Equal(t, "oss", rule.SourceBackendType)
	require.Equal(t, `{"bucket":"src"}`, rule.SourceBackendConfig)
	require.Equal(t, "registry", rule.TargetBackendType)
	require.Equal(t, `{"host":"reg.io"}`, rule.TargetBackendConfig)
}

// Tests for FilesystemRule
func TestFilesystemRuleName(t *testing.T) {
	rule := FilesystemRule{}
	require.Equal(t, "filesystem", rule.Name())
}

func TestFilesystemRuleValidateSkipsWhenNoParsed(t *testing.T) {
	rule := FilesystemRule{
		SourceImage: &Image{Parsed: nil},
		TargetImage: &Image{Parsed: nil},
	}
	require.NoError(t, rule.Validate())
}

func TestFilesystemRuleValidateSkipsWhenSourceNil(t *testing.T) {
	rule := FilesystemRule{
		SourceImage: &Image{Parsed: nil},
		TargetImage: &Image{
			Parsed: &parser.Parsed{Remote: &remote.Remote{}},
		},
	}
	require.NoError(t, rule.Validate())
}

func TestFilesystemRuleValidateSkipsWhenTargetNil(t *testing.T) {
	rule := FilesystemRule{
		SourceImage: &Image{
			Parsed: &parser.Parsed{Remote: &remote.Remote{}},
		},
		TargetImage: &Image{Parsed: nil},
	}
	require.NoError(t, rule.Validate())
}

// Tests for Node
func TestNodeString(t *testing.T) {
	node := Node{
		Path:    "/test/file.txt",
		Size:    1024,
		Mode:    0644,
		Rdev:    0,
		Symlink: "/test/file.txt",
		UID:     1000,
		GID:     1000,
		Xattrs:  map[string][]byte{"user.test": []byte("val")},
		Hash:    []byte{0xde, 0xad, 0xbe, 0xef},
	}

	str := node.String()
	require.Contains(t, str, "path: /test/file.txt")
	require.Contains(t, str, "size: 1024")
	require.Contains(t, str, "uid: 1000")
	require.Contains(t, str, "gid: 1000")
	require.Contains(t, str, hex.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}))
}

func TestNodeStringEmptyHash(t *testing.T) {
	node := Node{
		Path: "/empty",
	}
	str := node.String()
	require.Contains(t, str, "path: /empty")
	require.Contains(t, str, "hash: ")
}

func TestNodeStringAllFields(t *testing.T) {
	node := Node{
		Path:    "/a/b",
		Size:    999,
		Mode:    0755,
		Rdev:    42,
		Symlink: "/c/d",
		UID:     0,
		GID:     0,
		Xattrs:  nil,
		Hash:    []byte{0x01},
	}
	str := node.String()
	require.Contains(t, str, "path: /a/b")
	require.Contains(t, str, "size: 999")
	require.Contains(t, str, "mode: 493") // 0755 in decimal
	require.Contains(t, str, "rdev: 42")
	require.Contains(t, str, "symink: /c/d")
	require.Contains(t, str, "uid: 0")
	require.Contains(t, str, "gid: 0")
}

// Tests for Image struct
func TestImageStructFields(t *testing.T) {
	img := Image{
		Parsed:   &parser.Parsed{Remote: &remote.Remote{Ref: "test:latest"}},
		Insecure: true,
	}
	require.True(t, img.Insecure)
	require.Equal(t, "test:latest", img.Parsed.Remote.Ref)
}

func TestImageStructDefaults(t *testing.T) {
	img := Image{}
	require.Nil(t, img.Parsed)
	require.False(t, img.Insecure)
}

// Tests for RegistryBackendConfig
func TestRegistryBackendConfigFields(t *testing.T) {
	cfg := RegistryBackendConfig{
		Scheme:     "https",
		Host:       "registry.example.com",
		Repo:       "library/alpine",
		Auth:       "dXNlcjpwYXNz",
		SkipVerify: true,
	}
	require.Equal(t, "https", cfg.Scheme)
	require.Equal(t, "registry.example.com", cfg.Host)
	require.Equal(t, "library/alpine", cfg.Repo)
	require.Equal(t, "dXNlcjpwYXNz", cfg.Auth)
	require.True(t, cfg.SkipVerify)
}

func TestRegistryBackendConfigJSON(t *testing.T) {
	cfg := RegistryBackendConfig{
		Scheme: "https",
		Host:   "reg.io",
		Repo:   "myrepo",
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	var decoded RegistryBackendConfig
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, cfg.Scheme, decoded.Scheme)
	require.Equal(t, cfg.Host, decoded.Host)
	require.Equal(t, cfg.Repo, decoded.Repo)
	require.Empty(t, decoded.Auth)
	require.False(t, decoded.SkipVerify)
}

func TestRegistryBackendConfigOmitEmpty(t *testing.T) {
	cfg := RegistryBackendConfig{
		Scheme: "https",
		Host:   "reg.io",
		Repo:   "myrepo",
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)
	_, hasAuth := m["auth"]
	require.False(t, hasAuth)
	_, hasSkip := m["skip_verify"]
	require.False(t, hasSkip)
}

// Tests for FilesystemRule.walk
func TestFilesystemRuleWalk(t *testing.T) {
	tmpDir := t.TempDir()

	require.NoError(t, os.MkdirAll(tmpDir+"/subdir", 0755))
	require.NoError(t, os.WriteFile(tmpDir+"/file1.txt", []byte("hello"), 0644))
	require.NoError(t, os.WriteFile(tmpDir+"/subdir/file2.txt", []byte("world"), 0644))

	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)
	require.NotNil(t, nodes)

	require.Contains(t, nodes, "/")
	require.Contains(t, nodes, "/file1.txt")
	require.Contains(t, nodes, "/subdir")
	require.Contains(t, nodes, "/subdir/file2.txt")

	require.Equal(t, int64(5), nodes["/file1.txt"].Size)
	require.Equal(t, int64(5), nodes["/subdir/file2.txt"].Size)
	require.Equal(t, int64(0), nodes["/"].Size)

	require.NotNil(t, nodes["/file1.txt"].Hash)
	require.NotNil(t, nodes["/subdir/file2.txt"].Hash)
}

func TestFilesystemRuleWalkEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	rule := FilesystemRule{}
	nodes, err := rule.walk(tmpDir)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Contains(t, nodes, "/")
}

func TestFilesystemRuleVerifyIdentical(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(dir1+"/a.txt", []byte("content"), 0644))
	require.NoError(t, os.WriteFile(dir2+"/a.txt", []byte("content"), 0644))
	require.NoError(t, os.MkdirAll(dir1+"/sub", 0755))
	require.NoError(t, os.MkdirAll(dir2+"/sub", 0755))
	require.NoError(t, os.WriteFile(dir1+"/sub/b.txt", []byte("data"), 0644))
	require.NoError(t, os.WriteFile(dir2+"/sub/b.txt", []byte("data"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.NoError(t, err)
}

func TestFilesystemRuleVerifyMissingInTarget(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(dir1+"/a.txt", []byte("content"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not found in target image")
}

func TestFilesystemRuleVerifyMissingInSource(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(dir2+"/extra.txt", []byte("extra"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not found in source image")
}

func TestFilesystemRuleVerifyContentMismatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	require.NoError(t, os.WriteFile(dir1+"/a.txt", []byte("hello"), 0644))
	require.NoError(t, os.WriteFile(dir2+"/a.txt", []byte("world"), 0644))

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "file not match")
}

func TestFilesystemRuleVerifyBothEmpty(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	rule := FilesystemRule{}
	err := rule.verify(dir1, dir2)
	require.NoError(t, err)
}

func TestFilesystemRuleFields(t *testing.T) {
	rule := FilesystemRule{
		WorkDir:             "/tmp/workdir",
		NydusdPath:          "/usr/bin/nydusd",
		SourceBackendType:   "oss",
		SourceBackendConfig: `{"bucket":"src"}`,
		TargetBackendType:   "registry",
		TargetBackendConfig: `{"host":"reg.io"}`,
	}
	require.Equal(t, "/tmp/workdir", rule.WorkDir)
	require.Equal(t, "/usr/bin/nydusd", rule.NydusdPath)
	require.Equal(t, "oss", rule.SourceBackendType)
	require.Equal(t, `{"bucket":"src"}`, rule.SourceBackendConfig)
}

// Test Rule interface implementation
func TestRuleInterfaceImplementations(t *testing.T) {
	var _ Rule = &ManifestRule{}
	var _ Rule = &BootstrapRule{}
	var _ Rule = &FilesystemRule{}
}

// Test WorkerCount variable
func TestWorkerCount(t *testing.T) {
	require.Equal(t, uint(8), WorkerCount)
}
