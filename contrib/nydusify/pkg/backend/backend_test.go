// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"encoding/json"
	"testing"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestBlobDesc(t *testing.T) {
	desc := blobDesc(123456, "205eed24cbec29ad9cb4593a73168ef1803402370a82f7d51ce25646fc2f943a")
	require.Equal(t, int64(123456), desc.Size)
	require.Equal(t, "sha256:205eed24cbec29ad9cb4593a73168ef1803402370a82f7d51ce25646fc2f943a", desc.Digest.String())
	require.Equal(t, utils.MediaTypeNydusBlob, desc.MediaType)
	require.Equal(t, map[string]string{
		utils.LayerAnnotationUncompressed: "sha256:205eed24cbec29ad9cb4593a73168ef1803402370a82f7d51ce25646fc2f943a",
		utils.LayerAnnotationNydusBlob:    "true",
	}, desc.Annotations)
}

func TestBlobDescTableDriven(t *testing.T) {
	tests := []struct {
		name   string
		size   int64
		blobID string
	}{
		{
			name:   "zero size",
			size:   0,
			blobID: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:   "large size",
			size:   1099511627776,
			blobID: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			name:   "typical blob",
			size:   104857600,
			blobID: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := blobDesc(tt.size, tt.blobID)

			require.Equal(t, tt.size, desc.Size)
			require.Equal(t, utils.MediaTypeNydusBlob, desc.MediaType)

			expectedDigest := digest.NewDigestFromEncoded(digest.SHA256, tt.blobID)
			require.Equal(t, expectedDigest, desc.Digest)

			require.Equal(t, expectedDigest.String(), desc.Annotations[utils.LayerAnnotationUncompressed])
			require.Equal(t, "true", desc.Annotations[utils.LayerAnnotationNydusBlob])
			require.Len(t, desc.Annotations, 2)
		})
	}
}

func TestBlobDescAnnotationsNotShared(t *testing.T) {
	desc1 := blobDesc(100, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	desc2 := blobDesc(200, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	desc1.Annotations["extra"] = "val"
	_, exists := desc2.Annotations["extra"]
	require.False(t, exists, "annotations should not be shared between descriptors")
}

func TestBackendTypeConstants(t *testing.T) {
	require.Equal(t, 0, OssBackend)
	require.Equal(t, 1, RegistryBackend)
	require.Equal(t, 2, S3backend)
	require.NotEqual(t, OssBackend, RegistryBackend)
	require.NotEqual(t, RegistryBackend, S3backend)
}

func TestRegistryBackendType(t *testing.T) {
	r := &Registry{}
	require.Equal(t, RegistryBackend, r.Type())
}

func TestRegistryFinalize(t *testing.T) {
	r := &Registry{}
	require.NoError(t, r.Finalize(false))
	require.NoError(t, r.Finalize(true))
}

func TestRegistryCheck(t *testing.T) {
	r := &Registry{}
	ok, err := r.Check("any-blob-id")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestOSSBackendType(t *testing.T) {
	b := &OSSBackend{}
	require.Equal(t, OssBackend, b.Type())
}

func TestS3BackendType(t *testing.T) {
	b := tempS3Backend()
	require.Equal(t, S3backend, b.Type())
}

func TestS3Finalize(t *testing.T) {
	b := tempS3Backend()
	require.NoError(t, b.Finalize(false))
	require.NoError(t, b.Finalize(true))
}

func TestNewBackend(t *testing.T) {
	ossConfigJSON := `
	{
		"bucket_name": "test",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON)))
	backend, err := NewBackend("oss", []byte(ossConfigJSON), nil)
	require.NoError(t, err)
	require.Equal(t, OssBackend, backend.Type())

	s3ConfigJSON := `
	{
		"bucket_name": "test",
		"endpoint": "s3.amazonaws.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
		"scheme": "https",
		"region": "region1"
	}`
	require.True(t, json.Valid([]byte(s3ConfigJSON)))
	backend, err = NewBackend("s3", []byte(s3ConfigJSON), nil)
	require.NoError(t, err)
	require.Equal(t, S3backend, backend.Type())

	testRegistryRemote, err := provider.DefaultRemote("test", false)
	require.NoError(t, err)
	backend, err = NewBackend("registry", nil, testRegistryRemote)
	require.NoError(t, err)
	require.Equal(t, RegistryBackend, backend.Type())

	backend, err = NewBackend("errBackend", nil, testRegistryRemote)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported backend type")
	require.Nil(t, backend)
}

func TestNewBackendInvalidJSON(t *testing.T) {
	_, err := NewBackend("oss", []byte("not json"), nil)
	require.Error(t, err)

	_, err = NewBackend("s3", []byte("not json"), nil)
	require.Error(t, err)
}

func TestNewBackendOSSMissingEndpoint(t *testing.T) {
	cfg := `{"bucket_name": "test"}`
	_, err := NewBackend("oss", []byte(cfg), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid OSS configuration")
}

func TestNewBackendOSSMissingBucket(t *testing.T) {
	cfg := `{"endpoint": "region.oss.com"}`
	_, err := NewBackend("oss", []byte(cfg), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid OSS configuration")
}

func TestNewBackendS3MissingRegion(t *testing.T) {
	cfg := `{"bucket_name": "test", "endpoint": "s3.amazonaws.com"}`
	_, err := NewBackend("s3", []byte(cfg), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid S3 configuration")
}

func TestNewBackendS3MissingBucket(t *testing.T) {
	cfg := `{"region": "us-east-1", "endpoint": "s3.amazonaws.com"}`
	_, err := NewBackend("s3", []byte(cfg), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid S3 configuration")
}

func TestNewBackendS3DefaultEndpointAndScheme(t *testing.T) {
	cfg := `{"bucket_name": "test", "region": "us-east-1"}`
	b, err := NewBackend("s3", []byte(cfg), nil)
	require.NoError(t, err)
	s3b := b.(*S3Backend)
	require.Equal(t, "https://s3.amazonaws.com", s3b.endpointWithScheme)
}

func TestNewBackendS3CustomScheme(t *testing.T) {
	cfg := `{"bucket_name": "test", "region": "us-east-1", "scheme": "http", "endpoint": "minio:9000"}`
	b, err := NewBackend("s3", []byte(cfg), nil)
	require.NoError(t, err)
	s3b := b.(*S3Backend)
	require.Equal(t, "http://minio:9000", s3b.endpointWithScheme)
}

func TestNewBackendS3NoCredentials(t *testing.T) {
	cfg := `{"bucket_name": "test", "region": "us-east-1"}`
	b, err := NewBackend("s3", []byte(cfg), nil)
	require.NoError(t, err)
	require.NotNil(t, b)
}

func TestNewBackendVariousUnsupportedTypes(t *testing.T) {
	types := []string{"gcs", "azure", "", "OSS", "S3", "Registry"}
	for _, bt := range types {
		t.Run(bt, func(t *testing.T) {
			_, err := NewBackend(bt, nil, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported backend type")
		})
	}
}
