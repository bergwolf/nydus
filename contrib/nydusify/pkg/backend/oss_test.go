// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"encoding/json"
	"hash/crc64"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func tempOSSBackend() *OSSBackend {
	ossConfigJSON := `
	{
		"bucket_name": "test",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	backend, _ := newOSSBackend([]byte(ossConfigJSON))
	return backend
}

func TestCalcCrc64ECMA(t *testing.T) {
	blobCrc64, err := calcCrc64ECMA("nil")
	require.Error(t, err)
	require.Contains(t, err.Error(), "calc md5sum")
	require.Zero(t, blobCrc64)

	file, err := os.CreateTemp("", "temp")
	require.NoError(t, err)
	defer os.RemoveAll(file.Name())

	_, err = file.WriteString("123")
	require.NoError(t, err)
	file.Sync()

	blobCrc64, err = calcCrc64ECMA(file.Name())
	require.NoError(t, err)
	require.Equal(t, crc64.Checksum([]byte("123"), crc64.MakeTable(crc64.ECMA)), blobCrc64)
}

func TestOSSRemoteID(t *testing.T) {
	ossBackend := tempOSSBackend()
	id := ossBackend.remoteID("111")
	require.Equal(t, "oss://test/blob111", id)
}

func TestNewOSSBackend(t *testing.T) {
	ossConfigJSON1 := `
	{
		"bucket_name": "test",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON1)))
	backend, err := newOSSBackend([]byte(ossConfigJSON1))
	require.NoError(t, err)
	require.Equal(t, "test", backend.bucket.BucketName)
	require.Equal(t, "blob", backend.objectPrefix)

	ossConfigJSON2 := `
	{
		"bucket_name": "test",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON2)))
	backend, err = newOSSBackend([]byte(ossConfigJSON2))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid OSS configuration: missing 'endpoint' or 'bucket'")
	require.Nil(t, backend)

	ossConfigJSON3 := `
	{
		"bucket_name": "test",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON3)))
	backend, err = newOSSBackend([]byte(ossConfigJSON3))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid OSS configuration: missing 'endpoint' or 'bucket'")
	require.Nil(t, backend)

	ossConfigJSON4 := `
	{
		"bucket_name": "t",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON4)))
	backend, err = newOSSBackend([]byte(ossConfigJSON4))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Create bucket")
	require.Contains(t, err.Error(), "len is between [3-63],now is")
	require.Nil(t, backend)

	ossConfigJSON5 := `
	{
		"bucket_name": "AAA",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(ossConfigJSON5)))
	backend, err = newOSSBackend([]byte(ossConfigJSON5))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Create bucket")
	require.Contains(t, err.Error(), "can only include lowercase letters, numbers, and -")
	require.Nil(t, backend)

	ossConfigJSON6 := `
	{
		"bucket_name": "AAA",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
	}`
	backend, err = newOSSBackend([]byte(ossConfigJSON6))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Parse OSS storage backend configuration")
	require.Nil(t, backend)
}

func TestOSSBackendObjectPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		blobID   string
		expected string
	}{
		{"with prefix", "prefix/", "blob1", "oss://test/prefix/blob1"},
		{"no prefix", "", "blob1", "oss://test/blob1"},
		{"nested prefix", "a/b/c/", "blob1", "oss://test/a/b/c/blob1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &OSSBackend{
				objectPrefix: tt.prefix,
				bucket:       tempOSSBackend().bucket,
			}
			require.Equal(t, tt.expected, b.remoteID(tt.blobID))
		})
	}
}

func TestOSSRangeReaderCreation(t *testing.T) {
	b := tempOSSBackend()
	rr, err := b.RangeReader("testblob")
	require.NoError(t, err)
	require.NotNil(t, rr)
}

func TestOSSBackendEmptyConfig(t *testing.T) {
	_, err := newOSSBackend([]byte(`{}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid OSS configuration")
}

func TestOSSBackendNoCredentials(t *testing.T) {
	cfg := `{"bucket_name": "test-bucket", "endpoint": "region.oss.com"}`
	b, err := newOSSBackend([]byte(cfg))
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, "", b.objectPrefix)
}
