// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"
)

func tempS3Backend() *S3Backend {
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
	backend, _ := newS3Backend([]byte(s3ConfigJSON))
	return backend
}

func TestS3RemoteID(t *testing.T) {
	s3Backend := tempS3Backend()
	id := s3Backend.remoteID("111")
	require.Equal(t, "https://s3.amazonaws.com/test/111", id)
}

func TestBlobObjectKey(t *testing.T) {
	s3Backend := tempS3Backend()
	blobObjectKey := s3Backend.blobObjectKey("111")
	require.Equal(t, "blob111", blobObjectKey)
}

func TestNewS3Backend(t *testing.T) {
	s3ConfigJSON1 := `
	{
		"bucket_name": "test",
		"endpoint": "s3.amazonaws.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
		"scheme": "https",
		"region": "region1"
	}`
	require.True(t, json.Valid([]byte(s3ConfigJSON1)))
	backend, err := newS3Backend([]byte(s3ConfigJSON1))
	require.NoError(t, err)
	require.Equal(t, "blob", backend.objectPrefix)
	require.Equal(t, "test", backend.bucketName)
	require.Equal(t, "https://s3.amazonaws.com", backend.endpointWithScheme)
	require.Equal(t, "https://s3.amazonaws.com", *backend.client.Options().BaseEndpoint)
	testCredentials, err := backend.client.Options().Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	realCredentials, err := credentials.NewStaticCredentialsProvider("testAK", "testSK", "").Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, testCredentials, realCredentials)

	s3ConfigJSON2 := `
	{
		"bucket_name": "test",
		"endpoint": "s3.amazonaws.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
		"scheme": "https",
		"region": "region1",
	}`
	backend, err = newS3Backend([]byte(s3ConfigJSON2))
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse S3 storage backend configuration")
	require.Nil(t, backend)

	s3ConfigJSON3 := `
	{
		"bucket_name": "test",
		"endpoint": "",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
		"scheme": "",
		"region": "region1"
	}`
	require.True(t, json.Valid([]byte(s3ConfigJSON3)))
	backend, err = newS3Backend([]byte(s3ConfigJSON3))
	require.NoError(t, err)
	require.Equal(t, "blob", backend.objectPrefix)
	require.Equal(t, "test", backend.bucketName)
	require.Equal(t, "https://s3.amazonaws.com", backend.endpointWithScheme)
	testCredentials, err = backend.client.Options().Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	realCredentials, err = credentials.NewStaticCredentialsProvider("testAK", "testSK", "").Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, testCredentials, realCredentials)

	s3ConfigJSON4 := `
	{
		"bucket_name": "",
		"endpoint": "s3.amazonaws.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"object_prefix": "blob",
		"scheme": "https",
		"region": ""
	}`
	require.True(t, json.Valid([]byte(s3ConfigJSON4)))
	backend, err = newS3Backend([]byte(s3ConfigJSON4))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid S3 configuration: missing 'bucket_name' or 'region'")
	require.Nil(t, backend)
}

func TestS3ConfigSerialization(t *testing.T) {
	cfg := S3Config{
		AccessKeyID:     "mykey",
		AccessKeySecret: "mysecret",
		Endpoint:        "s3.example.com",
		Scheme:          "http",
		BucketName:      "mybucket",
		Region:          "us-west-2",
		ObjectPrefix:    "prefix/",
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	var decoded S3Config
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, cfg, decoded)
}

func TestS3ConfigOmitEmpty(t *testing.T) {
	cfg := S3Config{
		BucketName: "mybucket",
		Region:     "us-east-1",
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)
	_, hasKey := m["access_key_id"]
	require.False(t, hasKey, "omitempty fields should not appear")
}

func TestS3RangeReaderCreation(t *testing.T) {
	b := tempS3Backend()
	rr, err := b.RangeReader("testblob")
	require.NoError(t, err)
	require.NotNil(t, rr)
}

func TestS3BlobObjectKeyWithPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		blobID   string
		expected string
	}{
		{"with prefix", "myprefix/", "blob123", "myprefix/blob123"},
		{"empty prefix", "", "blob123", "blob123"},
		{"nested prefix", "a/b/c/", "blob", "a/b/c/blob"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &S3Backend{objectPrefix: tt.prefix}
			require.Equal(t, tt.expected, b.blobObjectKey(tt.blobID))
		})
	}
}

func TestS3RemoteIDPaths(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		bucket         string
		blobKey        string
		expectedSuffix string
	}{
		{
			"standard",
			"https://s3.amazonaws.com",
			"mybucket",
			"myblob",
			"/mybucket/myblob",
		},
		{
			"with path in key",
			"https://s3.amazonaws.com",
			"mybucket",
			"prefix/myblob",
			"/mybucket/prefix/myblob",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &S3Backend{
				endpointWithScheme: tt.endpoint,
				bucketName:         tt.bucket,
			}
			id := b.remoteID(tt.blobKey)
			require.Contains(t, id, tt.expectedSuffix)
		})
	}
}
