// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func testS3BackendWithServer(t *testing.T, handler http.HandlerFunc) (*S3Backend, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(handler)

	endpointURL := ts.URL
	cfg, err := awscfg.LoadDefaultConfig(context.TODO(),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithRetryMaxAttempts(1),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
		o.Region = "us-east-1"
		o.UsePathStyle = true
	})

	return &S3Backend{
		objectPrefix:       "",
		bucketName:         "test-bucket",
		endpointWithScheme: endpointURL,
		client:             client,
	}, ts
}

func TestS3ExistObjectFound(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	exists, err := b.existObject(context.Background(), "test-key")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestS3ExistObjectNotFound(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	defer ts.Close()

	exists, err := b.existObject(context.Background(), "test-key")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3ExistObjectServerError(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	defer ts.Close()

	exists, err := b.existObject(context.Background(), "test-key")
	require.Error(t, err)
	require.False(t, exists)
}

func TestS3CheckFound(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusOK)
		}
	})
	defer ts.Close()

	exists, err := b.Check("test-blob")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestS3CheckNotFound(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	defer ts.Close()

	exists, err := b.Check("test-blob")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3UploadSkipExisting(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusOK)
		}
	})
	defer ts.Close()

	blobID := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	desc, err := b.Upload(context.Background(), blobID, "/nonexistent", 100, false)
	require.NoError(t, err)
	require.NotNil(t, desc)
	require.Equal(t, int64(100), desc.Size)
	require.Len(t, desc.URLs, 1)
}

func TestS3UploadFileOpenError(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	defer ts.Close()

	blobID := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	_, err := b.Upload(context.Background(), blobID, "/nonexistent/path/blob", 100, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open blob file")
}

func TestS3UploadExistenceCheckError(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusForbidden)
		}
	})
	defer ts.Close()

	blobID := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	_, err := b.Upload(context.Background(), blobID, "/nonexistent", 100, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "check object existence")
}

func TestS3ReaderSuccess(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	})
	defer ts.Close()

	rc, err := b.Reader("test-blob")
	require.NoError(t, err)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))
}

func TestS3RangeReaderRead(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-4/11")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("hello"))
	})
	defer ts.Close()

	rr, err := b.RangeReader("test-blob")
	require.NoError(t, err)

	rc, err := rr.Reader(0, 5)
	require.NoError(t, err)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}

func TestS3SizeError(t *testing.T) {
	b, ts := testS3BackendWithServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	defer ts.Close()

	_, err := b.Size("test-blob")
	require.Error(t, err)
}

func TestS3FinalizeWithBool(t *testing.T) {
	b := &S3Backend{}
	require.NoError(t, b.Finalize(true))
	require.NoError(t, b.Finalize(false))
}

func TestS3TypeConsistency(t *testing.T) {
	b := tempS3Backend()
	for i := 0; i < 3; i++ {
		require.Equal(t, S3backend, b.Type())
	}
}

func TestS3BlobObjectKeyEmpty(t *testing.T) {
	b := &S3Backend{objectPrefix: ""}
	require.Equal(t, "blobid", b.blobObjectKey("blobid"))
}

func TestS3RemoteIDWithCustomEndpoint(t *testing.T) {
	b := &S3Backend{
		endpointWithScheme: "http://minio:9000",
		bucketName:         "test",
	}
	id := b.remoteID("prefix/blobid")
	require.Equal(t, "http://minio:9000/test/prefix/blobid", id)
}
