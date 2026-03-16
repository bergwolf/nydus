package provider

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestNewDefaultClient(t *testing.T) {
	tests := []struct {
		name          string
		skipTLSVerify bool
	}{
		{"with TLS verification", false},
		{"without TLS verification", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newDefaultClient(tt.skipTLSVerify)
			if client == nil {
				t.Fatal("newDefaultClient() returned nil")
			}
			transport, ok := client.Transport.(*http.Transport)
			if !ok {
				t.Fatal("Transport is not *http.Transport")
			}
			if transport.TLSClientConfig.InsecureSkipVerify != tt.skipTLSVerify {
				t.Errorf("InsecureSkipVerify = %v, want %v",
					transport.TLSClientConfig.InsecureSkipVerify, tt.skipTLSVerify)
			}
			if !transport.DisableKeepAlives {
				t.Error("DisableKeepAlives should be true")
			}
			if transport.MaxIdleConns != 10 {
				t.Errorf("MaxIdleConns = %d, want 10", transport.MaxIdleConns)
			}
			if transport.IdleConnTimeout != 30*time.Second {
				t.Errorf("IdleConnTimeout = %v, want 30s", transport.IdleConnTimeout)
			}
		})
	}
}

func TestProviderUsePlainHTTP(t *testing.T) {
	pvd := &Provider{}
	if pvd.usePlainHTTP {
		t.Error("initially usePlainHTTP should be false")
	}
	pvd.UsePlainHTTP()
	if !pvd.usePlainHTTP {
		t.Error("after UsePlainHTTP(), should be true")
	}
}

func TestProviderImageNotFound(t *testing.T) {
	pvd := &Provider{
		images: make(map[string]*ocispec.Descriptor),
	}
	_, err := pvd.Image(context.Background(), "nonexistent")
	if !errdefs.IsNotFound(err) {
		t.Errorf("Image() for missing ref: error = %v, want NotFound", err)
	}
}

func TestProviderSetPushRetryConfig(t *testing.T) {
	pvd := &Provider{
		pushRetryCount: 3,
		pushRetryDelay: 5 * time.Second,
	}
	pvd.SetPushRetryConfig(5, 10*time.Second)
	if pvd.pushRetryCount != 5 {
		t.Errorf("pushRetryCount = %d, want 5", pvd.pushRetryCount)
	}
	if pvd.pushRetryDelay != 10*time.Second {
		t.Errorf("pushRetryDelay = %v, want 10s", pvd.pushRetryDelay)
	}
}

func TestProviderWithLocalSource(t *testing.T) {
	pvd := &Provider{}
	pvd.WithLocalSource("/path/to/source.tar")
	if pvd.localSource != "/path/to/source.tar" {
		t.Errorf("localSource = %q, want %q", pvd.localSource, "/path/to/source.tar")
	}
}

func TestProviderWithLocalTarget(t *testing.T) {
	pvd := &Provider{}
	pvd.WithLocalTarget("/path/to/target.tar")
	if pvd.localTarget != "/path/to/target.tar" {
		t.Errorf("localTarget = %q, want %q", pvd.localTarget, "/path/to/target.tar")
	}
}

func TestLayerConcurrentLimit(t *testing.T) {
	if LayerConcurrentLimit != 5 {
		t.Errorf("LayerConcurrentLimit = %d, want 5", LayerConcurrentLimit)
	}
}

func TestProviderContentStore(t *testing.T) {
	pvd := &Provider{store: nil}
	if pvd.ContentStore() != nil {
		t.Error("ContentStore() should return nil when store is nil")
	}
}

func TestErrdefs(t *testing.T) {
	// Verify errdefs package is usable (used extensively in provider)
	if !errdefs.IsNotFound(errdefs.ErrNotFound) {
		t.Error("IsNotFound(ErrNotFound) should be true")
	}
	if !errdefs.IsAlreadyExists(errdefs.ErrAlreadyExists) {
		t.Error("IsAlreadyExists(ErrAlreadyExists) should be true")
	}
}
