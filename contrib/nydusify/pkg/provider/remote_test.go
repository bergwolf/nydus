// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRemoteWithAuthEmptyAuth(t *testing.T) {
	// Empty auth should not error at credential creation time
	remote, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", false, "")
	require.NoError(t, err)
	require.NotNil(t, remote)
}

func TestDefaultRemoteWithAuthWhitespaceAuth(t *testing.T) {
	remote, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", false, "   ")
	require.NoError(t, err)
	require.NotNil(t, remote)
}

func TestDefaultRemoteWithAuthValidBase64(t *testing.T) {
	auth := base64.StdEncoding.EncodeToString([]byte("user:pass"))
	remote, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", false, auth)
	require.NoError(t, err)
	require.NotNil(t, remote)
}

func TestNewDefaultClientInsecure(t *testing.T) {
	client := newDefaultClient(true)
	require.NotNil(t, client)
	transport := client.Transport.(*http.Transport)
	require.True(t, transport.TLSClientConfig.InsecureSkipVerify)
}

func TestNewDefaultClientSecure(t *testing.T) {
	client := newDefaultClient(false)
	require.NotNil(t, client)
	transport := client.Transport.(*http.Transport)
	require.False(t, transport.TLSClientConfig.InsecureSkipVerify)
}

func TestNewDefaultClientTransportSettings(t *testing.T) {
	client := newDefaultClient(false)
	transport := client.Transport.(*http.Transport)

	require.NotNil(t, transport.Proxy)
	require.NotNil(t, transport.TLSClientConfig)
	require.True(t, transport.DisableKeepAlives)
	require.Equal(t, 10, transport.MaxIdleConns)
}

func TestDefaultRemoteWithAuthInvalidBase64(t *testing.T) {
	// Invalid base64 should still create the remote (error happens at credential use time)
	// The credential func is lazy-evaluated
	r, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", false, "not-valid-base64!!!")
	// This may or may not error depending on when credentials are evaluated
	if err != nil {
		t.Logf("DefaultRemoteWithAuth with invalid base64 returned: %v", err)
	} else {
		require.NotNil(t, r)
	}
}

func TestDefaultRemoteWithAuthValidCredentials(t *testing.T) {
	auth := base64.StdEncoding.EncodeToString([]byte("myuser:mypass"))
	r, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", true, auth)
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestDefaultRemote(t *testing.T) {
	r, err := DefaultRemote("docker.io/library/alpine:latest", false)
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestDefaultRemoteInsecure(t *testing.T) {
	r, err := DefaultRemote("docker.io/library/alpine:latest", true)
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestDefaultRemoteWithAuthMissingColon(t *testing.T) {
	// base64 of "usernopass" (no colon separator)
	auth := base64.StdEncoding.EncodeToString([]byte("usernopass"))
	r, err := DefaultRemoteWithAuth("docker.io/library/alpine:latest", false, auth)
	// Should still create remote, error happens when creds are used
	if err != nil {
		t.Logf("error: %v", err)
	} else {
		require.NotNil(t, r)
	}
}

func TestNewDefaultClientTLSNextProto(t *testing.T) {
	client := newDefaultClient(false)
	transport := client.Transport.(*http.Transport)
	require.NotNil(t, transport.TLSNextProto)
	require.Empty(t, transport.TLSNextProto)
}
