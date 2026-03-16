// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package converter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostsReturnsFunction(t *testing.T) {
	opt := Opt{
		Source:            "source.example.com/image:latest",
		SourceInsecure:    true,
		Target:            "target.example.com/image:latest",
		TargetInsecure:    false,
		ChunkDictRef:      "dict.example.com/dict:v1",
		ChunkDictInsecure: true,
		CacheRef:          "cache.example.com/cache:v1",
		CacheInsecure:     false,
	}

	hostFunc := hosts(opt)
	require.NotNil(t, hostFunc)

	// Source ref should be insecure
	credFunc, insecure, err := hostFunc(opt.Source)
	require.NoError(t, err)
	require.True(t, insecure)
	require.NotNil(t, credFunc)

	// Target ref should not be insecure
	_, insecure, err = hostFunc(opt.Target)
	require.NoError(t, err)
	require.False(t, insecure)

	// ChunkDict ref should be insecure
	_, insecure, err = hostFunc(opt.ChunkDictRef)
	require.NoError(t, err)
	require.True(t, insecure)

	// Cache ref should not be insecure
	_, insecure, err = hostFunc(opt.CacheRef)
	require.NoError(t, err)
	require.False(t, insecure)
}

func TestHostsUnknownRef(t *testing.T) {
	opt := Opt{
		Source:         "source.example.com/image:latest",
		SourceInsecure: true,
	}

	hostFunc := hosts(opt)

	// Unknown ref defaults to false in the map
	_, insecure, err := hostFunc("unknown.example.com/image:latest")
	require.NoError(t, err)
	require.False(t, insecure)
}

func TestHostsEmptyOpt(t *testing.T) {
	opt := Opt{}
	hostFunc := hosts(opt)
	require.NotNil(t, hostFunc)

	_, insecure, err := hostFunc("")
	require.NoError(t, err)
	require.False(t, insecure)
}
