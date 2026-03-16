// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockExporter struct {
	exportCalled int
}

func (m *mockExporter) Export() {
	m.exportCalled++
}

func TestSinceInSeconds(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	result := sinceInSeconds(start)
	require.Greater(t, result, 1.0)
	require.Less(t, result, 10.0)
}

func TestSinceInSecondsRecent(t *testing.T) {
	start := time.Now()
	result := sinceInSeconds(start)
	require.GreaterOrEqual(t, result, 0.0)
	require.Less(t, result, 1.0)
}

func TestExportNilExporter(t *testing.T) {
	origExporter := exporter
	defer func() { exporter = origExporter }()
	exporter = nil

	// Should not panic when exporter is nil
	Export()
}

func TestExportWithExporter(t *testing.T) {
	origExporter := exporter
	defer func() { exporter = origExporter }()

	mock := &mockExporter{}
	exporter = mock

	Export()
	require.Equal(t, 1, mock.exportCalled)

	Export()
	require.Equal(t, 2, mock.exportCalled)
}

func TestConversionDuration(t *testing.T) {
	// Should not panic
	start := time.Now().Add(-1 * time.Second)
	ConversionDuration("test-ref", 3, start)
}

func TestConversionSuccessCount(t *testing.T) {
	// Should not panic
	ConversionSuccessCount("test-ref")
}

func TestConversionFailureCount(t *testing.T) {
	// Should not panic
	ConversionFailureCount("test-ref", "timeout")
}

func TestStoreCacheDuration(t *testing.T) {
	// Should not panic
	start := time.Now().Add(-500 * time.Millisecond)
	StoreCacheDuration("test-ref", start)
}

func TestExporterInterface(t *testing.T) {
	var exp Exporter
	mock := &mockExporter{}
	exp = mock
	exp.Export()
	require.Equal(t, 1, mock.exportCalled)
}
