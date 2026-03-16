// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package fileexporter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/metrics"
)

func TestNew(t *testing.T) {
	exp := New("test-metrics.prom")
	require.NotNil(t, exp)
	require.Equal(t, "test-metrics.prom", exp.name)
}

func TestNewWithEmptyName(t *testing.T) {
	exp := New("")
	require.NotNil(t, exp)
	require.Equal(t, "", exp.name)
}

func TestFileExporterImplementsExporter(t *testing.T) {
	var _ metrics.Exporter = &FileExporter{}
}

func TestFileExporterExportWithRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := tmpDir + "/metrics.prom"

	mock := &mockRegistryExporter{}
	metrics.Register(mock)

	exp := New(outputPath)
	// Export writes to text file; with a valid registry it should not panic
	exp.Export()
}

type mockRegistryExporter struct{}

func (m *mockRegistryExporter) Export() {}
