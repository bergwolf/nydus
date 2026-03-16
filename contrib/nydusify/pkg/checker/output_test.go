// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package checker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrettyDump(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	data := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	err := prettyDump(data, outputPath)
	require.NoError(t, err)

	// Verify file was created and contains valid JSON
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var result map[string]string
	err = json.Unmarshal(content, &result)
	require.NoError(t, err)
	require.Equal(t, "value1", result["key1"])
	require.Equal(t, "value2", result["key2"])
}

func TestPrettyDumpIndented(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	data := map[string]string{"key": "value"}
	err := prettyDump(data, outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	// Verify indentation (2 spaces)
	expected := "{\n  \"key\": \"value\"\n}"
	require.Equal(t, expected, string(content))
}

func TestPrettyDumpInvalidPath(t *testing.T) {
	err := prettyDump("data", "/nonexistent/dir/output.json")
	require.Error(t, err)
}

func TestPrettyDumpComplexStruct(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	type nested struct {
		Name  string   `json:"name"`
		Items []string `json:"items"`
	}

	data := nested{
		Name:  "test",
		Items: []string{"a", "b", "c"},
	}

	err := prettyDump(data, outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var result nested
	err = json.Unmarshal(content, &result)
	require.NoError(t, err)
	require.Equal(t, "test", result.Name)
	require.Equal(t, []string{"a", "b", "c"}, result.Items)
}

func TestPrettyDumpNilValue(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	err := prettyDump(nil, outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, "null", string(content))
}

func TestPrettyDumpEmptyMap(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	err := prettyDump(map[string]string{}, outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, "{}", string(content))
}

func TestPrettyDumpEmptySlice(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	err := prettyDump([]string{}, outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, "[]", string(content))
}

func TestPrettyDumpOverwritesExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	err := prettyDump("first", outputPath)
	require.NoError(t, err)

	err = prettyDump("second", outputPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, `"second"`, string(content))
}

func TestPrettyDumpNestedDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "sub", "dir", "output.json")

	// Should fail because parent directory doesn't exist
	err := prettyDump("data", outputPath)
	require.Error(t, err)
}

func TestPrettyDumpUnmarshalableType(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	// Channels cannot be marshaled to JSON
	err := prettyDump(make(chan int), outputPath)
	require.Error(t, err)

	// File should not be created on marshal failure
	_, err = os.Stat(outputPath)
	require.True(t, os.IsNotExist(err))
}

func TestPrettyDumpFuncType(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	// Functions cannot be marshaled to JSON
	err := prettyDump(func() {}, outputPath)
	require.Error(t, err)
}

func TestPrettyDumpWithNumericTypes(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"integer", 42, "42"},
		{"float", 3.14, "3.14"},
		{"negative", -1, "-1"},
		{"zero", 0, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(tmpDir, tt.name+".json")
			err := prettyDump(tt.value, path)
			require.NoError(t, err)

			content, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(content))
		})
	}
}

func TestPrettyDumpWithBoolean(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bool.json")

	err := prettyDump(true, path)
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "true", string(content))
}

func TestPrettyDumpLargeStruct(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large.json")

	// Create a large nested structure
	type inner struct {
		Key   string            `json:"key"`
		Tags  map[string]string `json:"tags"`
		Items []int             `json:"items"`
	}
	data := struct {
		Name   string  `json:"name"`
		Nested []inner `json:"nested"`
	}{
		Name: "test",
		Nested: []inner{
			{Key: "a", Tags: map[string]string{"env": "prod"}, Items: []int{1, 2, 3}},
			{Key: "b", Tags: map[string]string{"env": "dev"}, Items: []int{4, 5}},
		},
	}

	err := prettyDump(data, path)
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(content), "\"name\": \"test\"")
	require.Contains(t, string(content), "\"env\": \"prod\"")
}
