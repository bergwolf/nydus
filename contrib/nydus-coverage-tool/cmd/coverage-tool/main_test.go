package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCollectModuleFiles(t *testing.T) {
	// Create a temporary directory structure for testing
	tmpDir, err := os.MkdirTemp("", "module-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	testFiles := map[string]string{
		"target.rs":     "// Target file\npub fn target() {}",
		"mod.rs":        "// Module file\nmod target;",
		"helper.rs":     "// Helper file\npub fn helper() {}",
		"another.rs":    "// Another file\npub fn another() {}",
		"test_file.rs":  "// Test file - should be skipped\n#[test]\nfn test() {}",
		"README.md":     "# README - should be skipped",
	}

	for name, content := range testFiles {
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", name, err)
		}
	}

	// Test collectModuleFiles
	targetFile := filepath.Join(tmpDir, "target.rs")
	moduleFiles, err := collectModuleFiles(targetFile)
	if err != nil {
		t.Fatalf("collectModuleFiles failed: %v", err)
	}

	// Verify results
	expectedFiles := []string{"mod.rs", "helper.rs", "another.rs"}
	if len(moduleFiles) != len(expectedFiles) {
		t.Errorf("Expected %d module files, got %d", len(expectedFiles), len(moduleFiles))
	}

	// Check that expected files are present
	for _, expected := range expectedFiles {
		if _, exists := moduleFiles[expected]; !exists {
			t.Errorf("Expected file %s not found in module files", expected)
		}
	}

	// Check that excluded files are not present
	excludedFiles := []string{"target.rs", "test_file.rs", "README.md"}
	for _, excluded := range excludedFiles {
		if _, exists := moduleFiles[excluded]; exists {
			t.Errorf("File %s should have been excluded but was included", excluded)
		}
	}

	// Verify content
	if content, exists := moduleFiles["helper.rs"]; exists {
		if content != "// Helper file\npub fn helper() {}" {
			t.Errorf("Unexpected content for helper.rs: %s", content)
		}
	} else {
		t.Error("helper.rs should be in module files")
	}
}

func TestCollectModuleFilesEmptyDir(t *testing.T) {
	// Create a temporary directory with only the target file
	tmpDir, err := os.MkdirTemp("", "module-test-empty-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create only target file
	targetFile := filepath.Join(tmpDir, "target.rs")
	if err := os.WriteFile(targetFile, []byte("// Target"), 0644); err != nil {
		t.Fatalf("Failed to create target file: %v", err)
	}

	// Test with empty directory
	moduleFiles, err := collectModuleFiles(targetFile)
	if err != nil {
		t.Fatalf("collectModuleFiles failed: %v", err)
	}

	if len(moduleFiles) != 0 {
		t.Errorf("Expected 0 module files for empty dir, got %d", len(moduleFiles))
	}
}
