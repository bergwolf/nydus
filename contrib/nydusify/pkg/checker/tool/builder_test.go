// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package tool

import (
	"os"
	"testing"
)

func TestNewToolBuilder(t *testing.T) {
	b := NewBuilder("/usr/bin/nydus-image")
	if b == nil {
		t.Fatal("NewBuilder returned nil")
	}
	if b.binaryPath != "/usr/bin/nydus-image" {
		t.Errorf("binaryPath = %q, want %q", b.binaryPath, "/usr/bin/nydus-image")
	}
	if b.stdout != os.Stdout {
		t.Error("stdout should be os.Stdout")
	}
	if b.stderr != os.Stderr {
		t.Error("stderr should be os.Stderr")
	}
}

func TestNewToolBuilderEmptyPath(t *testing.T) {
	b := NewBuilder("")
	if b == nil {
		t.Fatal("NewBuilder returned nil")
	}
	if b.binaryPath != "" {
		t.Errorf("binaryPath = %q, want empty", b.binaryPath)
	}
}

func TestToolBuilderCheckInvalidBinary(t *testing.T) {
	b := NewBuilder("/nonexistent/binary")
	err := b.Check(BuilderOption{
		BootstrapPath:   "/tmp/bootstrap",
		DebugOutputPath: "/tmp/output.json",
	})
	if err == nil {
		t.Error("Check with invalid binary should return error")
	}
}

func TestBuilderOptionFields(t *testing.T) {
	opt := BuilderOption{
		BootstrapPath:   "/path/to/bootstrap",
		DebugOutputPath: "/path/to/output.json",
	}
	if opt.BootstrapPath != "/path/to/bootstrap" {
		t.Errorf("BootstrapPath = %q", opt.BootstrapPath)
	}
	if opt.DebugOutputPath != "/path/to/output.json" {
		t.Errorf("DebugOutputPath = %q", opt.DebugOutputPath)
	}
}

func TestBuilderOptionDefaults(t *testing.T) {
	opt := BuilderOption{}
	if opt.BootstrapPath != "" {
		t.Errorf("BootstrapPath should be empty, got %q", opt.BootstrapPath)
	}
	if opt.DebugOutputPath != "" {
		t.Errorf("DebugOutputPath should be empty, got %q", opt.DebugOutputPath)
	}
}
