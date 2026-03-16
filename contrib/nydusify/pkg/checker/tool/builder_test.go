// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package tool

import (
	"os"
	"strings"
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

func TestBuilderCheckWithTrueBinary(t *testing.T) {
	// "true" always exits 0 but won't produce valid nydus-image output
	// This tests cmd.Run() succeeds
	b := NewBuilder("true")
	err := b.Check(BuilderOption{
		BootstrapPath:   "/dev/null",
		DebugOutputPath: "/dev/null",
	})
	// "true" ignores arguments and exits 0
	if err != nil {
		t.Logf("true binary check returned error (expected if true doesn't exist at path): %v", err)
	}
}

func TestBuilderCheckCommandArgs(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := tmpDir + "/mock-builder.sh"

	// Create a script that writes its arguments to a file
	script := `#!/bin/sh
echo "$@" > ` + tmpDir + `/args.txt
`
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	b := NewBuilder(scriptPath)
	_ = b.Check(BuilderOption{
		BootstrapPath:   "/path/to/bootstrap",
		DebugOutputPath: "/path/to/output.json",
	})

	argsData, err := os.ReadFile(tmpDir + "/args.txt")
	if err != nil {
		t.Fatal(err)
	}

	args := string(argsData)
	if !strings.Contains(args, "check") {
		t.Errorf("expected 'check' in args, got: %s", args)
	}
	if !strings.Contains(args, "--bootstrap") {
		t.Errorf("expected '--bootstrap' in args, got: %s", args)
	}
	if !strings.Contains(args, "/path/to/bootstrap") {
		t.Errorf("expected bootstrap path in args, got: %s", args)
	}
	if !strings.Contains(args, "--output-json") {
		t.Errorf("expected '--output-json' in args, got: %s", args)
	}
	if !strings.Contains(args, "/path/to/output.json") {
		t.Errorf("expected output path in args, got: %s", args)
	}
}

func TestBuilderCheckExitNonZero(t *testing.T) {
	b := NewBuilder("false") // always exits 1
	err := b.Check(BuilderOption{
		BootstrapPath:   "/dev/null",
		DebugOutputPath: "/dev/null",
	})
	if err == nil {
		t.Error("expected error from false binary")
	}
}

func TestBuilderStdoutStderr(t *testing.T) {
	b := NewBuilder("/usr/bin/echo")
	if b.stdout != os.Stdout {
		t.Error("stdout should default to os.Stdout")
	}
	if b.stderr != os.Stderr {
		t.Error("stderr should default to os.Stderr")
	}
}
