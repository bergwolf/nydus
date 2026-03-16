// Copyright 2024 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultLogger(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)
	require.NotNil(t, logger)
}

func TestDefaultLoggerImplementsInterface(t *testing.T) {
	var _ ProgressLogger = &defaultLogger{}
}

func TestDefaultLoggerLog(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	done := logger.Log(context.Background(), "test message", nil)
	require.NotNil(t, done)

	err = done(nil)
	require.NoError(t, err)
}

func TestDefaultLoggerLogWithFields(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	fields := LoggerFields{
		"layer":    "sha256:abc123",
		"progress": 50,
	}

	done := logger.Log(context.Background(), "converting layer", fields)
	require.NotNil(t, done)

	err = done(nil)
	require.NoError(t, err)
}

func TestDefaultLoggerLogWithError(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	done := logger.Log(context.Background(), "converting layer", nil)
	require.NotNil(t, done)

	testErr := context.Canceled
	err = done(testErr)
	require.Equal(t, testErr, err)
}

func TestDefaultLoggerLogDurationTracked(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	fields := LoggerFields{}
	done := logger.Log(context.Background(), "timing test", fields)

	time.Sleep(10 * time.Millisecond)
	err = done(nil)
	require.NoError(t, err)
	// After calling done, the fields map should have a "Time" key
	require.Contains(t, fields, "Time")
	require.NotEmpty(t, fields["Time"])
}

func TestDefaultLoggerLogPassesErrorThrough(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	tests := []struct {
		name string
		err  error
	}{
		{"nil error", nil},
		{"context canceled", context.Canceled},
		{"custom error", fmt.Errorf("something went wrong")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := logger.Log(context.Background(), "test", nil)
			result := done(tt.err)
			require.Equal(t, tt.err, result)
		})
	}
}

func TestDefaultLoggerMultipleCalls(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	// Multiple Log calls should work independently
	done1 := logger.Log(context.Background(), "first", nil)
	done2 := logger.Log(context.Background(), "second", LoggerFields{"key": "val"})

	require.NoError(t, done1(nil))
	require.NoError(t, done2(nil))
}

func TestDefaultLoggerFieldsMutated(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	fields := LoggerFields{"initial": "value"}
	done := logger.Log(context.Background(), "test", fields)

	time.Sleep(5 * time.Millisecond)
	err = done(nil)
	require.NoError(t, err)

	// After calling done, fields should have "Time" added
	require.Contains(t, fields, "Time")
	// Original field should still be present
	require.Equal(t, "value", fields["initial"])
}

func TestDefaultLoggerNilFieldsInitialized(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	// Passing nil fields should not panic
	done := logger.Log(context.Background(), "nil fields test", nil)
	require.NotNil(t, done)

	err = done(nil)
	require.NoError(t, err)
}

func TestDefaultLoggerErrorPassthrough(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{"nil error", nil, true},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"custom error", fmt.Errorf("custom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := logger.Log(context.Background(), "test", nil)
			result := done(tt.err)
			if tt.wantNil {
				require.NoError(t, result)
			} else {
				require.Equal(t, tt.err, result)
			}
		})
	}
}

func TestLoggerFieldsType(t *testing.T) {
	// LoggerFields is a type alias, verify it works as map
	fields := LoggerFields{
		"string":  "value",
		"int":     42,
		"float":   3.14,
		"bool":    true,
		"nil":     nil,
	}
	require.Len(t, fields, 5)
	require.Equal(t, "value", fields["string"])
	require.Equal(t, 42, fields["int"])
}

func TestProgressLoggerInterface(t *testing.T) {
	logger, err := DefaultLogger()
	require.NoError(t, err)

	// Ensure it satisfies the ProgressLogger interface
	var pl ProgressLogger = logger
	require.NotNil(t, pl)
}
