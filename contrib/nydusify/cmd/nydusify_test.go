// Copyright 2023 Alibaba Cloud. All rights reserved.
// Copyright 2023 Nydus Developers. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestIsPossibleValue(t *testing.T) {
	value := "qwe"
	list := []string{"abc", "qwe", "xyz"}
	require.True(t, isPossibleValue(list, value))

	// Failure situation
	value2 := "vdf"
	require.False(t, isPossibleValue(list, value2))
}

func TestAddReferenceSuffix(t *testing.T) {
	source := "localhost:5000/nginx:latest"
	suffix := "-suffix"
	target, err := addReferenceSuffix(source, suffix)
	require.NoError(t, err)
	require.Equal(t, target, "localhost:5000/nginx:latest-suffix")

	// Failure situation
	source = "localhost:5000\nginx:latest"
	suffix = "-suffix"
	_, err = addReferenceSuffix(source, suffix)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid source image reference")

	source = "localhost:5000/nginx:latest@sha256:757574c5a2102627de54971a0083d4ecd24eb48fdf06b234d063f19f7bbc22fb"
	suffix = "-suffix"
	_, err = addReferenceSuffix(source, suffix)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported digested image reference")
}

func TestParseBackendConfig(t *testing.T) {
	configJSON := `
	{
		"bucket_name": "test",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"meta_prefix": "meta",
		"blob_prefix": "blob"
	}`
	require.True(t, json.Valid([]byte(configJSON)))

	file, err := os.CreateTemp("", "nydusify-backend-config-test.json")
	require.NoError(t, err)
	defer os.RemoveAll(file.Name())

	_, err = file.WriteString(configJSON)
	require.NoError(t, err)
	file.Sync()

	resultJSON, err := parseBackendConfig("", file.Name())
	require.NoError(t, err)
	require.True(t, json.Valid([]byte(resultJSON)))
	require.Equal(t, configJSON, resultJSON)

	// Failure situation
	_, err = parseBackendConfig(configJSON, file.Name())
	require.Error(t, err)

	_, err = parseBackendConfig("", "non-existent.json")
	require.Error(t, err)
}

func TestGetBackendConfig(t *testing.T) {
	tests := []struct {
		backendType   string
		backendConfig string
	}{
		{
			backendType: "oss",
			backendConfig: `
	{
		"bucket_name": "test",
		"endpoint": "region.oss.com",
		"access_key_id": "testAK",
		"access_key_secret": "testSK",
		"meta_prefix": "meta",
		"blob_prefix": "blob"
	}`,
		},
		{
			backendType: "localfs",
			backendConfig: `
	{
		"dir": "/path/to/blobs"
	}`,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("backend config %s", test.backendType), func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "prefixbackend-type",
						Value: "",
					},
					&cli.StringFlag{
						Name:  "prefixbackend-config",
						Value: "",
					},
					&cli.StringFlag{
						Name:  "prefixbackend-config-file",
						Value: "",
					},
				},
			}
			ctx := cli.NewContext(app, nil, nil)

			backendType, backendConfig, err := getBackendConfig(ctx, "prefix", false)
			require.NoError(t, err)
			require.Empty(t, backendType)
			require.Empty(t, backendConfig)

			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.Error(t, err)
			require.Contains(t, err.Error(), "backend type is empty, please specify option")
			require.Empty(t, backendType)
			require.Empty(t, backendConfig)

			flagSet := flag.NewFlagSet("test1", flag.PanicOnError)
			flagSet.String("prefixbackend-type", "errType", "")
			ctx = cli.NewContext(app, flagSet, nil)
			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.Error(t, err)
			require.Contains(t, err.Error(), "backend-type should be one of")
			require.Empty(t, backendType)
			require.Empty(t, backendConfig)

			flagSet = flag.NewFlagSet("test2", flag.PanicOnError)
			flagSet.String("prefixbackend-type", test.backendType, "")
			ctx = cli.NewContext(app, flagSet, nil)
			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.Error(t, err)
			require.Contains(t, err.Error(), "backend configuration is empty, please specify option")
			require.Empty(t, backendType)
			require.Empty(t, backendConfig)

			require.True(t, json.Valid([]byte(test.backendConfig)))

			flagSet = flag.NewFlagSet("test3", flag.PanicOnError)
			flagSet.String("prefixbackend-type", test.backendType, "")
			flagSet.String("prefixbackend-config", test.backendConfig, "")
			ctx = cli.NewContext(app, flagSet, nil)
			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.NoError(t, err)
			require.Equal(t, test.backendType, backendType)
			require.Equal(t, test.backendConfig, backendConfig)

			file, err := os.CreateTemp("", "nydusify-backend-config-test.json")
			require.NoError(t, err)
			defer os.RemoveAll(file.Name())

			_, err = file.WriteString(test.backendConfig)
			require.NoError(t, err)
			file.Sync()

			flagSet = flag.NewFlagSet("test4", flag.PanicOnError)
			flagSet.String("prefixbackend-type", test.backendType, "")
			flagSet.String("prefixbackend-config-file", file.Name(), "")
			ctx = cli.NewContext(app, flagSet, nil)
			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.NoError(t, err)
			require.Equal(t, test.backendType, backendType)
			require.Equal(t, test.backendConfig, backendConfig)

			flagSet = flag.NewFlagSet("test5", flag.PanicOnError)
			flagSet.String("prefixbackend-type", test.backendType, "")
			flagSet.String("prefixbackend-config", test.backendConfig, "")
			flagSet.String("prefixbackend-config-file", file.Name(), "")
			ctx = cli.NewContext(app, flagSet, nil)
			backendType, backendConfig, err = getBackendConfig(ctx, "prefix", true)
			require.Error(t, err)
			require.Contains(t, err.Error(), "--backend-config conflicts with --backend-config-file")
			require.Empty(t, backendType)
			require.Empty(t, backendConfig)
		})
	}
}

func TestGetTargetReference(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "target",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "target-suffix",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "source",
				Value: "",
			},
		},
	}
	ctx := cli.NewContext(app, nil, nil)

	target, err := getTargetReference(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--target or --target-suffix is required")
	require.Empty(t, target)

	flagSet := flag.NewFlagSet("test1", flag.PanicOnError)
	flagSet.String("target", "testTarget", "")
	flagSet.String("target-suffix", "testSuffix", "")
	ctx = cli.NewContext(app, flagSet, nil)
	target, err = getTargetReference(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "-target conflicts with --target-suffix")
	require.Empty(t, target)

	flagSet = flag.NewFlagSet("test2", flag.PanicOnError)
	flagSet.String("target-suffix", "-nydus", "")
	flagSet.String("source", "localhost:5000/nginx:latest", "")
	ctx = cli.NewContext(app, flagSet, nil)
	target, err = getTargetReference(ctx)
	require.NoError(t, err)
	require.Equal(t, "localhost:5000/nginx:latest-nydus", target)

	flagSet = flag.NewFlagSet("test3", flag.PanicOnError)
	flagSet.String("target-suffix", "-nydus", "")
	flagSet.String("source", "localhost:5000\nginx:latest", "")
	ctx = cli.NewContext(app, flagSet, nil)
	target, err = getTargetReference(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid source image reference")
	require.Empty(t, target)

	flagSet = flag.NewFlagSet("test4", flag.PanicOnError)
	flagSet.String("target", "testTarget", "")
	ctx = cli.NewContext(app, flagSet, nil)
	target, err = getTargetReference(ctx)
	require.NoError(t, err)
	require.Equal(t, "testTarget", target)
}

func TestGetCacheReference(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "build-cache",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "build-cache-tag",
				Value: "",
			},
		},
	}
	ctx := cli.NewContext(app, nil, nil)

	cache, err := getCacheReference(ctx, "")
	require.NoError(t, err)
	require.Empty(t, cache)

	flagSet := flag.NewFlagSet("test1", flag.PanicOnError)
	flagSet.String("build-cache", "cache", "")
	flagSet.String("build-cache-tag", "cacheTag", "")
	ctx = cli.NewContext(app, flagSet, nil)
	cache, err = getCacheReference(ctx, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "--build-cache conflicts with --build-cache-tag")
	require.Empty(t, cache)

	flagSet = flag.NewFlagSet("test2", flag.PanicOnError)
	flagSet.String("build-cache-tag", "cacheTag", "errTarget")
	ctx = cli.NewContext(app, flagSet, nil)
	cache, err = getCacheReference(ctx, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid target image reference: invalid reference format")
	require.Empty(t, cache)

	flagSet = flag.NewFlagSet("test2", flag.PanicOnError)
	flagSet.String("build-cache-tag", "latest-cache", "")
	ctx = cli.NewContext(app, flagSet, nil)
	cache, err = getCacheReference(ctx, "localhost:5000/nginx:latest")
	require.NoError(t, err)
	require.Equal(t, "localhost:5000/nginx:latest-cache", cache)
}

func TestGetPrefetchPatterns(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "prefetch-dir",
				Value: "",
			},
			&cli.BoolFlag{
				Name:  "prefetch-patterns",
				Value: false,
			},
		},
	}
	ctx := cli.NewContext(app, nil, nil)

	patterns, err := getPrefetchPatterns(ctx)
	require.NoError(t, err)
	require.Equal(t, "/", patterns)

	flagSet := flag.NewFlagSet("test1", flag.PanicOnError)
	flagSet.String("prefetch-dir", "/etc/passwd", "")
	ctx = cli.NewContext(app, flagSet, nil)
	patterns, err = getPrefetchPatterns(ctx)
	require.NoError(t, err)
	require.Equal(t, "/etc/passwd", patterns)

	flagSet = flag.NewFlagSet("test2", flag.PanicOnError)
	flagSet.String("prefetch-dir", "/etc/passwd", "")
	flagSet.Bool("prefetch-patterns", true, "")
	ctx = cli.NewContext(app, flagSet, nil)
	patterns, err = getPrefetchPatterns(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--prefetch-dir conflicts with --prefetch-patterns")
	require.Empty(t, patterns)

	flagSet = flag.NewFlagSet("test3", flag.PanicOnError)
	flagSet.Bool("prefetch-patterns", true, "")
	ctx = cli.NewContext(app, flagSet, nil)
	patterns, err = getPrefetchPatterns(ctx)
	require.NoError(t, err)
	require.Equal(t, "/", patterns)
}

func TestGetGlobalFlags(t *testing.T) {
	flags := getGlobalFlags()
	require.Equal(t, 3, len(flags))
}

func TestSetupLogLevelWithLogFile(t *testing.T) {
	logFilePath := "test_log_file.log"
	defer os.Remove(logFilePath)

	c := &cli.Context{}

	patches := gomonkey.ApplyMethodSeq(c, "String", []gomonkey.OutputCell{
		{Values: []interface{}{"info"}, Times: 1},
		{Values: []interface{}{"test_log_file.log"}, Times: 2},
	})
	defer patches.Reset()
	setupLogLevel(c)

	file, err := os.Open(logFilePath)
	assert.NoError(t, err)
	assert.NotNil(t, file)
	file.Close()

	logrusOutput := logrus.StandardLogger().Out
	assert.NotNil(t, logrusOutput)

	logrus.Info("This is a test log message")
	content, err := os.ReadFile(logFilePath)
	assert.NoError(t, err)
	assert.Contains(t, string(content), "This is a test log message")
}

func TestSetupLogLevelWithInvalidLogFile(t *testing.T) {

	c := &cli.Context{}

	patches := gomonkey.ApplyMethodSeq(c, "String", []gomonkey.OutputCell{
		{Values: []interface{}{"info"}, Times: 1},
		{Values: []interface{}{"test/test_log_file.log"}, Times: 2},
	})
	defer patches.Reset()
	setupLogLevel(c)

	logrusOutput := logrus.StandardLogger().Out
	assert.NotNil(t, logrusOutput)
}

func TestValidateSourceAndTargetArchives(t *testing.T) {
	// Create temporary source archive for valid cases
	validSourceFile, err := os.CreateTemp("", "source-archive-*.tar")
	require.NoError(t, err)
	defer os.Remove(validSourceFile.Name())
	validSourceFile.Close()

	// Create temporary directory for valid target cases
	validTargetDir, err := os.MkdirTemp("", "target-dir-*")
	require.NoError(t, err)
	defer os.RemoveAll(validTargetDir)

	tests := []struct {
		name          string
		sourceArchive string
		targetArchive string
		expectError   bool
		errorContains string
	}{
		{
			name:          "no archives specified",
			sourceArchive: "",
			targetArchive: "",
			expectError:   false,
		},
		{
			name:          "valid source archive only",
			sourceArchive: validSourceFile.Name(),
			targetArchive: "",
			expectError:   false,
		},
		{
			name:          "non-existent source archive",
			sourceArchive: "/path/to/non-existent-source.tar",
			targetArchive: "",
			expectError:   true,
			errorContains: "source archive not accessible",
		},
		{
			name:          "valid target archive with existing directory",
			sourceArchive: "",
			targetArchive: fmt.Sprintf("%s/target-archive.tar", validTargetDir),
			expectError:   false,
		},
		{
			name:          "target archive with non-existent directory",
			sourceArchive: "",
			targetArchive: "/non/existent/directory/target.tar",
			expectError:   true,
			errorContains: "target archive directory not accessible",
		},
		{
			name:          "valid source and target archives",
			sourceArchive: validSourceFile.Name(),
			targetArchive: fmt.Sprintf("%s/target-archive.tar", validTargetDir),
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "source-archive",
						Value: "",
					},
					&cli.StringFlag{
						Name:  "target-archive",
						Value: "",
					},
				},
			}

			flagSet := flag.NewFlagSet("test", flag.PanicOnError)
			flagSet.String("source-archive", tt.sourceArchive, "")
			flagSet.String("target-archive", tt.targetArchive, "")
			ctx := cli.NewContext(app, flagSet, nil)

			err := validateSourceAndTargetArchives(ctx)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsPossibleValueEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		list     []string
		value    string
		expected bool
	}{
		{"empty list", []string{}, "value", false},
		{"empty value", []string{"a", "b"}, "", false},
		{"empty both", []string{}, "", false},
		{"single item match", []string{"only"}, "only", true},
		{"single item no match", []string{"only"}, "other", false},
		{"first item", []string{"a", "b", "c"}, "a", true},
		{"last item", []string{"a", "b", "c"}, "c", true},
		{"case sensitive", []string{"ABC"}, "abc", false},
		{"whitespace", []string{" a "}, " a ", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isPossibleValue(tt.list, tt.value))
		})
	}
}

func TestAddReferenceSuffixEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		suffix      string
		expected    string
		expectError bool
	}{
		{
			name:     "empty suffix",
			source:   "localhost:5000/nginx:latest",
			suffix:   "",
			expected: "localhost:5000/nginx:latest",
		},
		{
			name:     "long suffix",
			source:   "localhost:5000/nginx:latest",
			suffix:   "-nydus-v2-optimized",
			expected: "localhost:5000/nginx:latest-nydus-v2-optimized",
		},
		{
			name:     "docker hub reference",
			source:   "library/alpine:3.18",
			suffix:   "-nydus",
			expected: "docker.io/library/alpine:3.18-nydus",
		},
		{
			name:     "no tag defaults to latest",
			source:   "myregistry.io/myrepo",
			suffix:   "-converted",
			expected: "myregistry.io/myrepo:latest-converted",
		},
		{
			name:        "digest reference not supported",
			source:      "nginx@sha256:757574c5a2102627de54971a0083d4ecd24eb48fdf06b234d063f19f7bbc22fb",
			suffix:      "-nydus",
			expectError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := addReferenceSuffix(tt.source, tt.suffix)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseBackendConfigBothEmpty(t *testing.T) {
	result, err := parseBackendConfig("", "")
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestParseBackendConfigInlineJSON(t *testing.T) {
	configJSON := `{"key": "value"}`
	result, err := parseBackendConfig(configJSON, "")
	require.NoError(t, err)
	require.Equal(t, configJSON, result)
}

func TestSetupLogLevelDebugFlag(t *testing.T) {
	originalLevel := logrus.GetLevel()
	originalOutput := logrus.StandardLogger().Out
	defer func() {
		logrus.SetLevel(originalLevel)
		logrus.SetOutput(originalOutput)
	}()

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "D", Value: false},
			&cli.StringFlag{Name: "log-level", Value: "info"},
			&cli.StringFlag{Name: "log-file", Value: ""},
		},
	}

	flagSet := flag.NewFlagSet("debug-test", flag.PanicOnError)
	flagSet.Bool("D", true, "")
	ctx := cli.NewContext(app, flagSet, nil)

	setupLogLevel(ctx)
	require.Equal(t, logrus.DebugLevel, logrus.GetLevel())
}

func TestSetupLogLevelInvalidLevel(t *testing.T) {
	originalLevel := logrus.GetLevel()
	originalOutput := logrus.StandardLogger().Out
	defer func() {
		logrus.SetLevel(originalLevel)
		logrus.SetOutput(originalOutput)
	}()

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "D", Value: false},
			&cli.StringFlag{Name: "log-level", Value: "info"},
			&cli.StringFlag{Name: "log-file", Value: ""},
		},
	}

	flagSet := flag.NewFlagSet("invalid-level-test", flag.PanicOnError)
	flagSet.String("log-level", "not-a-valid-level", "")
	ctx := cli.NewContext(app, flagSet, nil)

	setupLogLevel(ctx)
	// Should fall back to default log level (info)
	require.Equal(t, defaultLogLevel, logrus.GetLevel())
}

func TestSetupLogLevelValidLevels(t *testing.T) {
	originalLevel := logrus.GetLevel()
	originalOutput := logrus.StandardLogger().Out
	defer func() {
		logrus.SetLevel(originalLevel)
		logrus.SetOutput(originalOutput)
	}()

	levels := []struct {
		name  string
		level logrus.Level
	}{
		{"debug", logrus.DebugLevel},
		{"warn", logrus.WarnLevel},
		{"error", logrus.ErrorLevel},
		{"trace", logrus.TraceLevel},
	}

	for _, tt := range levels {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "D", Value: false},
					&cli.StringFlag{Name: "log-level", Value: "info"},
					&cli.StringFlag{Name: "log-file", Value: ""},
				},
			}

			flagSet := flag.NewFlagSet("level-test-"+tt.name, flag.PanicOnError)
			flagSet.String("log-level", tt.name, "")
			ctx := cli.NewContext(app, flagSet, nil)

			setupLogLevel(ctx)
			require.Equal(t, tt.level, logrus.GetLevel())
		})
	}
}

func TestGetGlobalFlagNames(t *testing.T) {
	flags := getGlobalFlags()
	require.Equal(t, 3, len(flags))

	flagNames := make(map[string]bool)
	for _, f := range flags {
		for _, name := range f.Names() {
			flagNames[name] = true
		}
	}

	require.True(t, flagNames["debug"])
	require.True(t, flagNames["D"])
	require.True(t, flagNames["log-level"])
	require.True(t, flagNames["l"])
	require.True(t, flagNames["log-file"])
}

func TestGetCacheReferenceDirectCache(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "build-cache", Value: ""},
			&cli.StringFlag{Name: "build-cache-tag", Value: ""},
		},
	}

	flagSet := flag.NewFlagSet("cache-test", flag.PanicOnError)
	flagSet.String("build-cache", "localhost:5000/cache:v1", "")
	ctx := cli.NewContext(app, flagSet, nil)

	cache, err := getCacheReference(ctx, "localhost:5000/nginx:latest")
	require.NoError(t, err)
	require.Equal(t, "localhost:5000/cache:v1", cache)
}

func TestGetBackendConfigS3Type(t *testing.T) {
	configJSON := `{"bucket_name": "test", "region": "us-east-1"}`

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "prefixbackend-type", Value: ""},
			&cli.StringFlag{Name: "prefixbackend-config", Value: ""},
			&cli.StringFlag{Name: "prefixbackend-config-file", Value: ""},
		},
	}

	flagSet := flag.NewFlagSet("s3-config-test", flag.PanicOnError)
	flagSet.String("prefixbackend-type", "s3", "")
	flagSet.String("prefixbackend-config", configJSON, "")
	ctx := cli.NewContext(app, flagSet, nil)

	bt, bc, err := getBackendConfig(ctx, "prefix", true)
	require.NoError(t, err)
	require.Equal(t, "s3", bt)
	require.Equal(t, configJSON, bc)
}

func TestMaxCacheMaxRecordsValue(t *testing.T) {
	require.Equal(t, uint(200), maxCacheMaxRecords)
}

func TestDefaultLogLevel(t *testing.T) {
	require.Equal(t, logrus.InfoLevel, defaultLogLevel)
}

func TestTryReverseConvert(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "work-dir", Value: "./tmp"},
			&cli.StringFlag{Name: "nydus-image", Value: "nydus-image"},
			&cli.StringFlag{Name: "source", Value: ""},
			&cli.BoolFlag{Name: "source-insecure", Value: false},
			&cli.BoolFlag{Name: "target-insecure", Value: false},
			&cli.BoolFlag{Name: "all-platforms", Value: false},
			&cli.StringFlag{Name: "platform", Value: "linux/amd64"},
			&cli.StringFlag{Name: "output-json", Value: ""},
			&cli.IntFlag{Name: "push-retry-count", Value: 3},
			&cli.StringFlag{Name: "push-retry-delay", Value: "5s"},
			&cli.BoolFlag{Name: "plain-http", Value: false},
		},
	}

	tmpDir := t.TempDir()
	flagSet := flag.NewFlagSet("reverse-test", flag.PanicOnError)
	flagSet.String("work-dir", tmpDir, "")
	flagSet.String("nydus-image", "nydus-image", "")
	flagSet.String("source", "localhost:1/source:latest", "")
	flagSet.Bool("source-insecure", false, "")
	flagSet.Bool("target-insecure", false, "")
	flagSet.Bool("all-platforms", false, "")
	flagSet.String("platform", "linux/amd64", "")
	flagSet.String("output-json", "", "")
	flagSet.Int("push-retry-count", 1, "")
	flagSet.String("push-retry-delay", "1s", "")
	flagSet.Bool("plain-http", true, "")
	ctx := cli.NewContext(app, flagSet, nil)

	converted, err := tryReverseConvert(ctx, "localhost:1/target:latest")
	require.True(t, converted)
	// ReverseConvert will fail because the source registry is unreachable
	require.Error(t, err)
}

func TestGetPrefetchPatternsStdinError(t *testing.T) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	// Create a pipe and close the read end to simulate broken stdin
	r, w, err := os.Pipe()
	require.NoError(t, err)
	w.Close()
	r.Close()
	os.Stdin = r

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "prefetch-dir", Value: ""},
			&cli.BoolFlag{Name: "prefetch-patterns", Value: false},
		},
	}
	flagSet := flag.NewFlagSet("stdin-error", flag.PanicOnError)
	flagSet.Bool("prefetch-patterns", true, "")
	ctx := cli.NewContext(app, flagSet, nil)

	_, err = getPrefetchPatterns(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read prefetch patterns from STDIN")
}

func TestGetPrefetchPatternsStdinData(t *testing.T) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdin = r

	go func() {
		w.WriteString("/etc\n/bin\n")
		w.Close()
	}()

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "prefetch-dir", Value: ""},
			&cli.BoolFlag{Name: "prefetch-patterns", Value: false},
		},
	}
	flagSet := flag.NewFlagSet("stdin-data", flag.PanicOnError)
	flagSet.Bool("prefetch-patterns", true, "")
	ctx := cli.NewContext(app, flagSet, nil)

	patterns, err := getPrefetchPatterns(ctx)
	require.NoError(t, err)
	require.Equal(t, "/etc\n/bin\n", patterns)
}

func TestMainCheckCommand(t *testing.T) {
	// Save global state
	oldArgs := os.Args
	oldLevel := logrus.GetLevel()
	oldOutput := logrus.StandardLogger().Out
	oldFormatter := logrus.StandardLogger().Formatter

	defer func() {
		os.Args = oldArgs
		logrus.SetLevel(oldLevel)
		logrus.SetOutput(oldOutput)
		logrus.SetFormatter(oldFormatter)
		logrus.StandardLogger().ExitFunc = nil
	}()

	// Prevent logrus.Fatal from calling os.Exit
	logrus.StandardLogger().ExitFunc = func(code int) {}

	tmpDir := t.TempDir()
	os.Args = []string{"nydusify", "check",
		"--target", "localhost:1/test:latest",
		"--work-dir", tmpDir,
	}

	main()
}

func TestMainConvertCommand(t *testing.T) {
	oldArgs := os.Args
	oldLevel := logrus.GetLevel()
	oldOutput := logrus.StandardLogger().Out
	oldFormatter := logrus.StandardLogger().Formatter

	defer func() {
		os.Args = oldArgs
		logrus.SetLevel(oldLevel)
		logrus.SetOutput(oldOutput)
		logrus.SetFormatter(oldFormatter)
		logrus.StandardLogger().ExitFunc = nil
	}()

	logrus.StandardLogger().ExitFunc = func(code int) {}

	tmpDir := t.TempDir()
	os.Args = []string{"nydusify", "convert",
		"--source", "localhost:1/source:latest",
		"--target", "localhost:1/target:latest",
		"--work-dir", tmpDir,
	}

	main()
}
