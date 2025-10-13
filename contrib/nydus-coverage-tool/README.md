# Nydus Coverage Tool

A command-line tool for automated test coverage improvement using AI-powered test generation.

## Overview

This tool analyzes Rust code coverage, generates unit tests using GitHub Models API (gpt-4o-mini), validates the tests, and produces detailed coverage reports.

## Building

```bash
make build
```

Or for a static release build:

```bash
make release
```

## Usage

The tool provides four main commands:

### Analyze Coverage

Analyzes current test coverage and identifies the least covered file:

```bash
./coverage-tool analyze --output-dir /tmp
```

**Output:**
- `/tmp/coverage_analysis.json` - Selected file details
- `/tmp/overall_coverage.json` - Project coverage statistics

### Generate Tests

Generates unit tests using AI for the selected file:

```bash
export GITHUB_TOKEN=your_token_here
./coverage-tool generate --output-dir /tmp
```

**Output:**
- `/tmp/updated_file.rs` - File with generated tests
- `/tmp/test_generation_metadata.json` - Generation metadata

### Validate Tests

Validates that generated tests compile and pass:

```bash
./coverage-tool validate --output-dir /tmp --max-retries 3
```

**Options:**
- `--max-retries`: Maximum validation attempts (default: 3)

### Generate Report

Creates a coverage improvement report:

```bash
./coverage-tool report --output-dir /tmp
```

**Output:**
- `/tmp/coverage_report.md` - Markdown coverage report

## Dependencies

- Rust toolchain with `cargo llvm-cov`
- GitHub token for Models API access

## Integration

This tool is used by the Coverage Improvement GitHub Actions workflow defined in `.github/workflows/coverage-improvement.yml`.

## Development

```bash
# Run tests
make test

# Clean build artifacts
make clean
```
