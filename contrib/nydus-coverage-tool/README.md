# Nydus Coverage Tool

A command-line tool for automated test coverage improvement using AI-powered test generation.

## Overview

This tool analyzes Rust code coverage, generates unit tests using GitHub Models API (gpt-4.1-mini), validates the tests, and produces detailed coverage reports.

**Key Feature**: When generating tests, the tool automatically collects all Rust source files in the same module directory to provide proper context to the AI model. This helps generate more accurate and contextually appropriate tests that understand the module's types, traits, and dependencies.

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

Generates unit tests using AI for the selected file. The tool automatically collects all Rust files in the same module directory to provide context for better test generation:

```bash
export GITHUB_TOKEN=your_token_here
./coverage-tool generate --output-dir /tmp
```

**Features:**
- Collects entire module context (all `.rs` files in the same directory)
- Sends module context to AI to generate more accurate tests
- Uses up to 8000 tokens for comprehensive test generation

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
