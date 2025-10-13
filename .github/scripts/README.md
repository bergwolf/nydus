# Coverage Improvement Automation

This directory contains documentation for the automated coverage improvement GitHub Action workflow.

## Overview

The coverage improvement workflow automatically:
1. Analyzes current test coverage across all Rust files
2. Identifies the file with the lowest coverage
3. Uses GitHub Models API (gpt-4o-mini) to generate new unit tests
4. Validates that the generated tests compile and pass
5. Re-runs coverage analysis to measure improvement
6. Generates a detailed report
7. Creates a pull request with the changes

## Tool Implementation

The core functionality is implemented in Go and located in `contrib/nydus-coverage-tool`.

### Building the Tool

```bash
cd contrib/nydus-coverage-tool
make build
```

### Using the Tool

The coverage tool provides four subcommands:

```bash
# Analyze current coverage
./contrib/nydus-coverage-tool/cmd/coverage-tool analyze --output-dir /tmp

# Generate tests using AI
./contrib/nydus-coverage-tool/cmd/coverage-tool generate --output-dir /tmp

# Validate generated tests
./contrib/nydus-coverage-tool/cmd/coverage-tool validate --output-dir /tmp --max-retries 3

# Generate coverage report
./contrib/nydus-coverage-tool/cmd/coverage-tool report --output-dir /tmp
```

## Workflow Configuration

The workflow is defined in `.github/workflows/coverage-improvement.yml`.

### Trigger

- **Scheduled:** Runs weekly on Mondays at 00:00 UTC
- **Manual:** Can be triggered via workflow_dispatch with optional parameters

### Workflow Inputs

When manually triggering the workflow, you can customize:

- `git_user_name`: Git user name for commits (default: "github-actions[bot]")
- `git_user_email`: Git user email for commits (default: "github-actions[bot]@users.noreply.github.com")
- `pr_target_repo`: Target repository for PR in owner/repo format (default: current repository)
- `create_pr`: Whether to create a pull request (default: true)

### Permissions

```yaml
permissions:
  contents: write
  pull-requests: write
  models: read
```

## Usage

### Running the Workflow Manually

1. Go to the Actions tab in the GitHub repository
2. Select "Coverage Improvement" workflow
3. Click "Run workflow"
4. (Optional) Configure workflow inputs
5. Click "Run workflow"

### Local Testing

You can test the tool locally:

```bash
# Install dependencies
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview

# Build the tool
cd contrib/nydus-coverage-tool
make build

# Run individual steps (requires GITHUB_TOKEN for test generation)
export GITHUB_TOKEN=your_token_here
./cmd/coverage-tool analyze --output-dir /tmp
./cmd/coverage-tool generate --output-dir /tmp
./cmd/coverage-tool validate --output-dir /tmp --max-retries 3
./cmd/coverage-tool report --output-dir /tmp
```

## Output Files

The tool generates these files in the output directory (default: /tmp):

- `coverage_analysis.json` - Initial coverage data and selected file
- `overall_coverage.json` - Overall project coverage statistics
- `updated_file.rs` - The file with new tests added
- `test_generation_metadata.json` - Metadata about generation and validation
- `coverage_report.md` - Final coverage improvement report

## Limitations

- The workflow processes one file per run
- Test generation quality depends on the AI model's understanding of the code
- Complex files with many dependencies may produce tests that fail validation
- The workflow has a 120-minute timeout

## Best Practices

1. **Review PRs Thoroughly**: Automated tests should be reviewed like any other code
2. **Enhance Generated Tests**: Use AI-generated tests as a starting point
3. **Monitor Success Rate**: Track which types of files work well
4. **Combine with Manual Work**: Use this workflow to handle easy cases, write complex tests manually

## Advanced Configuration

### Change Selection Criteria

Edit the `extractFileCoverage` function in `contrib/nydus-coverage-tool/cmd/coverage-tool/main.go` to change how files are selected.

### Customize PR Labels

Edit `.github/workflows/coverage-improvement.yml`:
```yaml
--label "your-custom-label" \
--label "another-label"
```

### Change Schedule

Edit the cron expression in the workflow:
```yaml
schedule:
  - cron: '0 0 * * 1'  # Weekly on Monday
  # Examples:
  # '0 */6 * * *'      # Every 6 hours
  # '0 0 1 * *'        # Monthly on the 1st
```
