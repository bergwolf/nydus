# Coverage Improvement Automation Scripts

This directory contains scripts used by the automated coverage improvement GitHub Action workflow.

## Overview

The coverage improvement workflow automatically:
1. Analyzes current test coverage across all Rust files
2. Identifies the file with the lowest coverage
3. Uses GitHub Models API (gpt-4o-mini) to generate new unit tests
4. Validates that the generated tests compile and pass
5. Re-runs coverage analysis to measure improvement
6. Generates a detailed report
7. Creates a pull request with the changes

## Scripts

### analyze_coverage.py

Runs `cargo llvm-cov` and analyzes the coverage data to find the least covered Rust file.

**Output:**
- `/tmp/coverage_analysis.json` - Details about the selected file
- `/tmp/overall_coverage.json` - Overall project coverage statistics

### generate_tests.py

Calls GitHub Models API (gpt-4o-mini) to generate comprehensive unit tests for the target file.

**Requirements:**
- `GITHUB_TOKEN` environment variable must be set

**Output:**
- `/tmp/updated_file.rs` - The file with new tests added
- `/tmp/test_generation_metadata.json` - Metadata about the generation process

### validate_tests.py

Validates that the generated tests compile and pass. Automatically retries up to 3 times if validation fails.

**Process:**
1. Creates a backup of the original file
2. Applies the generated tests
3. Runs `cargo check` to verify compilation
4. Runs `cargo test` to verify tests pass
5. Restores backup if validation fails

**Output:**
- Updates `/tmp/test_generation_metadata.json` with validation results

### generate_report.py

Generates a comprehensive coverage improvement report comparing before and after metrics.

**Output:**
- `/tmp/coverage_report.md` - Markdown report for the PR description
- `/tmp/coverage_stats.json` - Detailed statistics in JSON format

## Workflow Configuration

The workflow is defined in `.github/workflows/coverage-improvement.yml`.

### Trigger

- **Scheduled:** Runs weekly on Mondays at 00:00 UTC
- **Manual:** Can be triggered via workflow_dispatch

### Requirements

- GitHub token with permissions for:
  - Creating branches
  - Pushing commits
  - Creating pull requests
- Access to GitHub Models API (included with GitHub token)

### Permissions

```yaml
permissions:
  contents: write
  pull-requests: write
```

## Usage

### Running the Workflow Manually

1. Go to the Actions tab in the GitHub repository
2. Select "Coverage Improvement" workflow
3. Click "Run workflow"
4. Select the branch (usually `master`)
5. Click "Run workflow"

### Local Testing

You can test individual scripts locally:

```bash
# Install dependencies
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
pip install requests

# Run coverage analysis
python3 .github/scripts/analyze_coverage.py

# Generate tests (requires GITHUB_TOKEN)
export GITHUB_TOKEN=your_token_here
python3 .github/scripts/generate_tests.py

# Validate tests
python3 .github/scripts/validate_tests.py

# Generate report
python3 .github/scripts/generate_report.py
```

## Limitations

- The workflow processes one file per run
- Test generation quality depends on the AI model's understanding of the code
- Complex files with many dependencies may produce tests that fail validation
- The workflow has a 120-minute timeout

## Troubleshooting

### Tests Fail Validation

If generated tests fail validation after 3 attempts, the workflow will fail and no PR will be created. Check the workflow logs for details.

### Coverage Not Improving

Sometimes the generated tests may not significantly improve coverage. This can happen if:
- The uncovered code is hard to reach (e.g., error handling paths)
- The code requires complex setup or mocking
- The AI model misunderstands the code structure

### API Rate Limits

GitHub Models API has rate limits. If you encounter rate limit errors, wait and try again later.

## Future Improvements

Potential enhancements:
- Support for processing multiple files in one run
- Iterative test generation with feedback loop
- Integration with code review tools
- Custom prompts for different file types
- Support for integration tests in addition to unit tests

## Contributing

To improve these scripts:
1. Test changes locally first
2. Update this README if adding new scripts or changing behavior
3. Follow Python best practices and add error handling
4. Ensure scripts work in the GitHub Actions environment
