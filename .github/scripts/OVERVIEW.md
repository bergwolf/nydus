# Coverage Improvement Workflow - Visual Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COVERAGE IMPROVEMENT WORKFLOW                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ TRIGGER                                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Weekly: Mondays at 00:00 UTC                                              │
│ • Manual: Via GitHub Actions UI                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: ANALYZE CURRENT COVERAGE                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Script: analyze_coverage.py                                                 │
│ Action: Run `cargo llvm-cov --json` to get coverage data                   │
│                                                                              │
│ Process:                                                                     │
│ 1. Execute coverage analysis on all Rust files                             │
│ 2. Parse JSON output to extract file-level coverage                        │
│ 3. Filter out test files, external deps, and target directory              │
│ 4. Sort files by coverage percentage (ascending)                           │
│ 5. Select the file with lowest coverage                                    │
│                                                                              │
│ Output:                                                                      │
│ • /tmp/coverage_analysis.json       (selected file details)                │
│ • /tmp/overall_coverage.json        (project statistics)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: GENERATE UNIT TESTS                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Script: generate_tests.py                                                   │
│ Action: Call GitHub Models API (gpt-4o-mini) to generate tests             │
│                                                                              │
│ Process:                                                                     │
│ 1. Read the selected file content                                          │
│ 2. Prepare detailed prompt with:                                           │
│    - Current coverage statistics                                           │
│    - File source code                                                       │
│    - Testing requirements and best practices                               │
│ 3. Call GitHub Models API with prompt                                      │
│ 4. Extract generated test code from response                               │
│ 5. Integrate tests into file (existing test module or new)                 │
│                                                                              │
│ Output:                                                                      │
│ • /tmp/updated_file.rs              (file with new tests)                  │
│ • /tmp/test_generation_metadata.json (generation metadata)                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: VALIDATE TESTS (Up to 3 attempts)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Script: validate_tests.py                                                   │
│ Action: Verify tests compile and pass                                      │
│                                                                              │
│ Process (per attempt):                                                      │
│ 1. Backup original file                                                     │
│ 2. Apply generated tests to original file                                  │
│ 3. Run `cargo check --workspace` (compilation check)                       │
│ 4. Run `cargo test --workspace` (test execution)                           │
│ 5. If success: Keep changes, exit loop                                     │
│    If failure: Restore backup, retry                                       │
│                                                                              │
│ Output:                                                                      │
│ • Updated test_generation_metadata.json (validation status)                │
│ • Modified original file (if successful)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                        ┌───────────┴───────────┐
                        │                       │
                    SUCCESS                 FAILURE
                        │                       │
                        ▼                       ▼
                ┌───────────────┐      ┌──────────────────┐
                │   Continue    │      │  Exit workflow   │
                └───────────────┘      │  Save artifacts  │
                        │              └──────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: MEASURE IMPROVEMENT                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Script: generate_report.py                                                  │
│ Action: Re-run coverage and generate report                                │
│                                                                              │
│ Process:                                                                     │
│ 1. Run `cargo llvm-cov --json` again with new tests                        │
│ 2. Extract coverage for the modified file                                  │
│ 3. Calculate overall project coverage                                      │
│ 4. Compare before/after metrics                                            │
│ 5. Generate markdown report with:                                          │
│    - File coverage improvement                                             │
│    - Lines covered delta                                                   │
│    - Overall project impact                                                │
│    - Validation details                                                    │
│                                                                              │
│ Output:                                                                      │
│ • /tmp/coverage_report.md          (PR description)                        │
│ • /tmp/coverage_stats.json         (detailed statistics)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: COMMIT CHANGES                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Action: Commit modified file with descriptive message                      │
│                                                                              │
│ Commit Message Format:                                                      │
│ test: add unit tests for <filename> to improve coverage                    │
│                                                                              │
│ Automatically generated unit tests to improve code coverage.               │
│ Generated by: Coverage Improvement workflow                                │
│ Target file: <full_path>                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 6: CREATE PULL REQUEST                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ Action: Open PR with coverage improvement report                           │
│                                                                              │
│ PR Details:                                                                 │
│ • Title: "test: improve coverage for <filename>"                           │
│ • Body: Full coverage improvement report (markdown)                        │
│ • Base: master                                                             │
│ • Labels: "automated", "testing"                                           │
│                                                                              │
│ Report Includes:                                                            │
│ • Summary of changes                                                       │
│ • Before/after coverage metrics                                            │
│ • File-specific improvements                                               │
│ • Overall project impact                                                   │
│ • Review guidelines                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ ARTIFACTS (Saved for 30 days)                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ • coverage_analysis.json           - Initial coverage data                 │
│ • test_generation_metadata.json    - Generation and validation results     │
│ • coverage_report.md               - Final report                          │
│ • coverage_stats.json              - Detailed statistics                   │
└─────────────────────────────────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════════════════
                              KEY FEATURES
═══════════════════════════════════════════════════════════════════════════════

✓ Automatic file selection (least covered)
✓ AI-powered test generation (GitHub Models API)
✓ Validation with retry logic (up to 3 attempts)
✓ Comprehensive metrics and reporting
✓ Safe operation (no auto-merge)
✓ Full audit trail via artifacts
✓ Configurable and extensible

═══════════════════════════════════════════════════════════════════════════════
                            REQUIRED PERMISSIONS
═══════════════════════════════════════════════════════════════════════════════

• contents: write          (for creating branches and commits)
• pull-requests: write     (for creating PRs)
• GITHUB_TOKEN            (automatically provided by GitHub Actions)

═══════════════════════════════════════════════════════════════════════════════
                              ERROR HANDLING
═══════════════════════════════════════════════════════════════════════════════

Coverage Analysis Fails    → Workflow exits, logs available
Test Generation Fails      → Workflow exits, artifacts saved
Validation Fails (3x)      → Workflow exits, no PR created
PR Creation Fails          → Commits remain, manual PR possible

═══════════════════════════════════════════════════════════════════════════════
```

## Quick Start

### Run the Workflow Manually

1. Navigate to: **Actions** → **Coverage Improvement**
2. Click **Run workflow**
3. Select branch (usually `master`)
4. Click **Run workflow** button
5. Monitor progress in the workflow run page

### Review the Pull Request

1. Wait for workflow completion (~10-30 minutes)
2. Check for new PR with title: "test: improve coverage for [filename]"
3. Review the coverage report in PR description
4. Examine the generated tests
5. Approve and merge if tests are satisfactory

### Troubleshoot Issues

1. Check workflow logs for error messages
2. Download artifacts for detailed information
3. Run `.github/scripts/test_workflow.sh` locally
4. Refer to `.github/scripts/FAQ.md` for common issues

## Files Overview

| File | Purpose | Size |
|------|---------|------|
| `coverage-improvement.yml` | Main workflow definition | ~5KB |
| `analyze_coverage.py` | Coverage analysis logic | ~5KB |
| `generate_tests.py` | AI test generation | ~7KB |
| `validate_tests.py` | Test validation | ~5KB |
| `generate_report.py` | Report generation | ~8KB |
| `README.md` | Complete documentation | ~4KB |
| `FAQ.md` | Troubleshooting guide | ~7KB |
| `test_workflow.sh` | Integration test | ~4KB |

## Next Steps

- ✅ Workflow is ready to use
- ✅ All scripts tested and validated
- ✅ Documentation complete
- 🔜 Monitor first automated run
- 🔜 Review generated test quality
- 🔜 Adjust prompts based on results
