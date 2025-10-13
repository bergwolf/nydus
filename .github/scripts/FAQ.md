# Coverage Improvement Workflow - FAQ & Troubleshooting

## Frequently Asked Questions

### Q: How does the workflow select which file to improve?

The workflow selects the Rust source file with the **lowest coverage percentage**. It excludes:
- Test files (files in `/tests/` directories or ending with `_test.rs`)
- External dependencies (files in `target/` or `.cargo/`)
- Non-Rust files

### Q: What happens if the generated tests fail to compile?

The workflow has a built-in validation step that:
1. Attempts to compile the code with the new tests
2. Runs all tests to ensure they pass
3. Retries up to 3 times if validation fails
4. Only creates a PR if tests successfully compile and pass

If all attempts fail, the workflow exits without creating a PR, and artifacts are saved for debugging.

### Q: Can I customize which file gets improved?

The current implementation automatically selects the least covered file. To target a specific file, you would need to:
1. Fork the workflow
2. Modify `analyze_coverage.py` to filter or select specific files
3. Run as a custom workflow

### Q: How much does this workflow cost?

- **GitHub Actions minutes**: Free for public repositories, uses included minutes for private repos
- **GitHub Models API**: Currently free during preview (check GitHub Models pricing for updates)
- **Storage**: Minimal (artifacts are kept for 30 days)

### Q: Can this workflow be run on demand?

Yes! The workflow can be triggered:
- **Automatically**: Weekly on Mondays at 00:00 UTC (configurable via cron)
- **Manually**: Via the "Actions" tab → "Run workflow" button

### Q: What if the workflow creates tests that don't make sense?

The generated tests should be reviewed before merging. The workflow:
- Creates a PR (doesn't auto-merge)
- Includes a detailed coverage report
- Allows for code review and discussion

Reviewers should verify that tests:
- Test meaningful scenarios
- Follow project conventions
- Are maintainable

### Q: Can the workflow improve multiple files at once?

Currently, it processes one file per run. To improve multiple files:
- Run the workflow multiple times
- Modify the workflow to loop through multiple files (advanced)

## Troubleshooting

### Issue: "Coverage analysis failed"

**Symptoms**: Workflow fails at the "Run initial coverage analysis" step

**Possible Causes**:
- Build failures preventing coverage collection
- Insufficient disk space
- Timeout during coverage run

**Solutions**:
1. Check that `make coverage` works locally
2. Review build logs for compilation errors
3. Increase timeout if needed (edit `timeout-minutes` in workflow)

### Issue: "Test generation failed"

**Symptoms**: Workflow fails at "Generate unit tests" step

**Possible Causes**:
- GitHub Models API unavailable or rate-limited
- Network connectivity issues
- Invalid GITHUB_TOKEN

**Solutions**:
1. Check GitHub Models API status
2. Verify GITHUB_TOKEN has required permissions
3. Wait and retry if rate-limited

### Issue: "Test validation failed after all retry attempts"

**Symptoms**: Generated tests don't compile or pass

**Possible Causes**:
- Generated tests have syntax errors
- Tests require complex setup not provided by AI
- Dependencies missing in test environment

**Solutions**:
1. Review workflow artifacts to see generated tests
2. Check compilation errors in logs
3. Consider the file might be too complex for automated test generation
4. Manually write tests for this file

### Issue: "PR creation failed"

**Symptoms**: Tests pass but PR is not created

**Possible Causes**:
- Insufficient permissions for GITHUB_TOKEN
- Base branch doesn't exist
- GitHub API issues

**Solutions**:
1. Verify workflow has `pull-requests: write` permission
2. Check that base branch (default: `master`) exists
3. Review GitHub API status
4. Check workflow logs for detailed error messages

### Issue: Coverage doesn't improve significantly

**Symptoms**: PR shows minimal coverage improvement

**Possible Causes**:
- Generated tests cover already-tested code paths
- Uncovered code is difficult to reach (error handling, edge cases)
- File has mostly boilerplate or declarations

**Solutions**:
- Review and manually enhance generated tests
- Consider the improvement still valuable for regression testing
- Skip files that are not good candidates for automated test generation

## Debugging Tips

### Access Workflow Artifacts

After each run (successful or failed), artifacts are saved:
1. Go to Actions → Coverage Improvement → [specific run]
2. Scroll to "Artifacts" section
3. Download `coverage-improvement-artifacts`

Files included:
- `coverage_analysis.json` - Initial coverage data
- `test_generation_metadata.json` - Generation results
- `coverage_report.md` - Final report (if successful)
- `coverage_stats.json` - Detailed statistics

### Run Scripts Locally

Test the workflow components locally:

```bash
# Install dependencies
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
pip install requests

# Run individual steps
python3 .github/scripts/analyze_coverage.py
# (sets GITHUB_TOKEN first)
export GITHUB_TOKEN=your_token
python3 .github/scripts/generate_tests.py
python3 .github/scripts/validate_tests.py
python3 .github/scripts/generate_report.py
```

### Enable Debug Logging

To get more detailed logs:
1. Edit the workflow file
2. Add at the top of any step:
   ```yaml
   run: |
     set -x  # Enable bash debug mode
     # ... rest of commands
   ```

## Getting Help

If you encounter issues not covered here:

1. **Check Workflow Logs**: Detailed error messages are in the Actions logs
2. **Review Artifacts**: Downloaded artifacts contain intermediate results
3. **Run Integration Test**: Use `.github/scripts/test_workflow.sh` locally
4. **Open an Issue**: Include workflow logs and artifact files

## Best Practices

1. **Review PRs Thoroughly**: Automated tests should be reviewed like any other code
2. **Enhance Generated Tests**: Use AI-generated tests as a starting point
3. **Monitor Success Rate**: Track which types of files work well
4. **Adjust Prompts**: Modify `generate_tests.py` prompts for better results
5. **Combine with Manual Work**: Use this workflow to handle easy cases, write complex tests manually

## Advanced Configuration

### Adjust Test Generation Prompt

Edit `.github/scripts/generate_tests.py` and modify the `prompt` variable to:
- Focus on specific types of tests
- Match your project's testing style
- Add project-specific context

### Change Selection Criteria

Edit `.github/scripts/analyze_coverage.py` to change how files are selected:
- Filter by directory
- Prioritize specific file types
- Exclude certain patterns

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
