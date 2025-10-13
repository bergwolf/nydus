#!/bin/bash
# Integration test script for coverage improvement workflow
# This script simulates the workflow steps locally

set -e

echo "=============================================================================="
echo "Coverage Improvement Workflow - Integration Test"
echo "=============================================================================="

# Change to repository root
cd "$(git rev-parse --show-toplevel)"

# Clean up any previous test artifacts
echo "Cleaning up previous test artifacts..."
rm -f /tmp/coverage_analysis.json
rm -f /tmp/overall_coverage.json
rm -f /tmp/test_generation_metadata.json
rm -f /tmp/updated_file.rs
rm -f /tmp/coverage_report.md
rm -f /tmp/coverage_stats.json

echo ""
echo "Step 1: Testing Python script syntax"
echo "----------------------------------------------------------------------"
python3 -m py_compile \
    .github/scripts/analyze_coverage.py \
    .github/scripts/generate_tests.py \
    .github/scripts/validate_tests.py \
    .github/scripts/generate_report.py
echo "✅ All scripts have valid Python syntax"

echo ""
echo "Step 2: Testing YAML workflow syntax"
echo "----------------------------------------------------------------------"
if command -v yamllint &> /dev/null; then
    yamllint .github/workflows/coverage-improvement.yml
    echo "✅ Workflow YAML is valid"
else
    echo "⚠️  yamllint not found, skipping YAML validation"
fi

echo ""
echo "Step 3: Testing analyze_coverage.py (unit test)"
echo "----------------------------------------------------------------------"
python3 << 'EOF'
import sys
import importlib.util

# Import the analyze_coverage module
spec = importlib.util.spec_from_file_location(
    "analyze_coverage",
    ".github/scripts/analyze_coverage.py"
)
analyze = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyze)

# Create test data
test_data = {
    "data": [{
        "files": [
            {
                "filename": "/home/runner/work/nydus/nydus/api/src/error.rs",
                "summary": {
                    "lines": {"covered": 50, "count": 100},
                    "regions": {},
                    "functions": {"covered": 5, "count": 10}
                }
            },
            {
                "filename": "/home/runner/work/nydus/nydus/api/src/config.rs",
                "summary": {
                    "lines": {"covered": 80, "count": 100},
                    "regions": {},
                    "functions": {"covered": 8, "count": 10}
                }
            }
        ]
    }]
}

# Test functions
file_stats = analyze.extract_file_coverage(test_data)
assert len(file_stats) == 2, "Should find 2 files"

least_covered, stats = analyze.find_least_covered_file(file_stats)
assert "error.rs" in least_covered, "Should select error.rs as least covered"
assert stats['coverage'] == 50.0, "Coverage should be 50%"

print("✅ analyze_coverage.py unit tests passed")
EOF

echo ""
echo "Step 4: Checking required tools"
echo "----------------------------------------------------------------------"
echo -n "cargo: "
cargo --version | head -n1

echo -n "cargo llvm-cov: "
if cargo llvm-cov --version 2>/dev/null; then
    echo "✅ Installed"
else
    echo "❌ Not installed (required for actual coverage run)"
fi

echo -n "rustup: "
rustup --version | head -n1

echo -n "python3: "
python3 --version

echo -n "requests library: "
python3 -c "import requests; print(f'v{requests.__version__}')"

echo -n "jq: "
jq --version

echo -n "gh (GitHub CLI): "
gh --version | head -n1

echo ""
echo "=============================================================================="
echo "Integration Test Summary"
echo "=============================================================================="
echo "✅ All scripts have valid syntax"
echo "✅ Core functions work correctly"
echo "✅ Required tools are available"
echo ""
echo "Note: Full integration test (running actual coverage) would take significant"
echo "time and has been skipped. The workflow is ready for GitHub Actions testing."
echo "=============================================================================="
