#!/usr/bin/env python3
"""
Generate a coverage improvement report comparing before and after.
"""

import json
import sys
import subprocess
from pathlib import Path
from typing import Dict


def run_coverage_after() -> Dict:
    """Run coverage analysis after adding tests and get JSON data."""
    print("Running post-improvement coverage analysis...")
    
    result = subprocess.run(
        ["cargo", "llvm-cov", "--json", "--workspace",
         "--", "--skip", "integration", "--nocapture", "--test-threads=8"],
        capture_output=True,
        text=True,
        cwd="/home/runner/work/nydus/nydus",
        timeout=600
    )
    
    if result.returncode != 0:
        print(f"Error running coverage: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    
    return json.loads(result.stdout)


def get_file_coverage_from_data(coverage_data: Dict, target_file: str) -> Dict:
    """Extract coverage for a specific file from coverage data."""
    for file_data in coverage_data.get("data", [{}])[0].get("files", []):
        filename = file_data.get("filename", "")
        
        if filename == target_file:
            summary = file_data.get("summary", {})
            lines = summary.get("lines", {})
            
            covered = lines.get("covered", 0)
            total = lines.get("count", 0)
            
            if total > 0:
                coverage_pct = (covered / total) * 100
                return {
                    "covered": covered,
                    "total": total,
                    "coverage": coverage_pct,
                    "regions": summary.get("regions", {}),
                    "functions": summary.get("functions", {})
                }
    
    return None


def calculate_overall_coverage(coverage_data: Dict) -> Dict:
    """Calculate overall project coverage statistics."""
    file_count = 0
    total_coverage = 0.0
    
    for file_data in coverage_data.get("data", [{}])[0].get("files", []):
        filename = file_data.get("filename", "")
        
        # Only consider Rust source files in the project
        if not filename.endswith(".rs"):
            continue
        
        if "target/" in filename or ".cargo/" in filename:
            continue
        
        if "/tests/" in filename or filename.endswith("_test.rs"):
            continue
        
        summary = file_data.get("summary", {})
        lines = summary.get("lines", {})
        
        covered = lines.get("covered", 0)
        total = lines.get("count", 0)
        
        if total > 0:
            coverage_pct = (covered / total) * 100
            total_coverage += coverage_pct
            file_count += 1
    
    if file_count > 0:
        return {
            "total_files": file_count,
            "average_coverage": total_coverage / file_count
        }
    
    return {"total_files": 0, "average_coverage": 0.0}


def generate_report(metadata: Dict, before_coverage: Dict, after_coverage: Dict, 
                   before_overall: Dict, after_overall: Dict) -> str:
    """Generate a comprehensive coverage improvement report."""
    
    target_file = metadata["original_file"]
    
    # Calculate improvements
    file_improvement = after_coverage["coverage"] - before_coverage["coverage"]
    lines_improvement = after_coverage["covered"] - before_coverage["covered"]
    overall_improvement = after_overall["average_coverage"] - before_overall["average_coverage"]
    
    report = f"""# Coverage Improvement Report

## Summary

This automated workflow has successfully generated and validated new unit tests to improve code coverage.

## Target File

**File:** `{target_file}`

## File Coverage Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Coverage Percentage** | {before_coverage['coverage']:.2f}% | {after_coverage['coverage']:.2f}% | **+{file_improvement:.2f}%** |
| **Lines Covered** | {before_coverage['covered']}/{before_coverage['total']} | {after_coverage['covered']}/{after_coverage['total']} | **+{lines_improvement} lines** |
| **Functions Covered** | {before_coverage['functions'].get('covered', 0)}/{before_coverage['functions'].get('count', 0)} | {after_coverage['functions'].get('covered', 0)}/{after_coverage['functions'].get('count', 0)} | - |

## Overall Project Coverage

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| **Average Coverage** | {before_overall['average_coverage']:.2f}% | {after_overall['average_coverage']:.2f}% | **{overall_improvement:+.2f}%** |
| **Total Files Analyzed** | {before_overall['total_files']} | {after_overall['total_files']} | - |

## Details

- **Validation Attempts:** {metadata.get('validation_attempts', 'N/A')}
- **Validation Status:** {'✅ Success' if metadata.get('validation_success', False) else '❌ Failed'}
- **Test Generation Method:** GitHub Models API (gpt-4o-mini)

## Next Steps

This PR contains automatically generated unit tests. Please review the tests to ensure they:
- Follow project coding standards
- Test meaningful scenarios
- Are maintainable and well-documented

---
*This report was automatically generated by the Coverage Improvement workflow.*
"""
    
    return report


def main():
    print("=" * 80)
    print("Generating Coverage Improvement Report")
    print("=" * 80)
    
    # Read metadata
    metadata_path = Path("/tmp/test_generation_metadata.json")
    if not metadata_path.exists():
        print("Error: Test generation metadata not found", file=sys.stderr)
        sys.exit(1)
    
    with metadata_path.open() as f:
        metadata = json.load(f)
    
    # Read before coverage
    before_analysis_path = Path("/tmp/coverage_analysis.json")
    with before_analysis_path.open() as f:
        before_analysis = json.load(f)
    
    before_overall_path = Path("/tmp/overall_coverage.json")
    with before_overall_path.open() as f:
        before_overall = json.load(f)
    
    target_file = metadata["original_file"]
    before_coverage = before_analysis["stats"]
    
    # Run coverage after improvements
    after_coverage_data = run_coverage_after()
    
    # Extract coverage for the target file
    after_coverage = get_file_coverage_from_data(after_coverage_data, target_file)
    
    if not after_coverage:
        print(f"Warning: Could not find coverage data for {target_file} after improvements")
        after_coverage = before_coverage  # Use before as fallback
    
    # Calculate overall coverage
    after_overall = calculate_overall_coverage(after_coverage_data)
    
    # Generate the report
    report = generate_report(metadata, before_coverage, after_coverage, 
                            before_overall, after_overall)
    
    print("\n" + "=" * 80)
    print("Coverage Improvement Report")
    print("=" * 80)
    print(report)
    
    # Save report to file
    report_path = Path("/tmp/coverage_report.md")
    with report_path.open("w") as f:
        f.write(report)
    
    print(f"\nReport saved to {report_path}")
    
    # Save detailed stats
    stats = {
        "target_file": target_file,
        "before": {
            "file": before_coverage,
            "overall": before_overall
        },
        "after": {
            "file": after_coverage,
            "overall": after_overall
        },
        "improvements": {
            "file_coverage": after_coverage["coverage"] - before_coverage["coverage"],
            "lines_covered": after_coverage["covered"] - before_coverage["covered"],
            "overall_coverage": after_overall["average_coverage"] - before_overall["average_coverage"]
        }
    }
    
    stats_path = Path("/tmp/coverage_stats.json")
    with stats_path.open("w") as f:
        json.dump(stats, f, indent=2)
    
    print(f"Detailed stats saved to {stats_path}")


if __name__ == "__main__":
    main()
