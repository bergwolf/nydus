#!/usr/bin/env python3
"""
Analyze cargo llvm-cov coverage report and find the least covered Rust file.
"""

import json
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple


def run_coverage() -> Dict:
    """Run cargo llvm-cov and get JSON coverage data."""
    print("Running coverage analysis...")
    
    # Run cargo llvm-cov with JSON output
    result = subprocess.run(
        ["cargo", "llvm-cov", "--json", "--workspace", 
         "--", "--skip", "integration", "--nocapture", "--test-threads=8"],
        capture_output=True,
        text=True,
        cwd="/home/runner/work/nydus/nydus"
    )
    
    if result.returncode != 0:
        print(f"Error running coverage: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    
    return json.loads(result.stdout)


def extract_file_coverage(coverage_data: Dict) -> List[Tuple[str, float, Dict]]:
    """
    Extract file-level coverage from the JSON data.
    Returns a list of (filepath, coverage_percentage, stats) tuples.
    """
    file_stats = []
    
    # Navigate the coverage data structure
    for file_data in coverage_data.get("data", [{}])[0].get("files", []):
        filename = file_data.get("filename", "")
        
        # Only consider Rust source files in the project
        if not filename.endswith(".rs"):
            continue
        
        # Skip files in target directory and external dependencies
        if "target/" in filename or ".cargo/" in filename:
            continue
        
        # Skip test files (we want to improve coverage of implementation files)
        if "/tests/" in filename or filename.endswith("_test.rs"):
            continue
            
        summary = file_data.get("summary", {})
        lines = summary.get("lines", {})
        
        covered = lines.get("covered", 0)
        total = lines.get("count", 0)
        
        if total > 0:
            coverage_pct = (covered / total) * 100
            stats = {
                "covered": covered,
                "total": total,
                "coverage": coverage_pct,
                "regions": summary.get("regions", {}),
                "functions": summary.get("functions", {})
            }
            file_stats.append((filename, coverage_pct, stats))
    
    return file_stats


def find_least_covered_file(file_stats: List[Tuple[str, float, Dict]]) -> Tuple[str, Dict]:
    """Find the file with the lowest coverage percentage."""
    if not file_stats:
        print("No files found with coverage data", file=sys.stderr)
        sys.exit(1)
    
    # Sort by coverage percentage (ascending)
    sorted_files = sorted(file_stats, key=lambda x: x[1])
    
    least_covered = sorted_files[0]
    filename, coverage_pct, stats = least_covered
    
    return filename, stats


def main():
    print("=" * 80)
    print("Coverage Analysis")
    print("=" * 80)
    
    # Run coverage and get data
    coverage_data = run_coverage()
    
    # Extract file-level coverage
    file_stats = extract_file_coverage(coverage_data)
    
    if not file_stats:
        print("No files found for coverage analysis")
        sys.exit(1)
    
    # Print summary of all files
    print(f"\nFound {len(file_stats)} files with coverage data")
    print("\nTop 10 least covered files:")
    print("-" * 80)
    
    sorted_files = sorted(file_stats, key=lambda x: x[1])
    for i, (filename, coverage_pct, stats) in enumerate(sorted_files[:10], 1):
        print(f"{i:2d}. {coverage_pct:5.2f}% - {filename}")
        print(f"    Lines: {stats['covered']}/{stats['total']}")
    
    # Find the least covered file
    least_covered_file, stats = find_least_covered_file(file_stats)
    
    print("\n" + "=" * 80)
    print("Least covered file selected for improvement:")
    print("=" * 80)
    print(f"File: {least_covered_file}")
    print(f"Coverage: {stats['coverage']:.2f}%")
    print(f"Lines covered: {stats['covered']}/{stats['total']}")
    print(f"Functions: {stats['functions'].get('covered', 0)}/{stats['functions'].get('count', 0)}")
    
    # Output to file for the workflow
    output = {
        "file": least_covered_file,
        "coverage": stats['coverage'],
        "stats": stats
    }
    
    output_path = Path("/tmp/coverage_analysis.json")
    with output_path.open("w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults written to {output_path}")
    
    # Also save overall project coverage
    overall_stats = {
        "total_files": len(file_stats),
        "average_coverage": sum(x[1] for x in file_stats) / len(file_stats)
    }
    
    overall_path = Path("/tmp/overall_coverage.json")
    with overall_path.open("w") as f:
        json.dump(overall_stats, f, indent=2)
    
    print(f"Overall stats written to {overall_path}")


if __name__ == "__main__":
    main()
