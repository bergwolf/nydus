#!/usr/bin/env python3
"""
Generate unit tests for a Rust file using GitHub Models API (gpt-4o-mini).
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional
import requests


def read_file_content(filepath: str) -> str:
    """Read the content of a Rust source file."""
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def call_github_models_api(file_content: str, filepath: str, coverage_stats: Dict) -> str:
    """
    Call GitHub Models API (gpt-4o-mini) to generate unit tests.
    """
    # Get GitHub token from environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        print("Error: GITHUB_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)
    
    # Prepare the prompt for test generation
    prompt = f"""You are an expert Rust developer tasked with writing comprehensive unit tests.

I have a Rust source file that currently has {coverage_stats['coverage']:.2f}% test coverage ({coverage_stats['covered']}/{coverage_stats['total']} lines covered).

File path: {filepath}

Here is the file content:

```rust
{file_content}
```

Please generate comprehensive unit tests for this file following these requirements:

1. Focus on testing the most critical and complex functions that are currently uncovered
2. Write tests that follow Rust best practices and conventions
3. Include tests for:
   - Normal/happy path cases
   - Edge cases and boundary conditions
   - Error handling paths
   - Different input variations
4. Use the existing test module structure if present, or create a new #[cfg(test)] module
5. Make sure tests are self-contained and don't require external dependencies when possible
6. Follow the coding style and patterns already present in the file
7. Add descriptive test names that clearly indicate what is being tested

Please provide ONLY the test code that should be added to the existing #[cfg(test)] mod tests section, or a complete new test module if none exists. Do not include the entire file, just the test code to be added.

Format your response as:
```rust
// Your test code here
```
"""

    # Call GitHub Models API
    api_url = "https://models.inference.ai.azure.com/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {github_token}"
    }
    
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "You are an expert Rust developer who writes high-quality, comprehensive unit tests."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "max_tokens": 4000
    }
    
    print(f"Calling GitHub Models API to generate tests...")
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        
        result = response.json()
        generated_content = result["choices"][0]["message"]["content"]
        
        # Extract code from markdown code blocks if present
        if "```rust" in generated_content:
            # Find the rust code block
            start = generated_content.find("```rust") + 7
            end = generated_content.find("```", start)
            if end > start:
                generated_content = generated_content[start:end].strip()
        
        return generated_content
        
    except requests.exceptions.RequestException as e:
        print(f"Error calling GitHub Models API: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}", file=sys.stderr)
        sys.exit(1)


def integrate_tests_into_file(original_content: str, generated_tests: str) -> str:
    """
    Integrate generated tests into the original file content.
    """
    # Check if file already has a test module
    if "#[cfg(test)]" in original_content:
        # Find the test module
        lines = original_content.split('\n')
        
        # Find the last closing brace of the test module
        in_test_module = False
        test_module_start = -1
        brace_count = 0
        insert_position = -1
        
        for i, line in enumerate(lines):
            if "#[cfg(test)]" in line:
                in_test_module = True
                test_module_start = i
                continue
            
            if in_test_module:
                brace_count += line.count('{')
                brace_count -= line.count('}')
                
                if brace_count == 0 and '}' in line:
                    # Found the end of test module
                    insert_position = i
                    break
        
        if insert_position > 0:
            # Insert before the closing brace
            lines.insert(insert_position, generated_tests)
            lines.insert(insert_position, "")
            return '\n'.join(lines)
    
    # No existing test module, add one at the end
    return original_content + "\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n" + generated_tests + "\n}\n"


def main():
    # Read coverage analysis results
    analysis_path = Path("/tmp/coverage_analysis.json")
    if not analysis_path.exists():
        print("Error: Coverage analysis results not found", file=sys.stderr)
        sys.exit(1)
    
    with analysis_path.open() as f:
        analysis = json.load(f)
    
    filepath = analysis["file"]
    coverage_stats = analysis["stats"]
    
    print("=" * 80)
    print("Generating Unit Tests")
    print("=" * 80)
    print(f"Target file: {filepath}")
    print(f"Current coverage: {coverage_stats['coverage']:.2f}%")
    print()
    
    # Read the file content
    file_content = read_file_content(filepath)
    
    # Generate tests using GitHub Models API
    generated_tests = call_github_models_api(file_content, filepath, coverage_stats)
    
    print("\nGenerated tests:")
    print("-" * 80)
    print(generated_tests)
    print("-" * 80)
    
    # Integrate tests into the file
    updated_content = integrate_tests_into_file(file_content, generated_tests)
    
    # Save the updated file
    output_path = Path("/tmp/updated_file.rs")
    with output_path.open("w") as f:
        f.write(updated_content)
    
    print(f"\nUpdated file saved to {output_path}")
    
    # Save metadata
    metadata = {
        "original_file": filepath,
        "generated_tests_path": str(output_path),
        "coverage_before": coverage_stats['coverage']
    }
    
    metadata_path = Path("/tmp/test_generation_metadata.json")
    with metadata_path.open("w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Metadata saved to {metadata_path}")


if __name__ == "__main__":
    main()
