#!/usr/bin/env python3
"""
Validate that generated tests compile and pass, then apply them to the original file.
"""

import json
import sys
import subprocess
import shutil
from pathlib import Path
from typing import Dict


def validate_tests(test_file_path: str, original_file_path: str, max_retries: int = 3) -> bool:
    """
    Validate that the tests compile and pass.
    Returns True if successful, False otherwise.
    """
    print("=" * 80)
    print("Validating Generated Tests")
    print("=" * 80)
    
    # Backup the original file
    backup_path = Path(original_file_path + ".backup")
    shutil.copy2(original_file_path, backup_path)
    
    try:
        # Copy the generated file to the original location
        shutil.copy2(test_file_path, original_file_path)
        
        print(f"Testing file: {original_file_path}")
        print("\nRunning cargo check...")
        
        # First, check if it compiles
        check_result = subprocess.run(
            ["cargo", "check", "--workspace"],
            capture_output=True,
            text=True,
            cwd="/home/runner/work/nydus/nydus",
            timeout=300
        )
        
        if check_result.returncode != 0:
            print("❌ Compilation failed!")
            print(check_result.stderr)
            return False
        
        print("✅ Compilation successful!")
        
        print("\nRunning tests...")
        
        # Run the tests
        test_result = subprocess.run(
            ["cargo", "test", "--workspace", "--", "--skip", "integration", "--nocapture"],
            capture_output=True,
            text=True,
            cwd="/home/runner/work/nydus/nydus",
            timeout=600
        )
        
        if test_result.returncode != 0:
            print("❌ Tests failed!")
            print(test_result.stderr)
            print(test_result.stdout)
            return False
        
        print("✅ All tests passed!")
        return True
        
    except subprocess.TimeoutExpired:
        print("❌ Test execution timed out!")
        return False
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False
    finally:
        # Always restore the backup if validation failed
        if not validate_tests.success:
            shutil.copy2(backup_path, original_file_path)
            print(f"Restored original file from backup")
        
        # Clean up backup
        if backup_path.exists():
            backup_path.unlink()


# Use a class attribute to track success
validate_tests.success = False


def apply_tests(test_file_path: str, original_file_path: str) -> None:
    """Apply the validated tests to the original file."""
    print("\n" + "=" * 80)
    print("Applying Generated Tests")
    print("=" * 80)
    
    shutil.copy2(test_file_path, original_file_path)
    print(f"✅ Tests applied to {original_file_path}")


def main():
    # Read metadata
    metadata_path = Path("/tmp/test_generation_metadata.json")
    if not metadata_path.exists():
        print("Error: Test generation metadata not found", file=sys.stderr)
        sys.exit(1)
    
    with metadata_path.open() as f:
        metadata = json.load(f)
    
    test_file_path = metadata["generated_tests_path"]
    original_file_path = metadata["original_file"]
    
    # Validate the tests
    for attempt in range(1, 4):  # Try up to 3 times
        print(f"\n{'='*80}")
        print(f"Validation Attempt {attempt}/3")
        print(f"{'='*80}")
        
        validate_tests.success = validate_tests(test_file_path, original_file_path)
        
        if validate_tests.success:
            # Apply the tests permanently
            apply_tests(test_file_path, original_file_path)
            
            # Update metadata with success
            metadata["validation_success"] = True
            metadata["validation_attempts"] = attempt
            
            with metadata_path.open("w") as f:
                json.dump(metadata, f, indent=2)
            
            print("\n" + "=" * 80)
            print("✅ SUCCESS: Tests validated and applied!")
            print("=" * 80)
            sys.exit(0)
        else:
            print(f"\n❌ Validation attempt {attempt} failed")
            
            if attempt < 3:
                print("Regenerating tests with different parameters...")
                # In a real implementation, we would regenerate with adjusted parameters
                # For now, we'll just report failure
                print("Note: Automatic regeneration not implemented in this version")
    
    # If we get here, all attempts failed
    print("\n" + "=" * 80)
    print("❌ FAILURE: All validation attempts failed")
    print("=" * 80)
    
    metadata["validation_success"] = False
    metadata["validation_attempts"] = 3
    
    with metadata_path.open("w") as f:
        json.dump(metadata, f, indent=2)
    
    sys.exit(1)


if __name__ == "__main__":
    main()
