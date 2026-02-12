#!/bin/bash
#
# Cleanup script for profraw files during coverage collection
# This script merges profraw files into profdata format and removes processed files
# to prevent disk space exhaustion during long-running test suites.
#
# Usage: cleanup-profraw.sh [coverage_dir] [profdata_output]
#

set -e

COVERAGE_DIR="${1:-coverage}"
PROFDATA_OUTPUT="${2:-${COVERAGE_DIR}/merged.profdata}"
LLVM_PROFDATA="${LLVM_PROFDATA:-llvm-profdata}"

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Create coverage directory if it doesn't exist
mkdir -p "${COVERAGE_DIR}"

# Check if llvm-profdata is available
if ! command -v "${LLVM_PROFDATA}" &> /dev/null; then
    log_error "llvm-profdata not found. Please install llvm-tools-preview:"
    log_error "  rustup component add llvm-tools-preview"
    exit 1
fi

# Find all profraw files
PROFRAW_FILES=($(find "${COVERAGE_DIR}" -name "*.profraw" -type f 2>/dev/null || true))

if [ ${#PROFRAW_FILES[@]} -eq 0 ]; then
    log_info "No profraw files found in ${COVERAGE_DIR}"
    exit 0
fi

log_info "Found ${#PROFRAW_FILES[@]} profraw file(s) in ${COVERAGE_DIR}"

# Calculate total size before processing
TOTAL_SIZE_BEFORE=0
for file in "${PROFRAW_FILES[@]}"; do
    if [ -f "$file" ]; then
        SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
        TOTAL_SIZE_BEFORE=$((TOTAL_SIZE_BEFORE + SIZE))
    fi
done

log_info "Total profraw size: $(numfmt --to=iec-i --suffix=B $TOTAL_SIZE_BEFORE 2>/dev/null || echo ${TOTAL_SIZE_BEFORE} bytes)"

# Merge profraw files into profdata
if [ -f "${PROFDATA_OUTPUT}" ]; then
    log_info "Merging with existing profdata file: ${PROFDATA_OUTPUT}"
    # Create temporary file for new merge
    TEMP_PROFDATA="${PROFDATA_OUTPUT}.tmp"
    
    if "${LLVM_PROFDATA}" merge -sparse "${PROFDATA_OUTPUT}" "${PROFRAW_FILES[@]}" -o "${TEMP_PROFDATA}" 2>&1; then
        mv "${TEMP_PROFDATA}" "${PROFDATA_OUTPUT}"
        log_info "Successfully merged ${#PROFRAW_FILES[@]} profraw files into existing profdata"
    else
        log_error "Failed to merge profraw files with existing profdata"
        rm -f "${TEMP_PROFDATA}"
        exit 1
    fi
else
    log_info "Creating new profdata file: ${PROFDATA_OUTPUT}"
    if "${LLVM_PROFDATA}" merge -sparse "${PROFRAW_FILES[@]}" -o "${PROFDATA_OUTPUT}" 2>&1; then
        log_info "Successfully created profdata from ${#PROFRAW_FILES[@]} profraw files"
    else
        log_error "Failed to create profdata from profraw files"
        exit 1
    fi
fi

# Remove processed profraw files
REMOVED_COUNT=0
SPACE_FREED=0
for file in "${PROFRAW_FILES[@]}"; do
    if [ -f "$file" ]; then
        SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
        if rm "$file" 2>/dev/null; then
            REMOVED_COUNT=$((REMOVED_COUNT + 1))
            SPACE_FREED=$((SPACE_FREED + SIZE))
        else
            log_warn "Failed to remove $file"
        fi
    fi
done

log_info "Removed ${REMOVED_COUNT} profraw file(s)"
log_info "Space freed: $(numfmt --to=iec-i --suffix=B $SPACE_FREED 2>/dev/null || echo ${SPACE_FREED} bytes)"

# Show final profdata size
if [ -f "${PROFDATA_OUTPUT}" ]; then
    PROFDATA_SIZE=$(stat -f%z "${PROFDATA_OUTPUT}" 2>/dev/null || stat -c%s "${PROFDATA_OUTPUT}" 2>/dev/null || echo 0)
    log_info "Current profdata size: $(numfmt --to=iec-i --suffix=B $PROFDATA_SIZE 2>/dev/null || echo ${PROFDATA_SIZE} bytes)"
    
    if [ $TOTAL_SIZE_BEFORE -gt 0 ]; then
        REDUCTION_PERCENT=$(( (SPACE_FREED * 100) / TOTAL_SIZE_BEFORE ))
        log_info "Space reduction: ${REDUCTION_PERCENT}%"
    fi
fi

log_info "Cleanup completed successfully"
