#!/usr/bin/env python3
"""
PMFW Code Explorer - CTags Compatibility Check

Verifies that Universal CTags outputs expected kind values.
This prevents silent failures when different ctags versions/builds use different kind names.
"""

import subprocess
import json
import os
import tempfile
from typing import Set


# Expected ctags kind values that our type_map in ctags_parser.py knows about
EXPECTED_KINDS = {
    'function',
    'prototype',
    'variable',
    'struct',
    'union',
    'enum',
    'enumerator',
    'typedef',
    'macro',
    'member',
    'header',
}

# Core kinds that MUST appear in our test code
# (We won't see all kinds in a small test, but these should always appear)
CORE_KINDS = {
    'function',
    'prototype',
    'macro',
    'typedef',
    'member',
    'variable',
    'enumerator',
}


def verify_ctags_compatibility(ctags_bin: str = "ctags") -> None:
    """
    Verify ctags outputs expected kind values by parsing a minimal test file.

    This startup check ensures that:
    1. ctags is installed and working
    2. Kind values match what our type_map expects
    3. We fail early with a clear error vs silent data corruption

    Args:
        ctags_bin: Path to ctags binary (default: "ctags")

    Raises:
        RuntimeError: If ctags is incompatible or missing expected kinds
    """
    # Get ctags version for error messages
    try:
        version_result = subprocess.run(
            [ctags_bin, "--version"],
            capture_output=True,
            check=True,
            text=True
        )
        version_info = version_result.stdout.strip().split('\n')[0]
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        raise RuntimeError(f"ctags not found or failed to run: {e}")

    # Minimal test C code with all expected symbol types
    test_code = """
#define TEST_MACRO 1
typedef struct { int member_x; } test_struct_t;
typedef union { int u_val; } test_union_t;
typedef enum { ENUM_VAL = 0 } test_enum_t;
void test_func(void);
void test_func(void) {}
static int test_static_var = 0;
int test_global_var;
"""

    # Write test code to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False) as f:
        test_file = f.name
        f.write(test_code)

    try:
        # Run ctags with same options as parser
        cmd = [
            ctags_bin,
            "--output-format=json",
            "--fields=+nKSz",
            "--c-kinds=+p",  # Include function prototypes
            "-f", "-",
            test_file
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Extract unique kind values from ctags output
        observed_kinds = set()
        for line in result.stdout.strip().split('\n'):
            if not line or line.startswith('!'):
                continue
            try:
                tag = json.loads(line)
                kind = tag.get('kind')
                if kind:
                    observed_kinds.add(kind)
            except json.JSONDecodeError:
                continue

        # Check 1: Did we see the core kinds we expect?
        missing_core = CORE_KINDS - observed_kinds
        if missing_core:
            raise RuntimeError(
                f"\n"
                f"╔══════════════════════════════════════════════════════════════════╗\n"
                f"║ CTAGS COMPATIBILITY CHECK FAILED                                 ║\n"
                f"╚══════════════════════════════════════════════════════════════════╝\n"
                f"\n"
                f"ctags version: {version_info}\n"
                f"\n"
                f"Missing expected core kinds: {missing_core}\n"
                f"Expected kinds: {EXPECTED_KINDS}\n"
                f"Observed kinds: {observed_kinds}\n"
                f"\n"
                f"Your ctags installation may be incompatible.\n"
                f"Please install Universal CTags:\n"
                f"  Ubuntu/Debian: sudo apt install universal-ctags\n"
                f"  macOS:         brew install universal-ctags\n"
            )

        # Check 2: Did we see unexpected kinds not in our type_map?
        unexpected_kinds = observed_kinds - EXPECTED_KINDS
        if unexpected_kinds:
            # This is a warning, not an error - we handle unknown kinds gracefully
            print(f"⚠️  Warning: ctags returned unexpected kinds: {unexpected_kinds}")
            print(f"   These will be stored as-is in the database.")
            print(f"   ctags version: {version_info}")
            print()

    finally:
        # Clean up temp file
        try:
            os.unlink(test_file)
        except OSError:
            pass


def get_ctags_version(ctags_bin: str = "ctags") -> str:
    """
    Get ctags version string for debugging/logging.

    Args:
        ctags_bin: Path to ctags binary

    Returns:
        Version string (first line of --version output)
    """
    try:
        result = subprocess.run(
            [ctags_bin, "--version"],
            capture_output=True,
            check=True,
            text=True
        )
        return result.stdout.strip().split('\n')[0]
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unknown (ctags not found)"


# Simple test
if __name__ == "__main__":
    print("Running ctags compatibility check...")
    try:
        verify_ctags_compatibility()
        print("✅ ctags compatibility check PASSED")
        print(f"   Version: {get_ctags_version()}")
    except RuntimeError as e:
        print(f"❌ {e}")
        exit(1)
