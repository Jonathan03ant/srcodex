#!/usr/bin/env python3
"""
PMFW Code Explorer - CTags Parser
Wrapper around Universal CTags to extract symbols from C code
"""

import subprocess
import json
import os
from typing import List, Dict, Optional
from pathlib import Path


class CTagsParser:
    """Parse C source code using Universal CTags"""

    def __init__(self, ctags_bin: str = "ctags"):
        """
        Initialize CTags parser

        Args:
            ctags_bin: Path to ctags binary (default: "ctags")
        """
        self.ctags_bin = ctags_bin
        self._verify_ctags()

    def _verify_ctags(self):
        """Verify that ctags is installed"""
        try:
            subprocess.run(
                [self.ctags_bin, "--version"],
                capture_output=True,
                check=True
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            raise RuntimeError(f"ctags not found. Install: sudo apt install universal-ctags")

    def parse_file(self, file_path: str) -> List[Dict]:
        """
        Parse a single file and extract symbols

        Args:
            file_path: Path to C source file

        Returns:
            List of symbol dictionaries with keys:
                - name: Symbol name
                - type: Symbol type (function, variable, struct, etc.)
                - line: Line number
                - signature: Full signature (if available)
                - scope: Scope (global, static, etc.)
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Run ctags with JSON output
        cmd = [
            self.ctags_bin,
            "--output-format=json",
            "--fields=+nKSz",  # +n (line numbers), +K (kind), +S (signature), +z (scope)
            "--c-kinds=+p",     # Include function prototypes
            "-f", "-",          # Output to stdout
            file_path
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Warning: ctags failed on {file_path}: {e}")
            return []

        # Parse JSON output
        symbols = []
        for line in result.stdout.strip().split('\n'):
            if not line or line.startswith('!'):
                continue

            try:
                tag = json.loads(line)
                symbol = self._parse_tag(tag, file_path)
                if symbol:
                    symbols.append(symbol)
            except json.JSONDecodeError:
                continue

        return symbols

    def _parse_tag(self, tag: Dict, file_path: str) -> Optional[Dict]:
        """
        Parse a ctags tag into our symbol format

        Args:
            tag: Raw ctags tag dictionary
            file_path: Source file path

        Returns:
            Symbol dictionary or None if invalid
        """
        # Extract basic info
        name = tag.get('name')
        kind = tag.get('kind')
        line = tag.get('line', 0)

        if not name or not kind:
            return None

        if name.startswith('__anon'):
            return None

        # Handle typedef structs/unions/enums - treat them as struct/union/enum with the typedef name
        typeref = tag.get('typeref', '')
        if kind == 'typedef' and typeref.startswith('struct:'):
            symbol_type = 'struct'
        elif kind == 'typedef' and typeref.startswith('union:'):
            symbol_type = 'union'
        elif kind == 'typedef' and typeref.startswith('enum:'):
            symbol_type = 'enum'
        else:
            # Map ctags kinds to our types
            type_map = {
                'function': 'function',
                'prototype': 'function',
                'variable': 'variable',
                'struct': 'struct',
                'union': 'union',
                'enum': 'enum',
                'enumerator': 'enumerator',
                'typedef': 'typedef',
                'macro': 'macro',
                'member': 'member',
                'header': 'header',
            }
            symbol_type = type_map.get(kind, kind)

        # Extract signature if available
        signature = tag.get('signature', '')
        if not signature and symbol_type == 'function':
            # Try to build basic signature from typeref
            if typeref:
                signature = f"{typeref} {name}()"
            else:
                signature = f"{name}()"

        # Extract scope
        scope = tag.get('scope', 'global')
        if 'scopeKind' in tag and 'scope' in tag:
            scope_name = tag['scope']
            # If scope is anonymous struct, skip the scope info (we don't care)
            if not scope_name.startswith('__anon'):
                scope = f"{tag['scopeKind']}:{scope_name}"

        # Check if static
        access = tag.get('access', '')
        if 'file' in tag.get('extras', []) or access == 'private':
            scope = 'static'

        return {
            'name': name,
            'type': symbol_type,
            'line': line,
            'signature': signature,
            'scope': scope,
            'file_path': file_path
        }

    def parse_directory(self, dir_path: str, extensions: List[str] = None) -> Dict[str, List[Dict]]:
        """
        Parse all files in a directory recursively

        Args:
            dir_path: Directory to scan
            extensions: File extensions to include (default: ['.c', '.h'])

        Returns:
            Dictionary mapping file paths to symbol lists
        """
        if extensions is None:
            extensions = ['.c', '.h']

        results = {}
        dir_path = Path(dir_path)

        # Find all matching files
        for ext in extensions:
            for file_path in dir_path.rglob(f'*{ext}'):
                if file_path.is_file():
                    symbols = self.parse_file(str(file_path))
                    results[str(file_path)] = symbols

        return results


# Simple test
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python ctags_parser.py <file_or_directory>")
        sys.exit(1)

    parser = CTagsParser()
    path = sys.argv[1]

    if os.path.isfile(path):
        symbols = parser.parse_file(path)
        print(f"Found {len(symbols)} symbols in {path}:")
        for sym in symbols:  # Show all symbols
            print(f"  {sym['type']:12} {sym['name']:30} @ line {sym['line']}")
    else:
        results = parser.parse_directory(path)
        total = sum(len(syms) for syms in results.values())
        print(f"Found {total} symbols in {len(results)} files")
        for file_path, symbols in list(results.items())[:5]:  # Show first 5 files
            print(f"\n{file_path}: {len(symbols)} symbols")
            for sym in symbols[:5]:
                print(f"  {sym['type']:12} {sym['name']:30} @ line {sym['line']}")
