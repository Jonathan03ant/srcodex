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
                - scope_kind: Parent scope kind (struct, union, enum)
                - scope_name: Parent scope name (PowerState, Dummy, etc.)
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

        # Parse JSON output - TWO PASS approach:
        # Pass 1: Build mapping of anonymous structs to typedef names
        # Pass 2: Parse all tags and resolve anonymous struct references

        raw_tags = []
        anon_to_typedef = {}  # Maps __anonXXX -> typedef name

        for line in result.stdout.strip().split('\n'):
            if not line or line.startswith('!'):
                continue

            try:
                tag = json.loads(line)
                raw_tags.append(tag)

                # If this is a typedef for a struct/union/enum, record the mapping
                if tag.get('kind') == 'typedef':
                    typeref = tag.get('typeref', '')
                    if typeref.startswith('struct:') or typeref.startswith('union:') or typeref.startswith('enum:'):
                        # typeref is like "struct:__anondd0b9e6c0108"
                        anon_name = typeref.split(':', 1)[1]
                        typedef_name = tag.get('name')
                        if anon_name.startswith('__anon') and typedef_name:
                            anon_to_typedef[anon_name] = typedef_name
            except json.JSONDecodeError:
                continue

        # Pass 2: Parse all tags with resolved scope names
        symbols = []
        for tag in raw_tags:
            symbol = self._parse_tag(tag, file_path, anon_to_typedef)
            if symbol:
                symbols.append(symbol)

        return symbols

    def _parse_tag(self, tag: Dict, file_path: str, anon_to_typedef: Dict[str, str] = None) -> Optional[Dict]:
        """
        Parse a ctags tag into our symbol format

        Args:
            tag: Raw ctags tag dictionary
            file_path: Source file path
            anon_to_typedef: Mapping from anonymous struct names to typedef names

        Returns:
            Symbol dictionary or None if invalid
        """
        if anon_to_typedef is None:
            anon_to_typedef = {}
        # Extract basic info
        name = tag.get('name')
        kind = tag.get('kind')
        line = tag.get('line', 0)

        if not name or not kind:
            return None

        if name.startswith('__anon'):
            return None

        # Extract raw typeref and signature from ctags (before we process them)
        # Store NULL if not provided - DO NOT invent values
        raw_typeref = tag.get('typeref') if 'typeref' in tag else None
        raw_signature = tag.get('signature') if 'signature' in tag else None

        # Handle typedef structs/unions/enums - treat them as struct/union/enum with the typedef name
        if kind == 'typedef' and raw_typeref:
            if raw_typeref.startswith('struct:'):
                symbol_type = 'struct'
            elif raw_typeref.startswith('union:'):
                symbol_type = 'union'
            elif raw_typeref.startswith('enum:'):
                symbol_type = 'enum'
            else:
                symbol_type = 'typedef'
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

        # Extract scope information
        scope = 'global'  # Default scope for top-level symbols (deprecated, use is_file_scope)
        scope_kind = None
        scope_name = None

        # Extract parent scope (struct/union/enum/class)
        if 'scopeKind' in tag and 'scope' in tag:
            parent_scope_name = tag['scope']

            # Resolve anonymous struct names to their typedef names
            if parent_scope_name.startswith('__anon') and parent_scope_name in anon_to_typedef:
                parent_scope_name = anon_to_typedef[parent_scope_name]

            # Store scope info (skip only if still anonymous after resolution)
            if not parent_scope_name.startswith('__anon'):
                scope_kind = tag['scopeKind']
                scope_name = parent_scope_name

        # Detect file-local scope (static in C)
        # ctags provides this via the 'file' boolean field or 'fileScope' in extras
        is_file_scope = None  # NULL = unknown

        # Check the 'file' boolean field (most reliable)
        if 'file' in tag:
            is_file_scope = 1 if tag['file'] else 0
        # Fallback: check 'extras' string for 'fileScope'
        elif 'extras' in tag:
            extras_str = tag.get('extras', '')
            if 'fileScope' in extras_str:
                is_file_scope = 1
            else:
                is_file_scope = 0

        # Keep old 'scope' field for backwards compatibility (deprecated)
        if is_file_scope == 1:
            scope = 'static'

        return {
            'name': name,
            'type': symbol_type,
            'line': line,
            'signature': raw_signature,  # Raw from ctags, NULL if not available
            'typeref': raw_typeref,      # Raw from ctags, NULL if not available
            'scope': scope,  # Deprecated: kept for backwards compatibility
            'scope_kind': scope_kind,
            'scope_name': scope_name,
            'is_file_scope': is_file_scope,
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
            # Build qualified name if it has a parent scope
            if sym.get('scope_kind') and sym.get('scope_name'):
                qualified = f"{sym['scope_name']}.{sym['name']}"
                scope_info = f" ({sym['scope_kind']}:{sym['scope_name']})"
            else:
                qualified = sym['name']
                scope_info = ""

            # Add file-scope indicator
            file_scope_indicator = ""
            if sym.get('is_file_scope') == 1:
                file_scope_indicator = " [file-local]"
            elif sym.get('is_file_scope') == 0:
                file_scope_indicator = " [global]"

            # Add signature for functions (including return type from typeref)
            sig_display = ""
            if sym['type'] == 'function':
                # Build full signature: "return_type name(params)"
                return_type = ""
                if sym.get('typeref'):
                    # typeref is like "typename:void" or "typename:int"
                    return_type = sym['typeref'].replace('typename:', '') + ' '

                params = sym.get('signature', '()')
                sig_display = f"{return_type}{params}"

            print(f"  {sym['type']:12} {qualified:30}{sig_display:40} @ line {sym['line']}{scope_info}{file_scope_indicator}")
    else:
        results = parser.parse_directory(path)
        total = sum(len(syms) for syms in results.values())
        print(f"Found {total} symbols in {len(results)} files")
        for file_path, symbols in list(results.items())[:5]:  # Show first 5 files
            print(f"\n{file_path}: {len(symbols)} symbols")
            for sym in symbols[:5]:
                print(f"  {sym['type']:12} {sym['name']:30} @ line {sym['line']}")
