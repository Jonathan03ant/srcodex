#!/usr/bin/env python3
"""
Cscope Client - Query cscope database for cross-references
This module provides a Python interface to query cscope databases.
It runs cscope commands and parses the output into structured data.
"""

import subprocess
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class Reference:
    """A single cross-reference result from cscope"""
    file_path: str      # File where the reference occurs
    function: str       # Function name containing the reference
    line_number: int    # Line number in the file
    line_text: str      # The actual line of code

class CscopeClient:
    """Client for querying cscope database"""

    def __init__(self, cscope_dir: str):
        """
        Initialize cscope client

        Args:
            cscope_dir: Directory containing cscope.out and related files
        """
        self.cscope_dir = Path(cscope_dir)
        self.cscope_out = self.cscope_dir / "cscope.out"

        if not self.cscope_out.exists():
            raise FileNotFoundError(
                f"Cscope database not found: {self.cscope_out}\n"
                f"Run indexer with --cscope flag to build it."
            )

    def _run_query(self, query_type: int, symbol: str) -> List[Reference]:
        """
        Run a cscope query and parse results

        Args:
            query_type: Cscope query type (0-8)
            symbol: Symbol to search for

        Returns:
            List of Reference objects
        """
        try:
            result = subprocess.run(
                ['cscope', '-dL', f'-{query_type}', symbol],
                cwd=self.cscope_dir,
                capture_output=True,
                text=True,
                check=True
            )

            return self._parse_output(result.stdout)

        except subprocess.CalledProcessError as e:
            # Cscope returns non-zero if no results found - that's okay
            if e.returncode == 1 and not e.stderr:
                return []
            raise RuntimeError(f"Cscope query failed: {e.stderr}")

        except FileNotFoundError:
            raise RuntimeError(
                "cscope command not found. Install with: sudo apt install cscope"
            )

    def _parse_output(self, output: str) -> List[Reference]:
        """
        Parse cscope output into Reference objects

        Cscope output format:
        filename function_name line_number line_text

        Example:
        power.c init_power 15 int init_power(void) {
        thermal.c adjust_thermal 23     init_power();
        """
        references = []

        for line in output.strip().split('\n'):
            if not line:
                continue

            # Split on whitespace (max 3 splits to preserve line_text)
            parts = line.split(None, 3)

            if len(parts) < 3:
                continue  # Malformed line

            file_path = parts[0]
            function = parts[1]
            line_number = int(parts[2])
            line_text = parts[3] if len(parts) > 3 else ''

            references.append(Reference(
                file_path=file_path,
                function=function,
                line_number=line_number,
                line_text=line_text
            ))

        return references

    """
        Below are API's to find references to symbols 
        Args: Symbol name, Function name, Text to search for
        Returns: Lists of symbol references, functions...
    """
    def find_symbol(self, symbol: str) -> List[Reference]:
        # List of references where this symbol appears
        return self._run_query(0, symbol)

    def find_definition(self, symbol: str) -> List[Reference]:
        # List with one reference (the definition location)
        return self._run_query(1, symbol)

    def find_callees(self, function: str) -> List[Reference]:
        # List of functions called by this function
        return self._run_query(2, function)

    def find_callers(self, function: str) -> List[Reference]:
        # List of functions that call this function
        return self._run_query(3, function)

    def find_text(self, text: str) -> List[Reference]:
        # List of references where this text appears
        return self._run_query(4, text)

    def find_egrep_pattern(self, pattern: str) -> List[Reference]:
        # List of references matching the pattern
        return self._run_query(6, pattern)

    def find_files_including(self, filename: str) -> List[Reference]:
        # List of files that include this file
        return self._run_query(8, filename)

    def get_stats(self) -> Dict[str, any]:
        # Dictionary with database information
        stats = {
            'database_path': str(self.cscope_out),
            'database_exists': self.cscope_out.exists(),
        }

        if self.cscope_out.exists():
            stats['database_size_mb'] = self.cscope_out.stat().st_size / (1024 * 1024)

        # Check for cscope.files
        cscope_files = self.cscope_dir / "cscope.files"
        if cscope_files.exists():
            with open(cscope_files) as f:
                stats['indexed_files'] = len(f.readlines())

        return stats


# Convenience function for quick queries
def query_cscope(cscope_dir: str, query_type: str, symbol: str) -> List[Reference]:
    """
    Convenience function for one-off cscope queries

    Args:
        cscope_dir: Directory containing cscope.out
        query_type: Query type (symbol, definition, callers, callees, text, includes)
        symbol: Symbol/text to search for

    Returns:
        List of references

    Example:
        >>> refs = query_cscope('data/', 'callers', 'init_power')
        >>> for ref in refs:
        ...     print(f"{ref.file_path}:{ref.line_number} {ref.function}")
    """
    client = CscopeClient(cscope_dir)

    query_map = {
        'symbol': client.find_symbol,
        'definition': client.find_definition,
        'callers': client.find_callers,
        'callees': client.find_callees,
        'text': client.find_text,
        'includes': client.find_files_including,
    }

    if query_type not in query_map:
        raise ValueError(
            f"Invalid query type: {query_type}\n"
            f"Valid types: {', '.join(query_map.keys())}"
        )

    return query_map[query_type](symbol)
