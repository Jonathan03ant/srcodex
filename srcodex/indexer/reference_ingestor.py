#!/usr/bin/env python3
"""
Reference Ingestor - Raw cscope output to database storage
Ingests cscope query results into raw_references table (untrusted sensor data)
"""

import sqlite3
from pathlib import Path
from typing import List, Tuple
from tqdm import tqdm

from cscope_client import CscopeClient


class ReferenceIngestor:
    """Ingests raw cscope query results into raw_references table"""

    def __init__(self, db_conn: sqlite3.Connection, source_root: Path, cscope_dir: Path):
        """
        Initialize reference ingestor

        Args:
            db_conn: SQLite database connection (must have raw_references table)
            source_root: Root directory of source code (for path normalization)
            cscope_dir: Directory containing cscope.out
        """
        self.conn = db_conn
        self.source_root = source_root
        self.cscope_client = CscopeClient(str(cscope_dir))

    def _normalize_path(self, cscope_path: str) -> str:
        """
        Normalize cscope output path to canonical rel_posix format.

        After Fix 1.2 (cscope built with cwd=source_root), cscope output paths
        should already be rel_posix. This function validates and handles edge cases.

        Args:
            cscope_path: Path from cscope output (should be rel_posix from source_root)

        Returns:
            Canonical rel_posix path (relative to source_root)
        """
        path = Path(cscope_path)

        # If absolute (shouldn't happen after Fix 1.2, but handle defensively)
        if path.is_absolute():
            try:
                return path.relative_to(self.source_root).as_posix()
            except ValueError:
                # Path outside source_root - this is an error, but store as-is
                return path.as_posix()

        # Already relative (expected case after Fix 1.2)
        # Normalize to POSIX format
        return path.as_posix()

    def get_all_functions(self) -> List[Tuple[int, str]]:
        """
        Query all function symbols from database (implementations only, not prototypes)

        Returns:
            List of (symbol_id, function_name) tuples

        Note: Excludes prototypes to avoid querying cscope multiple times for the same function
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, name FROM symbols WHERE type = 'function' AND kind_raw = 'function' ORDER BY id"
        )
        return cursor.fetchall()

    def get_all_headers(self) -> List[Tuple[str, str]]:
        """
        Query all header files from database

        Returns:
            List of (file_path, basename) tuples
            Example: [("power.h", "power.h"), ("common/voltage.h", "voltage.h")]
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT path FROM files WHERE path LIKE '%.h' ORDER BY path"
        )
        files = cursor.fetchall()

        # Return (full_path, basename) tuples for cscope queries
        return [(row['path'], Path(row['path']).name) for row in files]

    def ingest_callees(self, clear_existing: bool = False) -> int:
        """
        Ingest callgraph data: query cscope -2 (callees) for all functions

        For each function in symbols table:
        - Query cscope find_callees(function_name)
        - Store results in raw_references with query_type='callees'

        Args:
            clear_existing: If True, delete existing raw_references before ingestion

        Returns:
            Number of raw references inserted
        """
        if clear_existing:
            print("Clearing existing raw_references...")
            self.conn.execute("DELETE FROM raw_references WHERE query_type = 'callees'")
            self.conn.commit()

        # Get all functions from symbols table
        functions = self.get_all_functions()
        print(f"Found {len(functions)} functions to query")

        if not functions:
            print("Warning: No functions found in symbols table")
            return 0

        # Prepare batch insert data
        raw_refs_batch = []

        # Query cscope for each function with progress bar
        print("Querying cscope for callees...")
        for symbol_id, function_name in tqdm(functions, desc="Ingesting callees"):
            try:
                # Query cscope: find functions called by this function
                results = self.cscope_client.find_callees(function_name)

                # Convert each Reference to raw_references row
                for ref in results:
                    normalized_path = self._normalize_path(ref.file_path)
                    raw_refs_batch.append((
                        'callees',              # query_type
                        function_name,          # query_symbol (the function we queried)
                        normalized_path,        # source_file
                        ref.function,           # source_function (from cscope output)
                        ref.line_number,        # line_number
                        ref.line_text,          # line_text
                    ))

            except Exception as e:
                # Don't fail entire ingestion on single query error
                tqdm.write(f"Warning: Failed to query {function_name}: {e}")
                continue

        # Batch insert with transaction
        if raw_refs_batch:
            print(f"Inserting {len(raw_refs_batch)} raw references...")
            cursor = self.conn.cursor()
            cursor.executemany(
                """INSERT INTO raw_references
                   (query_type, query_symbol, source_file, source_function, line_number, line_text)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                raw_refs_batch
            )
            self.conn.commit()
            print(f"Inserted {len(raw_refs_batch)} raw references")
        else:
            print("Warning: No references found")

        return len(raw_refs_batch)

    def ingest_callers(self, clear_existing: bool = False) -> int:
        """
        Ingest reverse callgraph data: query cscope -3 (callers) for all functions

        For each function in symbols table:
        - Query cscope find_callers(function_name)
        - Store results in raw_references with query_type='callers'

        Args:
            clear_existing: If True, delete existing raw_references before ingestion

        Returns:
            Number of raw references inserted
        """
        if clear_existing:
            print("Clearing existing callers references...")
            self.conn.execute("DELETE FROM raw_references WHERE query_type = 'callers'")
            self.conn.commit()

        # Get all functions from symbols table
        functions = self.get_all_functions()
        print(f"Found {len(functions)} functions to query for callers")

        if not functions:
            print("Warning: No functions found in symbols table")
            return 0

        # Prepare batch insert data
        raw_refs_batch = []

        # Query cscope for each function with progress bar
        print("Querying cscope for callers...")
        for symbol_id, function_name in tqdm(functions, desc="Ingesting callers"):
            try:
                # Query cscope: find functions that call this function
                results = self.cscope_client.find_callers(function_name)

                # Convert each Reference to raw_references row
                for ref in results:
                    normalized_path = self._normalize_path(ref.file_path)
                    raw_refs_batch.append((
                        'callers',              # query_type
                        function_name,          # query_symbol (the function we queried)
                        normalized_path,        # source_file
                        ref.function,           # source_function (caller)
                        ref.line_number,        # line_number
                        ref.line_text,          # line_text
                    ))

            except Exception as e:
                # Don't fail entire ingestion on single query error
                tqdm.write(f"Warning: Failed to query {function_name}: {e}")
                continue

        # Batch insert with transaction
        if raw_refs_batch:
            print(f"Inserting {len(raw_refs_batch)} raw references...")
            cursor = self.conn.cursor()
            cursor.executemany(
                """INSERT INTO raw_references
                   (query_type, query_symbol, source_file, source_function, line_number, line_text)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                raw_refs_batch
            )
            self.conn.commit()
            print(f"Inserted {len(raw_refs_batch)} callers references")
        else:
            print("Warning: No callers references found")

        return len(raw_refs_batch)

    def ingest_includes(self, clear_existing: bool = False) -> int:
        """
        Ingest include graph data: query cscope -8 (files including) for all headers

        For each .h file in files table:
        - Query cscope find_files_including(header_name)
        - Store results in raw_references with query_type='includes'

        Note: source_function will be "<global>" since includes are file-level

        Args:
            clear_existing: If True, delete existing includes references before ingestion

        Returns:
            Number of raw references inserted
        """
        if clear_existing:
            print("Clearing existing includes references...")
            self.conn.execute("DELETE FROM raw_references WHERE query_type = 'includes'")
            self.conn.commit()

        # Get all header files from files table
        headers = self.get_all_headers()
        print(f"Found {len(headers)} header files to query for includes")

        if not headers:
            print("Warning: No header files found in files table")
            return 0

        # Prepare batch insert data
        raw_refs_batch = []

        # Query cscope for each header with progress bar
        print("Querying cscope for includes...")
        for full_path, basename in tqdm(headers, desc="Ingesting includes"):
            try:
                # Query cscope: find files that include this header
                # NOTE: cscope -8 queries by basename, not full path
                results = self.cscope_client.find_files_including(basename)

                # Convert each Reference to raw_references row
                for ref in results:
                    normalized_path = self._normalize_path(ref.file_path)
                    raw_refs_batch.append((
                        'includes',             # query_type
                        basename,               # query_symbol (the header we queried)
                        normalized_path,        # source_file (file that includes the header)
                        '<global>',             # source_function (includes are file-level)
                        ref.line_number,        # line_number
                        ref.line_text,          # line_text (the #include directive)
                    ))

            except Exception as e:
                # Don't fail entire ingestion on single query error
                tqdm.write(f"Warning: Failed to query {basename}: {e}")
                continue

        # Batch insert with transaction
        if raw_refs_batch:
            print(f"Inserting {len(raw_refs_batch)} raw references...")
            cursor = self.conn.cursor()
            cursor.executemany(
                """INSERT INTO raw_references
                   (query_type, query_symbol, source_file, source_function, line_number, line_text)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                raw_refs_batch
            )
            self.conn.commit()
            print(f"Inserted {len(raw_refs_batch)} includes references")
        else:
            print("Warning: No includes references found")

        return len(raw_refs_batch)
