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
        Query all function symbols from database

        Returns:
            List of (symbol_id, function_name) tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, name FROM symbols WHERE type = 'function' ORDER BY id"
        )
        return cursor.fetchall()

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
            print("🗑️  Clearing existing raw_references...")
            self.conn.execute("DELETE FROM raw_references WHERE query_type = 'callees'")
            self.conn.commit()

        # Get all functions from symbols table
        functions = self.get_all_functions()
        print(f"📊 Found {len(functions)} functions to query")

        if not functions:
            print("⚠️  No functions found in symbols table")
            return 0

        # Prepare batch insert data
        raw_refs_batch = []

        # Query cscope for each function with progress bar
        print("🔍 Querying cscope for callees...")
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
                tqdm.write(f"⚠️  Failed to query {function_name}: {e}")
                continue

        # Batch insert with transaction
        if raw_refs_batch:
            print(f"💾 Inserting {len(raw_refs_batch)} raw references...")
            cursor = self.conn.cursor()
            cursor.executemany(
                """INSERT INTO raw_references
                   (query_type, query_symbol, source_file, source_function, line_number, line_text)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                raw_refs_batch
            )
            self.conn.commit()
            print(f"✅ Inserted {len(raw_refs_batch)} raw references")
        else:
            print("⚠️  No references found")

        return len(raw_refs_batch)
