#!/usr/bin/env python3
"""
Reference Resolver - Convert raw references to semantic graph edges
Resolves (file, function) names → symbol IDs and stores typed edges in symbol_edges
"""

import sqlite3
import re
from pathlib import Path
from typing import Optional, List, Tuple, Dict
from dataclasses import dataclass
from collections import Counter
from tqdm import tqdm


@dataclass
class ResolutionStats:
    """Statistics for resolution process"""
    total_raw_refs: int = 0
    resolved_edges: int = 0
    unresolved_src: int = 0
    unresolved_dst: int = 0
    ambiguous_dst: int = 0
    skipped_parsing: int = 0


class ReferenceResolver:
    """Resolves raw cscope references into semantic graph edges with symbol IDs"""

    # C keywords to exclude from callee extraction
    C_KEYWORDS = {
        'if', 'for', 'while', 'switch', 'return', 'sizeof', 'typeof',
        'do', 'else', 'case', 'break', 'continue', 'goto', 'default'
    }

    def __init__(self, db_conn: sqlite3.Connection):
        """
        Initialize reference resolver

        Args:
            db_conn: SQLite database connection (must have raw_references and symbols tables)
        """
        self.conn = db_conn
        self.stats = ResolutionStats()

    def _extract_callee_from_line(self, line_text: str) -> Optional[str]:
        """
        Extract callee function name from line_text using IDENT( pattern

        Looks for patterns like: function_name(...)
        Excludes: keywords, macro-ish ALL_CAPS (optional)

        Args:
            line_text: Raw line content from cscope

        Returns:
            Callee function name or None if not found/excluded
        """
        # Pattern: identifier followed by '('
        # Match: alphanumeric + underscore, then opening paren
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\('

        matches = re.findall(pattern, line_text)

        for match in matches:
            # Exclude C keywords
            if match in self.C_KEYWORDS:
                continue

            # Optional: exclude macro-ish ALL_CAPS
            # Uncomment to enable:
            # if match.isupper() and len(match) > 2:
            #     continue

            # Return first valid match
            return match

        return None

    def _resolve_src_symbol(self, query_symbol: str, source_file: str) -> Optional[int]:
        """
        Resolve source symbol (caller) by query_symbol name

        Args:
            query_symbol: Function name that was queried (caller)
            source_file: File where caller is defined (for disambiguation)

        Returns:
            symbol_id or None if not found/ambiguous
        """
        cursor = self.conn.cursor()

        # Query: find function symbol matching query_symbol
        cursor.execute(
            """SELECT id, file_path FROM symbols
               WHERE name = ? AND type = 'function'""",
            (query_symbol,)
        )

        results = cursor.fetchall()

        if len(results) == 0:
            return None  # Not found

        if len(results) == 1:
            return results[0][0]  # Unique match

        # Multiple matches: prefer same file
        for row in results:
            if row[1] == source_file:
                return row[0]

        # Still ambiguous: return None (could pick first, but being strict)
        return None

    def _resolve_dst_symbol(self, callee_name: str, source_file: str) -> Optional[int]:
        """
        Resolve destination symbol (callee) by function name with disambiguation

        Disambiguation rules:
        1. Accept if unique
        2. Prefer same file
        3. Prefer .c file over .h (definition vs declaration)
        4. If still multiple: return None (unresolved)

        Args:
            callee_name: Function name being called
            source_file: File where call occurs (for disambiguation)

        Returns:
            symbol_id or None if not found/ambiguous
        """
        cursor = self.conn.cursor()

        # Query: find function symbols matching callee name
        cursor.execute(
            """SELECT id, file_path FROM symbols
               WHERE name = ? AND type = 'function'""",
            (callee_name,)
        )

        results = cursor.fetchall()

        if len(results) == 0:
            return None  # Not found

        if len(results) == 1:
            return results[0][0]  # Unique match

        # Multiple matches: prefer same file first
        for row in results:
            if row[1] == source_file:
                return row[0]

        # Still ambiguous: prefer .c file over .h (definition vs declaration)
        c_files = [row for row in results if row[1].endswith('.c')]
        if len(c_files) == 1:
            return c_files[0][0]

        # Still ambiguous: unresolved
        return None

    def resolve_callees(self, clear_existing: bool = False) -> Dict[str, int]:
        """
        Resolve callgraph edges: raw_references (query_type='callees') → symbol_edges

        For each raw reference:
        1. Extract callee from line_text (IDENT( pattern)
        2. Resolve src_symbol_id (query_symbol → symbol.id)
        3. Resolve dst_symbol_id (callee → symbol.id)
        4. Insert into symbol_edges with edge_type='CALLS'

        Args:
            clear_existing: If True, delete existing CALLS edges before resolution

        Returns:
            Dictionary with resolution statistics
        """
        if clear_existing:
            print("🗑️  Clearing existing CALLS edges...")
            self.conn.execute("DELETE FROM symbol_edges WHERE edge_type = 'CALLS'")
            self.conn.commit()

        # Fetch all raw references with query_type='callees'
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT id, query_symbol, source_file, source_function, line_number, line_text
               FROM raw_references
               WHERE query_type = 'callees'
               ORDER BY id"""
        )

        raw_refs = cursor.fetchall()
        self.stats.total_raw_refs = len(raw_refs)

        print(f"📊 Found {self.stats.total_raw_refs} raw references to resolve")

        if not raw_refs:
            print("⚠️  No raw references found")
            return self._stats_dict()

        # Prepare batch insert for resolved edges
        edges_batch = []

        # Track unresolved reasons for reporting
        unresolved_reasons = Counter()

        print("🔍 Resolving symbol IDs...")
        for row in tqdm(raw_refs, desc="Resolving edges"):
            raw_id, query_symbol, source_file, source_function, line_number, line_text = row

            # Step 1: Extract callee from line_text
            callee_name = self._extract_callee_from_line(line_text)
            if not callee_name:
                self.stats.skipped_parsing += 1
                unresolved_reasons['no_callee_in_line'] += 1
                continue

            # Step 2: Resolve src_symbol_id (caller)
            src_symbol_id = self._resolve_src_symbol(query_symbol, source_file)
            if not src_symbol_id:
                self.stats.unresolved_src += 1
                unresolved_reasons['src_not_found'] += 1
                continue

            # Step 3: Resolve dst_symbol_id (callee)
            dst_symbol_id = self._resolve_dst_symbol(callee_name, source_file)
            if not dst_symbol_id:
                self.stats.unresolved_dst += 1
                unresolved_reasons['dst_not_found_or_ambiguous'] += 1
                continue

            # Both resolved: add to batch
            edges_batch.append((
                'CALLS',            # edge_type
                src_symbol_id,      # src_symbol_id (caller)
                dst_symbol_id,      # dst_symbol_id (callee)
                source_file,        # source_file (where edge occurs)
                line_number,        # line_number
            ))

        # Batch insert resolved edges
        if edges_batch:
            print(f"💾 Inserting {len(edges_batch)} resolved edges...")
            cursor.executemany(
                """INSERT OR IGNORE INTO symbol_edges
                   (edge_type, src_symbol_id, dst_symbol_id, source_file, line_number)
                   VALUES (?, ?, ?, ?, ?)""",
                edges_batch
            )
            self.conn.commit()
            self.stats.resolved_edges = len(edges_batch)
            print(f"✅ Inserted {self.stats.resolved_edges} CALLS edges")
        else:
            print("⚠️  No edges resolved")

        # Print resolution statistics
        self._print_stats(unresolved_reasons)

        return self._stats_dict()

    def _stats_dict(self) -> Dict[str, int]:
        """Convert stats to dictionary"""
        return {
            'total_raw_refs': self.stats.total_raw_refs,
            'resolved_edges': self.stats.resolved_edges,
            'unresolved_src': self.stats.unresolved_src,
            'unresolved_dst': self.stats.unresolved_dst,
            'skipped_parsing': self.stats.skipped_parsing,
        }

    def _print_stats(self, unresolved_reasons: Counter):
        """Print resolution statistics"""
        print()
        print("=" * 60)
        print("📈 Resolution Statistics")
        print("=" * 60)
        print(f"Total raw references:     {self.stats.total_raw_refs}")
        print(f"✅ Resolved edges:         {self.stats.resolved_edges}")
        print(f"❌ Unresolved (src):       {self.stats.unresolved_src}")
        print(f"❌ Unresolved (dst):       {self.stats.unresolved_dst}")
        print(f"⚠️  Skipped (no callee):   {self.stats.skipped_parsing}")

        if unresolved_reasons:
            print()
            print("Unresolved breakdown:")
            for reason, count in unresolved_reasons.most_common():
                print(f"  {reason}: {count}")

        # Calculate resolution rate
        if self.stats.total_raw_refs > 0:
            rate = (self.stats.resolved_edges / self.stats.total_raw_refs) * 100
            print()
            print(f"Resolution rate: {rate:.1f}%")
