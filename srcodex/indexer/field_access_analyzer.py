"""
Field Access Analyzer
Analyzes function bodies to find field access patterns and creates ACCESSES edges

Similar to cscope (which finds CALLS), but focuses on field accesses:
    function.field
    pointer->field
    struct.nested.field
Populates symbol_edges table with edge_type='ACCESSES'
"""
import re
from pathlib import Path
from typing import List, Tuple, Optional
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial


class FieldAccessAnalyzer:
    def __init__(self, db_conn, source_root: Path):
        """
        Args:
            db_conn: SQLite database connectio
            source_root: Root directory of source code (absolute path)
        """
        self.conn = db_conn
        self.source_root = Path(source_root)

        # Regex patterns for field access
        self.arrow_pattern = re.compile(r'\b(\w+)\s*->\s*(\w+)')
        self.dot_pattern = re.compile(r'\b([a-zA-Z_]\w*)\s*\.\s*(\w+)')

    def _get_functions(self) -> List[dict]:
        """Get all functions from database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, name, file_path, line_number
            FROM symbols
            WHERE type = 'function'
            ORDER BY file_path, line_number
        """)

        return [dict(row) for row in cursor.fetchall()]

    def _read_function_body(self, file_path: str, start_line: int) -> List[Tuple[int, str]]:
        """Read function body from source file"""
        full_path = self.source_root / file_path

        if not full_path.exists():
              return []

        try:
            with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
        except Exception:
            return []

        # find function body boundaries
        body_lines = []
        brace_count = 0
        in_function = False

        for i in range(start_line - 1, len(lines)):
            line = lines[i]
            line_num = i + 1

            # Count braces
            for char in line:
                if char == '{':
                    brace_count += 1
                    in_function = True
                elif char == '}':
                    brace_count -= 1

            # Collect lines inside function body
            if in_function:
                body_lines.append((line_num, line))

            # Found matching closing brace
            if in_function and brace_count == 0:
                break

        return body_lines

    def _extract_field_accesses(self, body_lines: List[Tuple[int, str]]) -> List[Tuple[str, int]]:
        """Extract field access patterns from function body"""
        accesses = []

        for line_num, line_text in body_lines:
            clean_line = self._clean_line(line_text)

            # Find arrow accesses: ptr->field
            for match in self.arrow_pattern.finditer(clean_line):
                field_name = match.group(2)
                accesses.append((field_name, line_num))

            # Find dot accesses: var.field
            for match in self.dot_pattern.finditer(clean_line):
                var_name = match.group(1)
                field_name = match.group(2)

                if self._is_valid_field_access(var_name, field_name):
                    accesses.append((field_name, line_num))

        return accesses

    def _clean_line(self, line: str) -> str:
        """Remove comments and string literals"""
        # Remove // comments
        line = re.sub(r'//.*', '', line)

        # Remove /* */ comments
        line = re.sub(r'/\*.*?\*/', '', line)

        # Remove string literals
        line = re.sub(r'"[^"]*"', '', line)
        line = re.sub(r"'[^']*'", '', line)

        return line

    def _is_valid_field_access(self, var_name: str, field_name: str) -> bool:
        """Filter false positives"""
        # Reject numeric literals, Keywords and preprocessors
        if var_name.isdigit():
            return False

        keywords = {'return', 'break', 'continue', 'goto', 'if', 'while', 'for', 'switch'}
        if var_name in keywords:
            return False

        if var_name.startswith('#'):
            return False

        return True

    def _resolve_and_create_edges(
        self, function_id: int, accesses: List[Tuple[str, int]], file_path: str
    ) -> Tuple[int, int]:
        """Resolve field names to IDs and create edges"""
        resolved = 0
        unresolved = 0

        cursor = self.conn.cursor()

        for field_name, line_num in accesses:
            cursor.execute("""
                SELECT id, scope_name
                FROM symbols
                WHERE name = ? AND type = 'member'
            """, (field_name,))

            matches = cursor.fetchall()

            if not matches:
                unresolved += 1
                continue

            for field_row in matches:
                field_id = field_row['id']

                try:
                    cursor.execute("""
                        INSERT INTO symbol_edges (
                            edge_type, src_symbol_id, dst_symbol_id,
                            source_file, line_number
                        )
                        VALUES ('ACCESSES', ?, ?, ?, ?)
                    """, (function_id, field_id, file_path, line_num))

                    resolved += 1
                except Exception:
                    # Duplicate edge - UNIQUE constraint prevents it
                    pass

        return resolved, unresolved

    @staticmethod
    def _analyze_function_worker(func_data: dict, source_root: Path) -> Tuple[int, List[Tuple[str, int, str, int]]]:
        """Worker function for parallel processing - analyzes one function

        Returns:
            (total_accesses, [(field_name, line_num, file_path, function_id), ...])
        """
        import re

        func_id = func_data['id']
        file_path = func_data['file_path']
        start_line = func_data['line_number']

        # Read function body
        full_path = source_root / file_path
        if not full_path.exists():
            return 0, []

        try:
            with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
        except Exception:
            return 0, []

        # Find function body boundaries
        body_lines = []
        brace_count = 0
        in_function = False

        for i in range(start_line - 1, len(lines)):
            line = lines[i]
            line_num = i + 1

            for char in line:
                if char == '{':
                    brace_count += 1
                    in_function = True
                elif char == '}':
                    brace_count -= 1

            if in_function:
                body_lines.append((line_num, line))

            if in_function and brace_count == 0:
                break

        # Extract field accesses
        arrow_pattern = re.compile(r'\b(\w+)\s*->\s*(\w+)')
        dot_pattern = re.compile(r'\b([a-zA-Z_]\w*)\s*\.\s*(\w+)')
        keywords = {'return', 'break', 'continue', 'goto', 'if', 'while', 'for', 'switch'}

        accesses = []
        for line_num, line_text in body_lines:
            # Clean line
            clean_line = re.sub(r'//.*', '', line_text)
            clean_line = re.sub(r'/\*.*?\*/', '', clean_line)
            clean_line = re.sub(r'"[^"]*"', '', clean_line)
            clean_line = re.sub(r"'[^']*'", '', clean_line)

            # Find arrow accesses: ptr->field
            for match in arrow_pattern.finditer(clean_line):
                field_name = match.group(2)
                accesses.append((field_name, line_num, file_path, func_id))

            # Find dot accesses: var.field
            for match in dot_pattern.finditer(clean_line):
                var_name = match.group(1)
                field_name = match.group(2)

                # Filter false positives
                if not var_name.isdigit() and var_name not in keywords and not var_name.startswith('#'):
                    accesses.append((field_name, line_num, file_path, func_id))

        return len(accesses), accesses

    def analyze_all_functions_parallel(self, clear_existing: bool = False, num_workers: int = None) -> dict:
        """Parallel version: Analyze all functions using multiprocessing

        Args:
            clear_existing: Delete existing ACCESSES edges before starting
            num_workers: Number of worker processes (default: cpu_count())
        """
        print("\n[Stage 1.5] Analyzing field accesses (parallel mode)...")

        if clear_existing:
            self.conn.execute("DELETE FROM symbol_edges WHERE edge_type = 'ACCESSES'")
            self.conn.commit()

        # Get all functions
        functions = self._get_functions()
        num_workers = num_workers or cpu_count()
        print(f"   Found {len(functions)} functions to analyze")
        print(f"   Using {num_workers} worker processes")

        # Process functions in parallel
        worker_fn = partial(self._analyze_function_worker, source_root=self.source_root)

        all_accesses = []
        total_accesses_count = 0

        with Pool(processes=num_workers) as pool:
            with tqdm(total=len(functions), desc="Analyzing functions", unit="func") as pbar:
                for access_count, accesses in pool.imap_unordered(worker_fn, functions, chunksize=10):
                    total_accesses_count += access_count
                    all_accesses.extend(accesses)
                    pbar.update(1)

        print(f"\n   Parallel analysis complete, resolving {len(all_accesses)} field accesses...")

        # Build field name → IDs lookup cache for faster resolution
        print("   Building field lookup cache...")
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, name FROM symbols WHERE type = 'member'")

        field_lookup = {}  # field_name → [id1, id2, ...]
        for row in cursor.fetchall():
            field_id, field_name = row['id'], row['name']
            if field_name not in field_lookup:
                field_lookup[field_name] = []
            field_lookup[field_name].append(field_id)

        # Filter out overly-ambiguous fields (appear in too many structs)
        MAX_AMBIGUITY = 100  # Skip fields that appear in >100 structs
        ambiguous_fields = [name for name, ids in field_lookup.items() if len(ids) > MAX_AMBIGUITY]
        for name in ambiguous_fields:
            del field_lookup[name]

        print(f"   Cached {len(field_lookup)} unique field names")
        print(f"   Filtered out {len(ambiguous_fields)} overly-ambiguous fields (>{MAX_AMBIGUITY} structs)")

        # Resolve and insert edges with batch inserts
        resolved_edges = 0
        unresolved_accesses = 0
        edges_batch = []
        batch_size = 5000

        with tqdm(total=len(all_accesses), desc="Resolving edges", unit="access") as pbar:
            for i, (field_name, line_num, file_path, func_id) in enumerate(all_accesses):
                # Fast lookup in cache (no database query)
                field_ids = field_lookup.get(field_name)

                if not field_ids:
                    unresolved_accesses += 1
                    pbar.update(1)
                    continue

                # Add edge for each matching field
                for field_id in field_ids:
                    edges_batch.append(('ACCESSES', func_id, field_id, file_path, line_num))
                    resolved_edges += 1

                # Batch insert every N edges
                if len(edges_batch) >= batch_size:
                    cursor.executemany("""
                        INSERT OR IGNORE INTO symbol_edges (
                            edge_type, src_symbol_id, dst_symbol_id,
                            source_file, line_number
                        )
                        VALUES (?, ?, ?, ?, ?)
                    """, edges_batch)
                    self.conn.commit()
                    edges_batch.clear()

                pbar.update(1)

        # Insert remaining edges
        if edges_batch:
            cursor.executemany("""
                INSERT OR IGNORE INTO symbol_edges (
                    edge_type, src_symbol_id, dst_symbol_id,
                    source_file, line_number
                )
                VALUES (?, ?, ?, ?, ?)
            """, edges_batch)

        self.conn.commit()

        stats = {
            'total_functions': len(functions),
            'total_accesses': total_accesses_count,
            'resolved_edges': resolved_edges,
            'unresolved_accesses': unresolved_accesses
        }

        print(f"\nField access analysis complete:")
        print(f"   Functions analyzed: {stats['total_functions']}")
        print(f"   Field accesses found: {stats['total_accesses']}")
        print(f"   ACCESSES edges created: {stats['resolved_edges']}")
        print(f"   Unresolved accesses: {stats['unresolved_accesses']}")

        return stats

    def analyze_all_functions(self, clear_existing: bool = False, batch_size: int = 100) -> dict:
        """Main entry point: Analyze all functions and create ACCESSES edges

        Args:
            clear_existing: Delete existing ACCESSES edges before starting
            batch_size: Commit every N functions (default 100) for better performance
        """
        print("\n[Stage 1.5] Analyzing field accesses...")

        if clear_existing:
            self.conn.execute("DELETE FROM symbol_edges WHERE edge_type = 'ACCESSES'")
            self.conn.commit()

        # Get all functions from database
        functions = self._get_functions()
        print(f"   Found {len(functions)} functions to analyze")

        total_accesses = 0
        resolved_edges = 0
        unresolved_accesses = 0

        # Analyze each function with batch commits
        with tqdm(total=len(functions), desc="Analyzing functions", unit="func") as pbar:
            for i, func in enumerate(functions):
                try:
                    body_lines = self._read_function_body(func['file_path'], func['line_number'])

                    if not body_lines:
                        pbar.update(1)
                        continue

                    accesses = self._extract_field_accesses(body_lines)
                    total_accesses += len(accesses)

                    if accesses:
                        resolved, unresolved = self._resolve_and_create_edges(
                            func['id'], accesses, func['file_path']
                        )
                        resolved_edges += resolved
                        unresolved_accesses += unresolved

                    # Batch commit every N functions for better performance
                    if (i + 1) % batch_size == 0:
                        self.conn.commit()

                except Exception as e:
                    print(f"\n   Error analyzing {func['name']}: {e}")

                finally:
                    pbar.update(1)

        # Final commit for remaining functions
        self.conn.commit()

        stats = {
            'total_functions': len(functions),
            'total_accesses': total_accesses,
            'resolved_edges': resolved_edges,
            'unresolved_accesses': unresolved_accesses
        }

        print(f"\nField access analysis complete:")
        print(f"   Functions analyzed: {stats['total_functions']}")
        print(f"   Field accesses found: {stats['total_accesses']}")
        print(f"   ACCESSES edges created: {stats['resolved_edges']}")
        print(f"   Unresolved accesses: {stats['unresolved_accesses']}")

        return stats
