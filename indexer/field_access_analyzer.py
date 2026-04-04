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
