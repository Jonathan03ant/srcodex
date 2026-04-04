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
    