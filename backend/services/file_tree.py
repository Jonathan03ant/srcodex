"""
File Tree Service - Left Panel Navigation

Provides hierarchical file tree structure from the database for UI rendering.
Queries the 'files' table to build directory/file trees.
"""

import sqlite3
from typing import Dict, List, Optional
from pathlib import Path


class FileTreeService:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_root(self) -> Dict:
        """
        Get metadata about the project's logical root directory.

        Root = the directory that was indexed (anchor for all relative paths).

        Returns metadata only
        - Project ID (derived from db filename)
        - Display name
        - Physical path (where source was indexed from)
        - Logical path (always "" for root)
        - Statistics (total files, symbols, children count)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get project metadata from metadata table
            cursor.execute("SELECT key, value FROM metadata")
            metadata = {row['key']: row['value'] for row in cursor.fetchall()}

            # Count immediate children (top-level directories/files)
            cursor.execute("""
                SELECT COUNT(*) as count FROM (
                    SELECT DISTINCT
                        CASE
                            WHEN instr(path, '/') > 0
                            THEN substr(path, 1, instr(path, '/') - 1)
                            ELSE path
                        END as top_level
                    FROM files
                )
            """)
            children_count = cursor.fetchone()['count']
            project_id = Path(self.db_path).stem
            physical_path = metadata.get('source_root', '')
            display_name = Path(physical_path).name.upper() if physical_path else project_id.upper()

            return {
                "id": project_id,
                "path": "",  # Logical root is always empty string
                "physical_path": physical_path,
                "is_dir": True,
                "children_count": children_count,
                "total_files": int(metadata.get('total_files', 0)),
                "total_symbols": int(metadata.get('total_symbols', 0)),
                "indexed_at": metadata.get('indexed_at', '')
            }

    def get_children(self, path: str = "") -> List[Dict]:
        """
        Get immediate children of a directory.

        Input contract:
        - path == "" → root children

        Output contract:
        - Returns immediate children only (not recursive)
        - Each child includes:
          * name: basename
          * path: full relative path (dirs end with /)
          * kind: "file" or "dir"
          * children_count: (dirs only) number of immediate children
          * symbol_count: total symbols (files: direct, dirs: recursive)
          * size: (files only) file size in bytes

        Algorithm:
        1. Get all files matching prefix
        2. Strip prefix, take next segment up to /
        3. If segment has / after it → dir, else → file
        """
     
        if path and not path.endswith('/'):
            path = path + '/'

        with self._get_connection() as conn:
            cursor = conn.cursor()

            if path:

                pattern = f"{path}%"
                cursor.execute("""
                    SELECT path, size FROM files
                    WHERE path LIKE ?
                    ORDER BY path
                """, (pattern,))
            else:
                # Root: get all files
                cursor.execute("SELECT path, size FROM files ORDER BY path")

            all_files = cursor.fetchall()

            # Build immediate children
            children = {}  # Use dict to dedupe (key = child_path)

            for file_row in all_files:
                file_path = file_row['path']

                # Strip the parent path prefix
                if path:
                    if not file_path.startswith(path):
                        continue
                    relative = file_path[len(path):]
                else:
                    relative = file_path

                # Find next segment (up to first /)
                slash_pos = relative.find('/')

                if slash_pos == -1:
                    # No slash → immediate file child
                    child_name = relative
                    child_path = file_path

                    if child_path not in children:
                        children[child_path] = {
                            "name": child_name,
                            "path": child_path,
                            "kind": "file",
                            "size": file_row['size'],
                            "symbol_count": 0 
                        }
                else:
                    # Has slash → directory child
                    child_name = relative[:slash_pos]
                    child_path = path + child_name + '/'

                    if child_path not in children:
                        children[child_path] = {
                            "name": child_name,
                            "path": child_path,
                            "kind": "dir",
                            "children_count": 0,  
                            "symbol_count": 0
                        }

            # Now populate counts
            for child_path, child_data in children.items():
                if child_data["kind"] == "file":
                    # Count symbols in this file
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM symbols WHERE file_path = ?
                    """, (child_path,))
                    child_data["symbol_count"] = cursor.fetchone()['count']

                else:  # dir
                    # Count symbols recursively under this directory
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM symbols
                        WHERE file_path LIKE ?
                    """, (f"{child_path}%",))
                    child_data["symbol_count"] = cursor.fetchone()['count']

                    # Count immediate children of this directory
                    child_children = self._count_immediate_children(cursor, child_path)
                    child_data["children_count"] = child_children

            # Convert to list and sort: directories first, then alphabetically
            result = list(children.values())
            result.sort(key=lambda x: (x["kind"] == "file", x["name"]))

            return result

    def _count_immediate_children(self, cursor, parent_path: str) -> int:
        """Count immediate children of a directory (helper for get_children)"""
        cursor.execute("""
            SELECT path FROM files WHERE path LIKE ?
        """, (f"{parent_path}%",))

        files = cursor.fetchall()
        children = set()

        for row in files:
            file_path = row['path']
            relative = file_path[len(parent_path):]

            slash_pos = relative.find('/')
            if slash_pos == -1:
                children.add(relative)
            else:
                children.add(relative[:slash_pos])

        return len(children)

    def search_file(self, query: str) -> List[Dict]:
        """
        Search for files by name or path (like Ctrl+P in VSCode).

        Fuzzy matching:
        - "pow" → matches "power.c", "power.h", "mp1/src/app/power.c"
        - "app/pow" → matches "mp1/src/app/power.c"
        - Case-insensitive

        Returns ranked results (best matches first):
        - Exact filename match (highest priority)
        - Filename contains query
        - Path contains query
        - Shorter paths ranked higher

        Each result includes:
        - name: filename only
        - path: full relative path
        - size: file size
        - symbol_count: number of symbols
        """
        if not query:
            return []

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Search pattern (case-insensitive)
            pattern = f"%{query}%"

            cursor.execute("""
                SELECT
                    f.path,
                    f.size,
                    COUNT(s.id) as symbol_count
                FROM files f
                LEFT JOIN symbols s ON f.path = s.file_path
                WHERE f.path LIKE ? COLLATE NOCASE
                GROUP BY f.path
                ORDER BY LENGTH(f.path), f.path
                LIMIT 100
            """, (pattern,))

            results = []
            for row in cursor.fetchall():
                file_path = row['path']
                filename = Path(file_path).name

                # Calculate match score for ranking
                score = self._calculate_match_score(file_path, filename, query.lower())

                results.append({
                    "name": filename,
                    "path": file_path,
                    "size": row['size'],
                    "symbol_count": row['symbol_count'],
                    "score": score
                })

            # Sort by score (higher = better match)
            results.sort(key=lambda x: (-x['score'], len(x['path']), x['path']))

            # Remove score from output
            for r in results:
                del r['score']

            return results

    def _calculate_match_score(self, path: str, filename: str, query: str) -> int:
        """
        Calculate match score for ranking search results.
        Higher score = better match.
        """
        path_lower = path.lower()
        filename_lower = filename.lower()

        score = 0

        # Exact filename match (highest priority)
        if filename_lower == query:
            score += 1000

        # Filename starts with query
        if filename_lower.startswith(query):
            score += 500

        # Filename contains query
        if query in filename_lower:
            score += 100

        # Path contains query (lower priority)
        if query in path_lower:
            score += 10

        # Bonus: shorter paths are better
        score -= len(path) // 10

        return score

    def search_symbol_global(self, query: str) -> List[Dict]:
        """
        Global search across everything (like Ctrl+Shift+F in VSCode).

        Searches:
        - Symbol names (functions, structs, macros, variables, enums, etc.)
        - File paths
        - Signatures

        Uses FTS5 for fast full-text search with ranking.

        Returns ranked results with:
        - id: symbol ID
        - name: symbol name
        - type: symbol type (function, struct, macro, etc.)
        - file_path: where it's defined
        - line_number: line number
        - signature: function signature (if applicable)
        - match_context: what matched (name, file, signature)
        """
        if not query:
            return []

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Use FTS5 for fast full-text search across name, signature, and file_path
            # FTS5 automatically ranks results by relevance
            # Escape FTS5 special characters: " - ( ) * to prevent syntax errors
            fts_query = self._escape_fts5_query(query)

            cursor.execute("""
                SELECT
                    s.id,
                    s.name,
                    s.type,
                    s.file_path,
                    s.line_number,
                    s.signature,
                    s.scope_kind,
                    s.scope_name
                FROM symbols_fts sf
                JOIN symbols s ON sf.rowid = s.id
                WHERE symbols_fts MATCH ?
                ORDER BY rank
                LIMIT 100
            """, (fts_query,))

            results = []
            for row in cursor.fetchall():
                # Determine what matched (for context)
                match_context = self._get_match_context(
                    row['name'],
                    row['file_path'],
                    row['signature'],
                    query.lower()
                )

                result = {
                    "id": row['id'],
                    "name": row['name'],
                    "type": row['type'],
                    "file_path": row['file_path'],
                    "line_number": row['line_number'],
                    "signature": row['signature'] or "",
                    "scope": self._format_scope(row['scope_kind'], row['scope_name']),
                    "match_context": match_context
                }

                results.append(result)

            return results

    def _get_match_context(self, name: str, file_path: str, signature: str, query: str) -> str:
        """Determine what part of the symbol matched the query"""
        name_lower = name.lower()
        file_lower = file_path.lower()
        sig_lower = (signature or "").lower()

        if query in name_lower:
            return "name"
        elif signature and query in sig_lower:
            return "signature"
        elif query in file_lower:
            return "file"
        else:
            return "fuzzy"

    def _format_scope(self, scope_kind: str, scope_name: str) -> str:
        """Format scope for display (e.g., 'struct PowerState')"""
        if scope_kind and scope_name:
            return f"{scope_kind} {scope_name}"
        elif scope_name:
            return scope_name
        else:
            return ""

    def _escape_fts5_query(self, query: str) -> str:
        """
        Escape FTS5 special characters to prevent syntax errors.
        FTS5 special chars: " - ( ) *
        We wrap the query in quotes to treat it as a phrase.
        """
        # Remove/escape problematic characters
        escaped = query.replace('"', '""')  # Escape double quotes
        # Wrap in quotes to treat as literal phrase
        return f'"{escaped}"'

    def search_symbol_infile(self, query: str, file_path: str) -> List[Dict]:
        """
        Search symbols within a specific file (like Ctrl+F in VSCode on open file).

        Use case:
        - User has opened "mp1/src/app/msg.c"
        - User presses Ctrl+F and searches "Isr"
        - Returns all symbols matching "Isr" in that file only

        Args:
            query: Search term (e.g., "Isr", "Message", "init")
            file_path: File to search in (e.g., "mp1/src/app/msg.c")

        Returns:
            List of symbols in the file matching query, ordered by line number:
            - id: symbol ID
            - name: symbol name
            - type: symbol type
            - line_number: where it's defined
            - signature: function signature (if applicable)
            - scope: parent scope (if any)
        """
        if not query or not file_path:
            return []

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Search for symbols in specific file (case-insensitive partial match)
            pattern = f"%{query}%"

            cursor.execute("""
                SELECT
                    id,
                    name,
                    type,
                    file_path,
                    line_number,
                    signature,
                    scope_kind,
                    scope_name
                FROM symbols
                WHERE file_path = ?
                  AND (name LIKE ? OR signature LIKE ?)
                ORDER BY line_number
            """, (file_path, pattern, pattern))

            results = []
            for row in cursor.fetchall():
                # Determine what matched
                name_match = query.lower() in row['name'].lower()
                sig_match = row['signature'] and query.lower() in row['signature'].lower()

                match_in = "name" if name_match else "signature" if sig_match else "unknown"

                result = {
                    "id": row['id'],
                    "name": row['name'],
                    "type": row['type'],
                    "line_number": row['line_number'],
                    "signature": row['signature'] or "",
                    "scope": self._format_scope(row['scope_kind'], row['scope_name']),
                    "match_in": match_in
                }

                results.append(result)

            return results