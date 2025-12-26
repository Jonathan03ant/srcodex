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

    def get_children(self, path:str="") -> list:
        """Get children of a directory - TODO: implement next"""
        return []

    def search_file(self, file:str) -> list:
        """Search for files - TODO: implement"""
        return []

    def search_symbol_global(self, sylbol:str) -> list:
        """Search symbols globally - TODO: implement"""
        return []

    def search_sybole_infile(self, symbole:str, file:str) -> list:
        """Search symbols in file - TODO: implement"""
        return []