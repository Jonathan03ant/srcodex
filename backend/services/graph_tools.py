"""
Semantic Graph Query Tools
These tools expose the semantic graph database to Claude,
Enables graph tools to query codebase database without reading entire source files.

Token savings: 50-200x for semantic queries vs file reading.
"""
import sqlite3
from typing import List, Dict, Optional
from pathlib import Path

class GraphTools:
    """Database query tools for exploring semnatic graph"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row


    def get_callers(self, function_name: str):
        """
        Find all functions that call the given function. (answwers "waht calls FunctionX)
        Args:
            function_name: Name of function to find callers for
        Returns:
            List of caller symbols with metadata:
            [
                {
                    'name': 'functionCaller',
                    'type': 'function',
                    'file_path': 'file_path',
                    'line_number': 156,
                    'signature': '(void)',
                    'call_site_line': 162
                },
                ...
            ]
        """
        cursor = self.conn.cursor()

        # Step 1: Find Target function's symbole ID(s)
        cursor.execute("""
            SELECT id FROM symbols
            WHERE name = ? AND type = 'function'
        """ (function_name))
        target_ids = [row['id'] for row in cursor.fetchall()]

        if not target_ids:
            return []

        #step 2: Find all CALL edges pointing to this functions
        placeholders = ','.join('?' * len(target_ids))
        cursor.execute(f"""
            SELECT src_symbol_id, line_number as call_site_line
            FROM symbol_edges
            WHERE edge_type = 'CALLS'
            AND dst_symbol_id IN ({placeholders})
        """, target_ids)

        edges = cursor.fetchall()
        if not edges:
            return []

        # Step3: Get caller symbole details
        caller_ids = [edge['src_symbole_id'] for edge in edges]
        cursor.execute(f"""
            SELECT id, name, type, file_path, line_number, signature
            FROM symbols
            WHERE id IN ({placeholders})
        """, caller_ids)

        callers = cursor.fetchall()

        # Step 4: Merge caller info with call site line numbers
        # Create a map: symbol_id -> call_site_line
        call_sites = {edge['src_symbol_id']: edge['call_site_line'] for edge in edges}

        results = []
        for caller in callers:
            results.append({
                'name': caller['name'],
                'type': caller['type'],
                'file_path': caller['file_path'],
                'line_number': caller['line_number'],
                'signature': caller['signature'] or '',
                'call_site_line': call_sites.get(caller['id'])
            })

        return results