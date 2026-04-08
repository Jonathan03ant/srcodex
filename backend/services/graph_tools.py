"""
Semantic Graph Query Tools
These tools expose the semantic graph database to Claude,
Enables graph tools to query codebase database without reading entire source files.

Token savings: 50-200x for semantic queries vs file reading.
"""
import sqlite3
from typing import List, Dict, Optional, Any
from pathlib import Path
import os

class GraphTools:
    """Database query tools for exploring semnatic graph"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row


    def get_callers(self, function_name: str):
        """
        Find all functions that call the given function.
        Answwers: "waht calls FunctionX
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
        """, (function_name,))
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
        caller_ids = [edge['src_symbol_id'] for edge in edges]
        caller_placeholders = ','.join('?' * len(caller_ids))
        cursor.execute(f"""
            SELECT id, name, type, file_path, line_number, signature
            FROM symbols
            WHERE id IN ({caller_placeholders})
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

    def get_callees(self, function_name: str):
        """
        Find all functions that the given function calls.
        Answers: "What does FunctionX call?"
        Args:
            function_name: Name of function to find callees for
        Returns:
            List of callee symbols with metadata
        """
        cursor = self.conn.cursor()

        #Step 1: Find target function
        cursor.execute("""
            SELECT id FROM symbols
            WHERE name = ? AND type = 'function'
        """, (function_name,))
        target_ids = [row['id'] for row in cursor.fetchall()]

        if not target_ids:
            return []

        #step 2: Find all CALL edges pointing to this functions
        placeholders = ','.join('?' * len(target_ids))
        cursor.execute(f"""
            SELECT dst_symbol_id, line_number as call_site_line
            FROM symbol_edges
            WHERE edge_type = 'CALLS'
            AND src_symbol_id IN ({placeholders})
        """, target_ids)

        edges = cursor.fetchall()
        if not edges:
            return []

        # Step 3: Get callee symbol details
        callee_ids = [edge['dst_symbol_id'] for edge in edges]
        callee_placeholders = ','.join('?' * len(callee_ids))
        cursor.execute(f"""
            SELECT id, name, type, file_path, line_number, signature
            FROM symbols
            WHERE id IN ({callee_placeholders})
        """, callee_ids)

        callees = cursor.fetchall()

        # Step 4: Merge callee info with call site line numbers
        call_sites = {edge['dst_symbol_id']: edge['call_site_line'] for edge in edges}

        results = []
        for callee in callees:
            results.append({
                'name': callee['name'],
                'type': callee['type'],
                'file_path': callee['file_path'],
                'line_number': callee['line_number'],
                'signature': callee['signature'] or '',
                'call_site_line': call_sites.get(callee['id'])
            })

        return results

    def get_call_chain(self, start_function: str, end_function: str, max_depth: int = 10):
        """
        Find call paths from start_function to end_function.
        Uses breadth-first search through the call graph.
        Answers: How does functionX eventually call functionY
        Args:
            start_function: Starting function name
            end_function: Target function name
            max_depth: Maximum hops to search (default: 5)

        Returns:
            List of call paths (each path is list of function names):
            [
                ['main', 'func1', 'func2'],
                ['main', 'func1', 'func2'],
                ...
            ]
        """
        cursor = self.conn.cursor()

        # Step 1: Get start and end function IDs
        cursor.execute("""
            SELECT id, name FROM symbols
            WHERE name IN (?, ?) AND type = 'function'
        """, (start_function, end_function))

        symbols = {row['name']: row['id'] for row in cursor.fetchall()}

        if start_function not in symbols or end_function not in symbols:
            return []  # One or both functions don't exist

        start_id = symbols[start_function]
        end_id = symbols[end_function]

        # Step 2: BFS to find all paths
        # Queue stores: (current_symbol_id, path_so_far)
        queue = [(start_id, [start_function])]
        found_paths = []
        visited = set()  # Track visited nodes to prevent cycles

        while queue and len(found_paths) < 10:  # Limit to 10 paths
            current_id, path = queue.pop(0)

            # Skip if we've already explored this node at this depth
            if (current_id, len(path)) in visited:
                continue
            visited.add((current_id, len(path)))

            # Stop if max depth reached
            if len(path) > max_depth:
                continue

            # Step 3: Get all callees from current function
            cursor.execute("""
                SELECT dst_symbol_id
                FROM symbol_edges
                WHERE edge_type = 'CALLS' AND src_symbol_id = ?
            """, (current_id,))

            callee_ids = [row['dst_symbol_id'] for row in cursor.fetchall()]

            # Step 4: Check each callee
            for callee_id in callee_ids:
                # Found the target!
                if callee_id == end_id:
                    found_paths.append(path + [end_function])
                    continue

                # Get callee name and add to queue
                cursor.execute("SELECT name FROM symbols WHERE id = ?", (callee_id,))
                callee_row = cursor.fetchone()
                if callee_row:
                    callee_name = callee_row['name']
                    # Avoid cycles - don't revisit functions already in this path
                    if callee_name not in path:
                        queue.append((callee_id, path + [callee_name]))

        return found_paths

    def execute_sql(self, query: str, params: tuple = ()):
        """
        Execute a custom SQL query on the semantic graph database.
        READ-ONLY fallback for complex queries not covered by other tools.
        """
        # Security: Only allow SELECT queries (read-only)
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed (read-only access)")

        # Block dangerous keywords
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        if any(keyword in query_upper for keyword in dangerous_keywords):
            raise ValueError(f"Query contains disallowed keyword: {query}")

        cursor = self.conn.cursor()
        cursor.execute(query, params)

        # Convert rows to dictionaries
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))

        return results


    def get_file_by_pattern(self, pattern: str):
        """
        Find files matching a name pattern (SQL LIKE syntax).
        Args:
            pattern: File name pattern (e.g., 'power.c', '%dpm%', 'thermal%')
        Returns:
            List of matching files with metadata:
        """
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT f.path, f.size, COUNT(s.id) as symbol_count
            FROM files f
            LEFT JOIN symbols s ON s.file_path = f.path
            WHERE f.path LIKE ?
            GROUP BY f.path
            ORDER BY f.path
            LIMIT 50
        """, (pattern,))

        results = []
        for row in cursor.fetchall():
            results.append({
                'file_path': row['path'],
                'size_bytes': row['size'],
                'symbol_count': row['symbol_count']
            })

        return results


    def get_file_info(self, file_path: str):
        """
        Get detailed information about a specific file.
        Args:
            file_path: Exact file path
        Returns:
            File metadata including symbol count
        """
        cursor = self.conn.cursor()

        # Get symbol count for this file
        cursor.execute("""
            SELECT COUNT(*) as symbol_count
            FROM symbols
            WHERE file_path = ?
        """, (file_path,))

        row = cursor.fetchone()
        if not row:
            return None

        return {
            'file_path': file_path,
            'symbol_count': row['symbol_count']
        }


    def search_symbols(self, pattern: str, symbol_type: str = None):
        """
        Search symbols by name pattern, optionally filtered by type.
        Args:
            pattern: Symbol name pattern (SQL LIKE syntax, e.g., '%DPM%', 'Init%')
            symbol_type: Optional filter ('function', 'struct', 'macro', 'variable', etc.)
        Returns:
            List of matching symbols with location info:
        """
        cursor = self.conn.cursor()

        if symbol_type:
            cursor.execute("""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE name LIKE ? AND type = ?
                ORDER BY name
                LIMIT 100
            """, (pattern, symbol_type))
        else:
            cursor.execute("""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE name LIKE ?
                ORDER BY type, name
                LIMIT 100
            """, (pattern,))

        results = []
        for row in cursor.fetchall():
            results.append({
                'name': row['name'],
                'type': row['type'],
                'file_path': row['file_path'],
                'line_number': row['line_number'],
                'signature': row['signature'] or ''
            })

        return results


    def get_symbol_definition(self, symbol_name: str, symbol_type: str = None, context_lines: int = 5):
        """
        Get ONLY the definition of a symbol (function body, struct, etc.) instead of the entire file.
        Args:
            symbol_name: Name of symbol to find
            symbol_type: Optional type filter ('function', 'struct', 'macro')
            context_lines: Number of lines before/after to include (default: 5)
        Returns:
            Symbol definition with surrounding context:
        """
        cursor = self.conn.cursor()

        # Find the symbol
        if symbol_type:
            cursor.execute("""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE name = ? AND type = ?
                LIMIT 1
            """, (symbol_name, symbol_type))
        else:
            cursor.execute("""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE name = ?
                LIMIT 1
            """, (symbol_name,))

        row = cursor.fetchone()
        if not row:
            return None

        # Read the file and extract the definition
        # HARDCODED: Source root (will fix with .srcodex/ later)
        source_root = Path("/utg/pmfwex/pmfw_source")
        file_path = source_root / row['file_path']

        if not file_path.exists():
            return {
                'error': f"File not found: {row['file_path']}",
                'name': row['name'],
                'type': row['type']
            }

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()

            # Extract definition based on symbol type
            start_line = max(0, row['line_number'] - 1 - context_lines)

            # For functions/structs, try to find the closing brace
            if row['type'] in ['function', 'struct']:
                end_line = self._find_closing_brace(lines, row['line_number'] - 1)
                if end_line:
                    end_line = min(len(lines), end_line + context_lines)
                else:
                    # Fallback: just grab context lines
                    end_line = min(len(lines), row['line_number'] + context_lines)
            else:
                # For macros, variables, etc - just grab surrounding lines
                end_line = min(len(lines), row['line_number'] + context_lines)

            definition_lines = lines[start_line:end_line]
            definition = ''.join(definition_lines)

            return {
                'name': row['name'],
                'type': row['type'],
                'file_path': row['file_path'],
                'line_number': row['line_number'],
                'signature': row['signature'] or '',
                'definition': definition,
                'start_line': start_line + 1,
                'end_line': end_line,
                'lines_returned': len(definition_lines)
            }

        except Exception as e:
            return {
                'error': str(e),
                'name': row['name'],
                'type': row['type']
            }


    def _find_closing_brace(self, lines: List[str], start_line: int) -> Optional[int]:
        """
        Find the closing brace for a function/struct starting at start_line.
        Simple brace matching
        """
        brace_count = 0
        found_opening = False

        for i in range(start_line, len(lines)):
            line = lines[i]

            for char in line:
                if char == '{':
                    brace_count += 1
                    found_opening = True
                elif char == '}':
                    brace_count -= 1
                    if found_opening and brace_count == 0:
                        return i

        return None  # Couldn't find closing brace

    def get_symbols_from_file(self, file_path: str, symbol_types: list = None, include_definitions: bool = False):
        """
        Get ALL symbols from a specific file (or filtered by type).
        Args:
            file_path: File path
            symbol_types: Optional filter list (e.g., ['struct', 'enum', 'macro'])
            include_definitions: If True, includes code snippets for each symbol (slower, more tokens)
        Returns:
            List of symbols with optional definitions:
            [
                {
                    'name': 'DpmManager_t',
                    'type': 'struct',
                    'line_number': 42,
                    'signature': '...',
                    'definition': '...' (if include_definitions=True)
                },
                ...
            ]
        """
        cursor = self.conn.cursor()

        # Build query based on filters
        if symbol_types:
            placeholders = ','.join('?' * len(symbol_types))
            cursor.execute(f"""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE file_path = ? AND type IN ({placeholders})
                ORDER BY line_number
            """, [file_path] + symbol_types)
        else:
            cursor.execute("""
                SELECT name, type, file_path, line_number, signature
                FROM symbols
                WHERE file_path = ?
                ORDER BY line_number
            """, (file_path,))

        results = []
        for row in cursor.fetchall():
            symbol_data = {
                'name': row['name'],
                'type': row['type'],
                'line_number': row['line_number'],
                'signature': row['signature'] or ''
            }

            # Optionally include definitions
            if include_definitions:
                definition_result = self.get_symbol_definition(row['name'], row['type'], context_lines=3)
                if definition_result and 'definition' in definition_result:
                    symbol_data['definition'] = definition_result['definition']
                    symbol_data['lines_returned'] = definition_result['lines_returned']

            results.append(symbol_data)

        return results

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


def execute_graph_tool(tool_name: str, tool_input: Dict[str, Any], db_path: str = "/utg/pmfwex/data/pmfw_main.db"):
        """
        Execute a graph tool by name
        Args:
            tool_name: Name of the tool to execute
            tool_input: Input parameters for the tool
            db_path: Path to the semantic graph database

        Returns:
            Tool execution result
        """
        graph = GraphTools(db_path)

        try:
            if tool_name == "get_callers":
                result = graph.get_callers(tool_input.get("function_name", ""))
                return {"callers": result, "count": len(result)}

            elif tool_name == "get_callees":
                result = graph.get_callees(tool_input.get("function_name", ""))
                return {"callees": result, "count": len(result)}

            elif tool_name == "get_call_chain":
                result = graph.get_call_chain(
                    start_function=tool_input.get("start_function", ""),
                    end_function=tool_input.get("end_function", ""),
                    max_depth=tool_input.get("max_depth", 5)
                )
                return {"paths": result, "count": len(result)}

            elif tool_name == "execute_sql":
                result = graph.execute_sql(
                    query=tool_input.get("query", ""),
                    params=tuple(tool_input.get("params", []))
                )
                return {"results": result, "count": len(result)}

            elif tool_name == "get_file_by_pattern":
                result = graph.get_file_by_pattern(tool_input.get("pattern", ""))
                return {"files": result, "count": len(result)}

            elif tool_name == "get_file_info":
                result = graph.get_file_info(tool_input.get("file_path", ""))
                return result if result else {"error": "File not found"}

            elif tool_name == "search_symbols":
                result = graph.search_symbols(
                    pattern=tool_input.get("pattern", ""),
                    symbol_type=tool_input.get("symbol_type")
                )
                return {"symbols": result, "count": len(result)}

            elif tool_name == "get_symbol_definition":
                result = graph.get_symbol_definition(
                    symbol_name=tool_input.get("symbol_name", ""),
                    symbol_type=tool_input.get("symbol_type"),
                    context_lines=tool_input.get("context_lines", 5)
                )
                return result if result else {"error": "Symbol not found"}

            elif tool_name == "get_symbols_from_file":
                result = graph.get_symbols_from_file(
                    file_path=tool_input.get("file_path", ""),
                    symbol_types=tool_input.get("symbol_types"),
                    include_definitions=tool_input.get("include_definitions", False)
                )
                return {"symbols": result, "count": len(result)}

            else:
                return {"error": f"Unknown graph tool: {tool_name}"}

        except Exception as e:
            return {"error": str(e)}

        finally:
            graph.close()


# Tool definitions for Claude API
TOOLS = [
    {
        "name": "get_callers",
        "description": "Find all functions that call a given function. Answers 'What calls FunctionX?' Use this to understand who depends on a function or to trace backwards through the call graph.",
        "input_schema": {
            "type": "object",
            "properties": {
                "function_name": {
                    "type": "string",
                    "description": "Name of the function to find callers for (e.g., 'EnableDldo')"
                }
            },
            "required": ["function_name"]
        }
    },
    {
        "name": "get_callees",
        "description": "Find all functions that a given function calls. Answers 'What does FunctionX call?' Use this to understand what a function does or to trace forward through the call graph.",
        "input_schema": {
            "type": "object",
            "properties": {
                "function_name": {
                    "type": "string",
                    "description": "Name of the function to find callees for (e.g., 'main')"
                }
            },
            "required": ["function_name"]
        }
    },
    {
        "name": "get_call_chain",
        "description": "Find execution paths from one function to another through the call graph. Answers 'How does A reach B?' Use this to trace complex execution flows or understand how one function eventually calls another.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_function": {
                    "type": "string",
                    "description": "Starting function name (e.g., 'main')"
                },
                "end_function": {
                    "type": "string",
                    "description": "Target function name (e.g., 'EnableDldo')"
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum number of hops to search (default: 5)",
                    "default": 5
                }
            },
            "required": ["start_function", "end_function"]
        }
    },
    {
        "name": "execute_sql",
        "description": "Execute a custom read-only SQL query on the semantic graph database. Use this for complex queries not covered by other tools (e.g., field access patterns, statistics, filtering by file/type). Only SELECT queries allowed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL SELECT query with ? placeholders for parameters"
                },
                "params": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Query parameters (optional, for ? placeholders)",
                    "default": []
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_file_by_pattern",
        "description": "Find files matching a name pattern. Use this to discover files without knowing exact paths. Much faster than filesystem search.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "SQL LIKE pattern (e.g., '%power%', 'dpm.c', 'thermal%')"
                }
            },
            "required": ["pattern"]
        }
    },
    {
        "name": "get_file_info",
        "description": "Get metadata about a specific file including symbol count.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Exact file path (e.g., 'mp1/src/app/power.c')"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "search_symbols",
        "description": "Search for symbols (functions, structs, macros) by name pattern. Faster than execute_sql for common symbol searches.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "SQL LIKE pattern (e.g., '%DPM%', 'Init%', '%handler')"
                },
                "symbol_type": {
                    "type": "string",
                    "description": "Optional filter: 'function', 'struct', 'macro', 'variable', 'enum', 'typedef'"
                }
            },
            "required": ["pattern"]
        }
    },
    {
        "name": "get_symbol_definition",
        "description": "Get ONLY the definition of a symbol (function body, struct definition, etc.) instead of reading the entire file. MASSIVE token savings - use this instead of read_file when you only need one symbol.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol_name": {
                    "type": "string",
                    "description": "Name of symbol to get definition for (e.g., 'InitPower', 'DpmManager_t')"
                },
                "symbol_type": {
                    "type": "string",
                    "description": "Optional type filter: 'function', 'struct', 'macro'"
                },
                "context_lines": {
                    "type": "integer",
                    "description": "Number of context lines before/after (default: 5)",
                    "default": 5
                }
            },
            "required": ["symbol_name"]
        }
    },
    {
        "name": "get_symbols_from_file",
        "description": "Get ALL symbols from a file (or filtered by type). Use this instead of read_file when you need multiple symbols from one file. IMPORTANT: Use two-step approach for best token efficiency: (1) Get metadata first (include_definitions=false) to see what symbols exist, (2) Then use get_symbol_definition() for only the specific symbols you need. Only set include_definitions=true if you genuinely need ALL symbol definitions from the file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "File path (e.g., 'mp1/src/app/dpm.h')"
                },
                "symbol_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional filter: ['struct', 'enum', 'macro', 'function'] - leave empty for all symbols"
                },
                "include_definitions": {
                    "type": "boolean",
                    "description": "WARNING: Setting this to true returns code for EVERY symbol, which can be 5-10x more tokens than the file itself. Default: false (metadata only). Only use true when you genuinely need all definitions. For selective definitions, use get_symbol_definition() instead.",
                    "default": False
                }
            },
            "required": ["file_path"]
        }
    }
]