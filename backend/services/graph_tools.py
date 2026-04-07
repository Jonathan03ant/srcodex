"""
Semantic Graph Query Tools
These tools expose the semantic graph database to Claude,
Enables graph tools to query codebase database without reading entire source files.

Token savings: 50-200x for semantic queries vs file reading.
"""
import sqlite3
from typing import List, Dict, Optional, Any
from pathlib import Path

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
        placeholders = ','.join('?' * len(callee_ids))
        cursor.execute(f"""
            SELECT id, name, type, file_path, line_number, signature
            FROM symbols
            WHERE id IN ({placeholders})
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
    }
]