"""
File Access Tools for Claude
Provides local filesystem access to Claude via tool calling
"""
import os
from pathlib import Path
from typing import Dict, List, Any
from .config_loader import get_config


# Load project configuration
config = get_config()
SOURCE_ROOT = config.source_root


def read_file(file_path: str):
    """
    Read contents of a local file
    Args: file_path: Relative path from source root or absolute path
    Returns: Dict with file content or error
    """
    try:
        if not Path(file_path).is_absolute():
            full_path = SOURCE_ROOT / file_path
        else:
            full_path = Path(file_path)

        # ensure path is within source root
        if not str(full_path.resolve()).startswith(str(SOURCE_ROOT.resolve())):
            return {
                "error": "Access denied - path outside project directory",
                "path": file_path
            }

        # Block read_file on indexed code files
        code_extensions = {'.c', '.h', '.cpp', '.hpp', '.cc', '.cxx', '.py', '.js', '.ts', '.java', '.go', '.rs'}
        if full_path.suffix in code_extensions:
            return {
                "error": f"🚫 read_file() blocked on code files ({full_path.suffix}). Use get_symbols_from_file() for 10-100x better token efficiency!",
                "path": str(full_path),
                "suggestion": f"Try: get_symbols_from_file('{file_path}', include_definitions=False) to see symbols, then get_symbol_definition() for specific ones.",
                "why_blocked": "Reading entire files wastes thousands of tokens. The semantic graph has this indexed - use it!"
            }

        # Check if file exists
        if not full_path.exists():
            return {
                "error": "File not found",
                "path": str(full_path)
            }

        if not full_path.is_file():
            return {
                "error": "Path is a directory, not a file",
                "path": str(full_path)
            }

        # Read File
        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        # Get file stats
        stat = full_path.stat()

        return {
            "path": str(full_path.relative_to(SOURCE_ROOT)),
            "content": content,
            "size_bytes": stat.st_size,
            "lines": len(content.splitlines())
        }

    except Exception as e:
        return {
            "error": str(e),
            "path": file_path
    }


def list_directory(dir_path: str=""):
    """
    List contents of a directory
    Args: dir_path:  Relative path from source root; Empty string for source root
    Returns: Dict with directory listing or error
    """
    try:
        # Convert to absolute path (paths are relative to SOURCE_ROOT, not PROJECT_ROOT)
        if not dir_path:
            full_path = SOURCE_ROOT
        elif not Path(dir_path).is_absolute():
            full_path = SOURCE_ROOT / dir_path
        else:
            full_path = Path(dir_path)

        # Security check (must be within SOURCE_ROOT)
        if not str(full_path.resolve()).startswith(str(SOURCE_ROOT.resolve())):
            return {
                "error": "Access denied - path outside source directory",
                "path": dir_path
            }

        # Check if directory exists
        if not full_path.exists():
            return {
                "error": "Directory not found",
                "path": str(full_path)
            }

        if not full_path.is_dir():
            return {
                "error": "Path is a file, not a directory",
                "path": str(full_path)
            }

        # List directory contents (max 100)
        entries = []
        all_items = sorted(full_path.iterdir())
        total_count = len(all_items)

        for item in all_items[:100]:
            entry = {
                "name": item.name,
                "type": "directory" if item.is_dir() else "file",
                "path": str(item.relative_to(SOURCE_ROOT))
            }
            # Add size for files
            if item.is_file():
                entry["size_bytes"] = item.stat().st_size
            entries.append(entry)

        result = {
            "path": str(full_path.relative_to(SOURCE_ROOT)) if dir_path else ".",
            "entries": entries,
            "count": len(entries),
            "total_count": total_count
        }

        if total_count > 100:
            result["truncated"] = True
            result["message"] = f"Showing first 100 of {total_count} entries. Use search_files or get_file_by_pattern for specific files."

        return result

    except Exception as e:
        return {
            "error": str(e),
            "path": dir_path
        }


def search_files(pattern: str, search_path: str = ""):
    """
    Search for files matching a pattern (glob-style)
    Args:
        pattern: Glob pattern (e.g., "*.c", "**/*.h", "main*")
        search_path: Directory to search in (relative to source root, e.g., 'firmware/main/mp1')
                    Empty string searches entire source tree
    Returns:  Dict with matching files or error
    """
    try:
        # Convert to absolute path (relative to SOURCE_ROOT)
        if not search_path:
            full_path = SOURCE_ROOT
        elif not Path(search_path).is_absolute():
            full_path = SOURCE_ROOT / search_path
        else:
            full_path = Path(search_path)

        # Security check
        if not str(full_path.resolve()).startswith(str(SOURCE_ROOT.resolve())):
            return {
                "error": "Access denied - path outside project directory",
                "path": search_path
            }

        if not full_path.exists():
            return {
                "error": "Search path not found",
                "path": str(full_path)
            }

        # Search for matching files
        matches = []
        for match in full_path.glob(pattern):
            if match.is_file():
                matches.append({
                    "name": match.name,
                    "path": str(match.relative_to(SOURCE_ROOT)),
                    "size_bytes": match.stat().st_size
                })

        # Sort by path
        matches.sort(key=lambda x: x["path"])

        return {
            "pattern": pattern,
            "search_path": str(full_path.relative_to(SOURCE_ROOT)) if search_path else ".",
            "matches": matches,
            "count": len(matches)
        }

    except Exception as e:
        return {
            "error": str(e),
            "pattern": pattern,
            "search_path": search_path
        }
# Tool definitions for Claude API
TOOL_DEFINITIONS = [
    {
        "name": "read_file",
        "description": "Read the contents of a file. WARNING: Do NOT use this on .c/.h files - they are indexed in the database. For code files, use get_symbols_from_file() instead (10-100x cheaper). Only use read_file for non-code files like .md, .txt, .json, .toml.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Relative path to the file from project root (e.g., 'pmfw_source/mp1/main.c')"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "list_directory",
        "description": "List files and directories in a given directory (max 100 entries). For large directories, returns first 100 and sets truncated=true. Use search_files or get_file_by_pattern if you need specific files.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dir_path": {
                    "type": "string",
                    "description": "Relative path to directory from project root. Use empty string or '.' for project root."
                }
            },
            "required": ["dir_path"]
        }
    },
    {
        "name": "search_files",
        "description": "Search for files matching a glob pattern (e.g., '*.c' for all C files, '**/*.h' for all headers recursively). Use this to find specific types of files.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern to match (e.g., '*.c', '**/*.py', 'main*')"
                },
                "search_path": {
                    "type": "string",
                    "description": "Directory to search within (relative to project root). Use empty string to search entire project."
                }
            },
            "required": ["pattern"]
        }
    }
]

def execute_tool(tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a file tool by name
    Args:
        tool_name: Name of the tool to execute
        tool_input: Input parameters for the tool
    Returns:
        Tool execution result
    """
    if tool_name == "read_file":
        return read_file(tool_input.get("file_path", ""))

    elif tool_name == "list_directory":
        return list_directory(tool_input.get("dir_path", ""))

    elif tool_name == "search_files":
        return search_files(
            pattern=tool_input.get("pattern", "*"),
            search_path=tool_input.get("search_path", "")
        )

    else:
        return {"error": f"Unknown tool: {tool_name}"}