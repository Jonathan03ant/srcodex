"""
File Access Tools for Claude
Provides local filesystem access to Claude via tool calling
"""
import os
from pathlib import Path
from typing import Dict, List, Any


# TODO (hardcoded project rule for demo)
PROJECT_ROOT = Path("/utg/pmfwex")


def read_file(file_path: str):
    """
    Read contents of a local file
    Args: file_path: Relative path from project root or absolute path
    Returns: Dict with file content or error
    """
    try:
        if not Path(file_path).is_absolute():
            full_path = PROJECT_ROOT / file_path
        else:
            full_path = Path(file_path)

        # ensure path is within project
        if not str(full_path.resolve()).startswith(str(PROJECT_ROOT.resolve*())):
            return {
                "error": "Access denied - path outside project directory",
                "path": file_path
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
            "path": str(full_path.relative_to(PROJECT_ROOT)),
            "content": content,
            "size_bytes": stat.st_size,
            "lines": len(content.splitlines())
        }

    except Exception as e:
        return {
        "error": str(e),
        "path": file_path
    }


def list_directories(dir_path: str=""):
    """
    List contents of a directory
    Args: dir_path:  Relative path from project root or Empty string for project root
    Returns: Dict with directory listing or error
    """
    try:
        # Convert to absolute path
        if not dir_path:
            full_path = PROJECT_ROOT
        elif not Path(dir_path).is_absolute():
            full_path = PROJECT_ROOT / dir_path
        else:
            full_path = Path(dir_path)

        # Security check
        if not str(full_path.resolve()).startswith(str(PROJECT_ROOT.resolve())):
            return {
                "error": "Access denied - path outside project directory",
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

        # List directory contents
        entries = []
        for item in sorted(full_path.iterdir()):
            entry = {
                "name": item.name,
                "type": "directory" if item.is_dir() else "file",
                "path": str(item.relative_to(PROJECT_ROOT))
            }
            # Add size for files
            if item.is_file():
                entry["size_bytes"] = item.stat().st_size
            entries.append(entry)

        return {
            "path": str(full_path.relative_to(PROJECT_ROOT)) if dir_path else ".",
            "entries": entries,
            "count": len(entries)
        }

    except Exception as e:
        return {
            "error": str(e),
            "path": dir_path
        }


def search_files(pattern: str, search_path: str = ""):
    pass