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
    pass

def search_files(pattern: str, search_path: str = ""):
    pass