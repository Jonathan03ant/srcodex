#!/usr/bin/env python3
"""
Explorer - Unified File Discovery Module
Used by both indexer and cscope to ensure consistent file sets

This module provides FileDiscovery class for finding source files
with consistent filtering rules across all tools.
"""

from pathlib import Path
from typing import List, Set


# Default directories to ignore during file discovery
DEFAULT_IGNORE_DIRS = {
    '.git',
    '__pycache__',
    'out',
    'build',
    'dist',
    '.pytest_cache',
    'node_modules',
    '.venv',
    'venv'
}


class FileDiscovery:
    """
    Discovers source files in a directory with consistent filtering

    CRITICAL: Both indexer and cscope MUST use this same discovery logic
    to ensure they index the exact same set of files.
    """

    def __init__(
        self,
        source_root: str,
        extensions: List[str] = None,
        ignore_dirs: Set[str] = None
    ):
        """
        Args:
            source_root: Root directory to scan
            extensions: File extensions to include (default: ['.c', '.h'])
            ignore_dirs: Directory names to skip (default: DEFAULT_IGNORE_DIRS)
        """
        self.source_root = Path(source_root).resolve()
        self.extensions = extensions or ['.c', '.h']
        self.ignore_dirs = ignore_dirs or DEFAULT_IGNORE_DIRS

        if not self.source_root.exists():
            raise FileNotFoundError(f"Directory not found: {source_root}")

        if not self.source_root.is_dir():
            raise NotADirectoryError(f"Not a directory: {source_root}")

    def discover_files(self) -> List[str]:
        """
        Find all files matching extensions, with ignore filters

        Returns:
            List of POSIX-formatted relative paths from source_root
            Example: ['power.c', 'drivers/thermal.c', 'include/power.h']
        """
        files = []

        for ext in self.extensions:
            for file_path in self.source_root.rglob(f'*{ext}'):
                if self._should_ignore(file_path):
                    continue
                # Convert to relative POSIX path
                rel_path = file_path.relative_to(self.source_root)
                files.append(rel_path.as_posix())

        return sorted(files)

    def discover_files_absolute(self) -> List[Path]:
        """
        Find all files matching extensions, with ignore filters

        Returns:
            List of absolute Path objects
        """
        files = []

        for ext in self.extensions:
            for file_path in self.source_root.rglob(f'*{ext}'):
                if self._should_ignore(file_path):
                    continue

                files.append(file_path)

        return sorted(files)

    def _should_ignore(self, file_path: Path) -> bool:
        """
            True if file should be ignored, False otherwise
        """
        for part in file_path.parts:
            if part in self.ignore_dirs:
                return True

        return False

    def get_stats(self) -> dict:
        """
        Returns:
            Dictionary with file counts and extensions
        """
        files = self.discover_files()

        # Count by extension
        ext_counts = {}
        for file_path in files:
            ext = Path(file_path).suffix
            ext_counts[ext] = ext_counts.get(ext, 0) + 1

        return {
            'total_files': len(files),
            'extensions': ext_counts,
            'source_root': str(self.source_root)
        }


# Convenience function for quick usage
def discover_files(source_root: str, extensions: List[str] = None) -> List[str]:
    discovery = FileDiscovery(source_root, extensions)
    return discovery.discover_files()
