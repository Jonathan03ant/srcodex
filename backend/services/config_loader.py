"""
Configuration Loader - Reads .srcodex/metadata.json
Provides project paths and stats to all services
"""
import json
from pathlib import Path
from typing import Dict, Any


class ProjectConfig:
    """Loads and provides access to .srcodex/metadata.json"""

    def __init__(self, project_root: Path = None):
        """
        Initialize config loader

        Args:
            project_root: Path to project root (defaults to current working directory)
        """
        if project_root is None:
            # Default to current working directory
            project_root = Path.cwd()

        self.project_root = Path(project_root)
        self.srcodex_dir = self.project_root / ".srcodex"
        self.metadata_file = self.srcodex_dir / "metadata.json"

        # Load metadata
        if not self.metadata_file.exists():
            raise FileNotFoundError(
                f"No .srcodex/metadata.json found in {self.project_root}\n"
                f"Run indexer first to generate .srcodex/ directory"
            )

        with open(self.metadata_file, 'r') as f:
            self.metadata = json.load(f)

    @property
    def project_name(self) -> str:
        """Get project name"""
        return self.metadata["project"]["name"]

    @property
    def source_root(self) -> Path:
        """Get absolute path to source root"""
        return self.project_root / self.metadata["paths"]["source_root"]

    @property
    def database_path(self) -> Path:
        """Get absolute path to database"""
        return self.project_root / self.metadata["paths"]["database"]

    @property
    def stats(self) -> Dict[str, Any]:
        """Get project statistics"""
        return self.metadata["stats"]

    @property
    def indexed_at(self) -> str:
        """Get indexing timestamp"""
        return self.metadata["project"]["indexed_at"]


# Global config instance (singleton pattern)
_config = None

def get_config(project_root: Path = None) -> ProjectConfig:
    """
    Get global project configuration (singleton)

    Args:
        project_root: Path to project root (only used on first call)

    Returns:
        ProjectConfig instance
    """
    global _config
    if _config is None:
        _config = ProjectConfig(project_root)
    return _config
