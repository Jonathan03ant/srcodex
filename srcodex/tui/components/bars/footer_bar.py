from textual.widgets import Static
from textual.containers import Horizontal
from pathlib import Path
import json
import importlib.metadata


class FooterItem(Static):
    """Individual item in the footer bar"""
    def __init__(self, label: str, item_id: str, **kwargs):
        super().__init__(label, **kwargs)
        self.item_id = item_id


class FooterBar(Horizontal):
    """Footer bar spanning full width at bottom of screen

    Matches design of LeftTab, ChatHeader, and CodeTabBar.
    Displays: version, project name, database name
    """

    DEFAULT_CSS = """
    FooterBar {
        height: 1;
        width: 100%;
        align: left middle;
        dock: bottom;
    }

    FooterItem {
        width: auto;
        height: 1;
        background: transparent;
        padding: 0 1;
    }

    FooterItem:hover {
        background: transparent;
    }
    """

    def __init__(self, project_root: str = None, **kwargs):
        super().__init__(**kwargs)
        self.project_root = Path(project_root) if project_root else Path.cwd()

    def compose(self):
        # Get version from package metadata
        try:
            version = importlib.metadata.version("srcodex")
        except Exception:
            version = "0.1.0"

        # Get project name and database name from metadata.json
        metadata_path = self.project_root / ".srcodex" / "metadata.json"
        project_name = self.project_root.name
        db_name = "unknown.db"

        try:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                project_name = metadata.get("project", {}).get("name", project_name)
                db_path = metadata.get("paths", {}).get("database", "")
                if db_path:
                    db_name = Path(db_path).name
        except Exception:
            pass

        # Layout: v0.2.0  🖿 project_name  ☰ database_name
        yield FooterItem(f"v{version}", "footer-version", id="footer-version")
        yield FooterItem(f"🖿 {project_name}", "footer-project", id="footer-project")
        yield FooterItem(f"☰{db_name}", "footer-database", id="footer-database")
