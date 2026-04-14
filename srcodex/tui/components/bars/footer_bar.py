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

    Left: version, project name, database name
    Right: persistent token usage (lifetime stats, never resets)
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

    #footer-spacer {
        width: 1fr;
    }
    """

    def __init__(self, project_root: str = None, **kwargs):
        super().__init__(**kwargs)
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.query_count = 0
        self.total_user_input = 0
        self.total_output = 0
        self.savings_percentage = 0.0

    def compose(self):
        # Get version from package metadata
        try:
            version = importlib.metadata.version("srcodex")
        except Exception:
            version = "0.3.0"

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

        # Load token stats from session
        self.load_stats()

        # Left side: version, project, database
        yield FooterItem(f"v{version}", "footer-version", id="footer-version")
        yield FooterItem(f"🖿 {project_name}", "footer-project", id="footer-project")
        yield FooterItem(f"☰ {db_name}", "footer-database", id="footer-database")

        # Spacer (pushes token stats to the right)
        yield FooterItem("", "footer-spacer", id="footer-spacer")

        # Right side: token stats (persistent)
        yield FooterItem(self._format_stats(), "footer-tokens", id="footer-tokens")

    def load_stats(self):
        """Load persistent stats from conversation metadata"""
        metadata_path = self.project_root / ".srcodex" / "conversations" / "latest.json"
        try:
            with open(metadata_path, 'r') as f:
                session_data = json.load(f)
                metadata = session_data.get("metadata", {})
                self.query_count = metadata.get("query_count", 0)
                self.total_user_input = metadata.get("total_user_input", 0)
                self.total_output = metadata.get("total_output", 0)
                self.savings_percentage = metadata.get("total_savings_percentage", 0.0)
        except Exception:
            pass

    def update_stats(self, query_count, total_user_input, total_output, savings_percentage):
        """Update footer token stats"""
        self.query_count = query_count
        self.total_user_input = total_user_input
        self.total_output = total_output
        self.savings_percentage = savings_percentage

        # Update footer display (right side only)
        footer_item = self.query_one("#footer-tokens", FooterItem)
        footer_item.update(self._format_stats())

    def _format_stats(self):
        """Format stats string"""
        # Format tokens (12500 → 12K, 125000 → 125K)
        def format_tokens(tokens):
            if tokens >= 1000:
                return f"{tokens // 1000}K"
            return str(tokens)

        return (
            f"{self.query_count} queries 🪙"
            f"{format_tokens(self.total_user_input)} input 🪙"
            f"{format_tokens(self.total_output)} output "
            f"({self.savings_percentage:.0f}% savings)"
        )
