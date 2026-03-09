from textual.widgets import Static, Button
from textual.containers import Horizontal


class LeftTab(Horizontal):
    """Horizontal tab bar with icon buttons
        Controls how left side is viewed
    """
    DEFAULT_CSS = """
    TabBar {
        height: 3;
        width: 100%;
        background: $surface;
        border-bottom: solid $primary;
    }

    TabBar Button {
        margin: 0 1;
        min-width: 10;
    }
    """

    def compose(self):
        yield Button("📁 Explorer", id="tab-explorer", variant="primary")
        yield Button("🔍 Search", id="tab-search")