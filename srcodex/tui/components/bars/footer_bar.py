from textual.widgets import Static
from textual.containers import Horizontal


class FooterItem(Static):
    """Individual item in the footer bar"""
    def __init__(self, label: str, item_id: str, **kwargs):
        super().__init__(label, **kwargs)
        self.item_id = item_id


class FooterBar(Horizontal):
    """Footer bar spanning full width at bottom of screen

    Matches design of LeftTab, ChatHeader, and CodeTabBar.
    Will be populated with status indicators, metrics, etc.
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

    def compose(self):
        # Placeholder items - will be populated later
        yield FooterItem("srcodex v0.2.0", "footer-version", id="footer-version")
