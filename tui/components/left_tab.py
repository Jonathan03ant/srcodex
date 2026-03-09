from textual.widgets import Static
from textual.containers import Horizontal
from textual.message import Message


class TabButton(Static):
    def __init__(self, label: str, tab_id: str, active: bool = False, **kwargs):
        super().__init__(label, **kwargs)
        self.tab_id = tab_id
        if active:
            self.add_class("active")


class LeftTab(Horizontal):
    class TabClicked(Message):
        def __init__(self, tab_id: str):
            super().__init__()
            self.tab_id = tab_id

    DEFAULT_CSS = """
    LeftTab {
        height: 1;
        width: 100%;
        align: left middle;
        dock: top;
    }

    TabButton {
        width: auto;
        height: 1;
        background: transparent;
        padding: 0 1;
    }

    TabButton:hover {
        background: transparent;
    }

    TabButton.active {
        text-style: bold;
    }
    """

    def compose(self):
        yield TabButton("📁", "tab-explorer", active=True, id="tab-explorer")
        yield TabButton("🔍", "tab-search", id="tab-search")

    def on_click(self, event) -> None:
        clicked = event.widget
        if isinstance(clicked, TabButton):
            for tab in self.query(TabButton):
                tab.remove_class("active")
            clicked.add_class("active")
            self.post_message(self.TabClicked(clicked.tab_id))