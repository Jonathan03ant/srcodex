from textual.widgets import Input, Static
from textual.containers import Horizontal
from textual.message import Message

class FindBox(Horizontal):
    """Small search box for in-file find (Ctrl+F style)"""

    class FindNext(Message):
        """Posted when user searches"""
        def __init__(self, query: str):
            super().__init__()
            self.query = query

    DEFAULT_CSS = """
    FindBox {
        width: 40;
        height: auto;
        background: transparent;
        padding: 0 0;
    }

    FindBox Input {
        margin: 0;
        height: auto;
        min-height: 3;
    }

    FindBox Input:focus {
        margin: 0;
        height: auto;
        min-height: 3;
    }

    FindBox Static {
        width: auto;
        height: auto;
        color: $text-muted;
        padding: 0 1;
        offset: -10 0;
    }
    """

    def compose(self):
        yield Input(placeholder="Find...", id="find-input")
        yield Static("", id="match-count")

    def on_mount(self):
        """Focus input when mounted"""
        self.query_one("#find-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted):
        """Handle Enter key"""
        query = event.value.strip()
        if query:
            self.post_message(self.FindNext(query))

    def on_input_changed(self, event: Input.Changed):
        """Search as you type"""
        query = event.value.strip()
        if len(query) >= 1:
            self.post_message(self.FindNext(query))

    def update_match_count(self, current: int, total: int):
        """Update the match counter display"""
        if total == 0:
            self.query_one("#match-count", Static).update("")
        else:
            self.query_one("#match-count", Static).update(f"{current}/{total}")
