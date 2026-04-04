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

    class NextMatch(Message):
        """Posted when user presses down arrow"""
        pass

    class PrevMatch(Message):
        """Posted when user presses up arrow"""
        pass

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
        """Handle Enter key - go to next match"""
        self.post_message(self.NextMatch())

    def on_input_changed(self, event: Input.Changed):
        """Search as you type"""
        query = event.value.strip()
        if len(query) >= 1:
            self.post_message(self.FindNext(query))

    def on_key(self, event):
        """Handle arrow keys for navigation"""
        if event.key == "down":
            self.post_message(self.NextMatch())
            event.prevent_default()
            event.stop()
        elif event.key == "up":
            self.post_message(self.PrevMatch())
            event.prevent_default()
            event.stop()

    def update_match_count(self, current: int, total: int):
        """Update the match counter display"""
        if total == 0:
            self.query_one("#match-count", Static).update("")
        else:
            self.query_one("#match-count", Static).update(f"{current}/{total}")
