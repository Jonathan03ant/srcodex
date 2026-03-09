from textual.containers import Container
from textual.widgets import Static

class SearchView(Container):
    """Search View - File and sybole search
        Place Holder for now
    """

    DEFAULT_CSS = """
    SearchView {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    """

    def compose(self):
        yield Static("🔍 Search Panel\n(Coming soon)")