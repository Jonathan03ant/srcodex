from textual.containers import VerticalScroll
from textual.widgets import Button
from .left_tab import LeftTab
from .explorer_view import ExplorerView
from .search_view import SearchView

class SidePanel(VerticalScroll):
    """Left Side Panel with tabs and switchable views"""

    DEFAULT_CSS = """
    Sidebar {
        width: 100%;
        height: 100%;
    }
    """

    def __init__(self, root_path: str, **kwargs):
        super().__init__(**kwargs)
        self.root_path = root_path
        self.current_view = "explorer"

    def compose(self):
        yield LeftTab()
        yield ExplorerView(self.root_path, id="explorer-view")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle Left Tab Button Pressed"""
        button_id = event.button.id

        if button_id == "tab-explorer":
            self.switch_to_explorer()
        elif button_id == "tab-search":
            self.switch_to_search()

    def switch_to_explorer(self):
        """Switch to explorer view"""
        if self.current_view != "explorer":
            #remove curr view
            self.query_one("#search-view").remove()
            # Mount explorer view
            self.mount(ExplorerView(self.root_path, id="explorer-view"))
            self.current_view = "explorer"

    def switch_to_search(self):
        """Switch to search view"""
        if self.current_view != "search":
            # Remove current view
            self.query_one("#explorer-view").remove()
            # Mount search view
            self.mount(SearchView(id="search-view"))
            self.current_view = "search"