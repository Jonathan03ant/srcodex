import asyncio
from pathlib import Path
from textual.containers import VerticalScroll
from .file_browser import FileBroswer
from .left_tab import LeftTab
from .explorer_view import ExplorerView
from .search_view import SearchView

class SidePanel(VerticalScroll):
    """Left Side Panel with tabs and switchable views"""

    DEFAULT_CSS = """
    SidePanel {
        width: 100%;
        height: 100%;
    }

    .hidden {
        display: none;
    }
    """

    def __init__(self, root_path: str, **kwargs):
        super().__init__(**kwargs)
        self.root_path = root_path
        self.current_view = "explorer"

    def compose(self):
        yield LeftTab()
        yield ExplorerView(self.root_path, id="explorer-view")
        # Mount search view but hide it initially
        yield SearchView(id="search-view", classes="hidden")

    def on_left_tab_tab_clicked(self, event):
        if event.tab_id == "tab-explorer":
            self.switch_to_explorer()
        elif event.tab_id == "tab-search":
            self.switch_to_search()

    async def on_search_view_file_selected(self, event: SearchView.FileSelected):
        """
        handle when file is selectef from search results
            1. switch back to explorer view
            2. Navigate to the file in the tree
        """
        self.switch_to_explorer()

        # Wait for the explorer view to become visible and fully rendered
        await asyncio.sleep(0.5)
        # Force a refresh of the entire panel
        self.refresh(layout=True)
        await asyncio.sleep(0.1)

        file_path = event.file_path
        await self.navigate_to_file(file_path)

    def switch_to_explorer(self):
        """Switch to explorer view"""
        if self.current_view != "explorer":
            # Hide search, show explorer
            self.query_one("#search-view").add_class("hidden")
            self.query_one("#explorer-view").remove_class("hidden")
            self.current_view = "explorer"

            # Update button styles
            self.query_one("#tab-explorer").add_class("active")
            self.query_one("#tab-search").remove_class("active")

    def switch_to_search(self):
        """Switch to search view"""
        if self.current_view != "search":
            self.query_one("#explorer-view").add_class("hidden")
            self.query_one("#search-view").remove_class("hidden")
            self.current_view = "search"

            self.query_one("#tab-search").add_class("active")
            self.query_one("#tab-explorer").remove_class("active")

    async def navigate_to_file(self, file_path: str):
        """navigate to file in explorer tree - replicate manual user interaction"""
        explorer_view = self.query_one("#explorer-view", ExplorerView)
        file_browser = explorer_view.query_one(FileBroswer)

        # Walk the tree to find the node
        parts = Path(file_path).parts
        current_node = file_browser.root

        # Expand root first - trigger the actual expand event like a user click
        if not current_node.is_expanded:
            current_node.toggle()  # This posts the NodeExpanded message
            await file_browser.reload_node(current_node)

        for part in parts:
            found = False
            for child in current_node.children:
                child_label = str(child.label.plain) if hasattr(child.label, 'plain') else str(child.label)
                if child_label == part:
                    # Only expand if not already expanded
                    if child.allow_expand and not child.is_expanded:
                        child.toggle()
                        await file_browser.reload_node(child)
                        file_browser.refresh()
                        import asyncio
                        await asyncio.sleep(0.05)
                    current_node = child
                    found = True
                    break

            if not found:
                return
        # For files, trigger selection like a user click
        file_browser.select_node(current_node)
        file_browser.scroll_to_node(current_node)
        file_browser.focus()