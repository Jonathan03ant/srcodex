from pathlib import Path
from textual.app import App
from textual.widgets import Label, DirectoryTree
import asyncio
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from components.panels.side_panel import SidePanel
from components.panels.code_panel import CodePanel
from components.panels.chat_panel import ChatPanel
from components.views.search_view import SearchView



class SrcodexApp(App):
    CSS_PATH = "app.tcss"

    # Source root - shared by all componenets
    SOURCE_ROOT = "/utg/pmfwex/pmfw_source"
    def compose(self):
        yield SidePanel(self.SOURCE_ROOT, id="left")
        yield CodePanel(self.SOURCE_ROOT, id="middle")
        yield ChatPanel(id="right")

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        """Handle file selection from FileBrowser explorer view"""
        code_panel = self.query_one("#middle", CodePanel)

        absolute_path = Path(event.path)
        source_root = Path(self.SOURCE_ROOT)

        try:
            relative_path = absolute_path.relative_to(source_root)
            code_panel.open_file(str(relative_path))
        except ValueError:
            self.notify(f"Cannot open file outside source root: {event.path}", severity="error")

    async def on_search_view_symbol_selected(self, event: SearchView.SymbolSelected):
        """Handle symbol selection from search results"""
        code_panel = self.query_one("#middle", CodePanel)
        side_panel = self.query_one("#left", SidePanel)

        # Open file directly with line number
        code_panel.open_file(event.file_path, line_number=event.line_number)

        # Also navigate tree (async) for visual feedback
        await side_panel.on_search_view_file_selected(
            SearchView.FileSelected(event.file_path)
        )



if __name__ == "__main__":
    app = SrcodexApp()
    app.run()
