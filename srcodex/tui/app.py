from pathlib import Path
from textual.app import App
from textual.widgets import Label, DirectoryTree
from textual.containers import Container
import asyncio
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
# Add backend to path for config loader
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from components.panels.side_panel import SidePanel
from components.panels.code_panel import CodePanel
from components.panels.chat_panel import ChatPanel
from components.views.search_view import SearchView
from components.bars.footer_bar import FooterBar
from services.config_loader import get_config



class SrcodexApp(App):
    CSS_PATH = "app.tcss"

    # Panel width state (fractional units: left=0.8, middle=2, right=1.4, sum=4.2)
    left_width = 0.8
    middle_width = 2
    right_width = 1.4

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Load project configuration at runtime
        config = get_config()
        self.SOURCE_ROOT = str(config.source_root)
        self.PROJECT_ROOT = str(config.project_root)

    def compose(self):
        yield SidePanel(self.SOURCE_ROOT, id="left")
        yield CodePanel(self.SOURCE_ROOT, id="middle")
        yield ChatPanel(id="right")
        yield FooterBar(self.PROJECT_ROOT)

    def on_key(self, event):
        """Handle keyboard shortcuts for panel resizing"""
        # Ctrl+Shift+Left: Grow chat panel, shrink code viewer
        if event.key == "ctrl+shift+left":
            if self.middle_width > 0.5:
                self.right_width += 0.1
                self.middle_width -= 0.1
                self._update_panel_widths()
                event.prevent_default()

        # Ctrl+Shift+Right: Shrink chat panel, grow code viewer
        elif event.key == "ctrl+shift+right":
            if self.right_width > 0.5:
                self.right_width -= 0.1
                self.middle_width += 0.1
                self._update_panel_widths()
                event.prevent_default()

    def _update_panel_widths(self):
        """Update panel widths dynamically"""
        left_panel = self.query_one("#left")
        middle_panel = self.query_one("#middle")
        right_panel = self.query_one("#right")

        # Update styles with new fractional widths
        left_panel.styles.width = f"{self.left_width}fr"
        middle_panel.styles.width = f"{self.middle_width}fr"
        right_panel.styles.width = f"{self.right_width}fr"

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
