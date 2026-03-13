from pathlib import Path
from textual.app import App
from textual.widgets import Label, DirectoryTree
from components.side_panel import SidePanel
from components.code_panel import CodePanel
from components.chat_panel import ChatPanel
from components.search_view import SearchView



class SrcodexApp(App):
    CSS_PATH = "app.tcss"

    # Source root - shared by all componenets
    SOURCE_ROOT = "/utg/pmfwex/pmfw_source"
    def compose(self):
        yield SidePanel(self.SOURCE_ROOT, id="left")
        yield CodePanel(self.SOURCE_ROOT, id="middle")
        yield ChatPanel(id="right")

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        """
        handle file selection from FileBrowser explorer view
        """
        code_panel = self.query_one("#middle", CodePanel)

        absolute_path = Path(event.path)
        source_root = Path(self.SOURCE_ROOT)

        try:
            relative_path = absolute_path.relative_to(source_root)
            code_panel.open_file(str(relative_path))
        except ValueError:
            self.notify(f"Cannot open file outside source root: {event.path}", severity="error")



if __name__ == "__main__":
    app = SrcodexApp()
    app.run()
