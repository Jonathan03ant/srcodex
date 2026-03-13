from pathlib import Path
from textual.widgets import Static
from textual.containers import Horizontal
from textual.message import Message

class FileTab(Static):
    """Simple file tab displaying filename
        Used in code panel
    """
    def __init__(self, file_path: str, active: bool = False, **kwargs):
        """ Show just the filename """
        filename = Path(file_path).name
        super().__init__(filename, **kwargs)
        self.file_path = file_path
        if active:
            self.add_class("active")

class CodeTabBar(Horizontal):
    """Horizonatal tab bar for open files"""
    class TabClicked(Message):
        """posed when file tab is clicked"""
        def __init__(self, file_path: str):
            super().__init__()
            self.file_path = file_path

    DEFAULT_CSS = """
    CodeTabBar {
        height: 1;
        width: 100%;
        align: left middle;
        dock: top;
    }

    FileTab {
        width: auto;
        height: 1;
        background: transparent;
        padding: 0 1;
    }

    FileTab:hover {
        background: transparent;
    }

    FileTab.active {
        text-style: bold;
    }
    """

    def __init__(self, **kwargs):
        self.open_files = [] #list of open files
        self.active_file = None
        super().__init__(**kwargs)

    def compose(self):
        return []

    def add_tab(self, file_path: str, set_active: bool = True):
        """add new file tab"""
        if file_path in self.open_files:
            if set_active:
                self._set_active_tab(file_path)
            return

        # add to open files list
        self.open_files.append(file_path)
        # create and mount the tab
        is_active = set_active or len(self.open_files) == 1
        tab = FileTab(file_path, active=is_active)
        self.mount(tab)

        if is_active:
            self.active_file = file_path

    def _set_active_tab(self, file_path: str):
        """set which tab is active"""
        self.active_file = file_path

        for tab in self.query(FileTab):
            if tab.file_path == file_path:
                tab.add_class("active")
            else:
                tab.remove_class("active")

    def on_click(self, event):
        """handle tab clicks"""
        clicked = event.widget
        if isinstance(clicked, FileTab):
            self._set_active_tab(clicked.file_path)
            self.post_message(self.TabClicked(clicked.file_path))