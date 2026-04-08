from pathlib import Path
from textual.widgets import Static
from textual.containers import Horizontal
from textual.message import Message

class CloseButton(Static):
    """Close button for file tab"""
    def __init__(self, **kwargs):
        super().__init__("✕", **kwargs)

class FileTab(Horizontal):
    """File tab with filename and close button"""
    def __init__(self, file_path: str, active: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.filename = Path(file_path).name
        if active:
            self.add_class("active")

    def compose(self):
        yield Static(self.filename, classes="tab-label")
        yield CloseButton(classes="tab-close")

class CodeTabBar(Horizontal):
    """Horizonatal tab bar for open files"""
    class TabClicked(Message):
        """Posted when file tab is clicked"""
        def __init__(self, file_path: str):
            super().__init__()
            self.file_path = file_path

    class TabClosed(Message):
        """Posted when tab close button is clicked"""
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

    FileTab .tab-label {
        width: auto;
        height: 1;
    }

    FileTab .tab-close {
        width: auto;
        height: 1;
        color: $text-muted;
        padding: 0 1;
    }

    FileTab .tab-close:hover {
        color: $error;
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

    def close_tab(self, file_path: str):
        """Close a tab"""
        if file_path not in self.open_files:
            return

        self.open_files.remove(file_path)

        # Remove the tab widget
        for tab in self.query(FileTab):
            if tab.file_path == file_path:
                tab.remove()
                break

        # If this was the active tab, switch to another
        if self.active_file == file_path:
            if self.open_files:
                new_active = self.open_files[-1]
                self._set_active_tab(new_active)
                self.post_message(self.TabClicked(new_active))
            else:
                self.active_file = None

    def on_click(self, event):
        """Handle tab clicks and close button"""
        clicked = event.widget

        # Check if close button was clicked
        if isinstance(clicked, CloseButton):
            # Find parent FileTab
            parent = clicked.parent
            if isinstance(parent, FileTab):
                self.close_tab(parent.file_path)
                self.post_message(self.TabClosed(parent.file_path))

        # Check if tab label or tab itself was clicked
        elif isinstance(clicked, FileTab):
            self._set_active_tab(clicked.file_path)
            self.post_message(self.TabClicked(clicked.file_path))

        # Check if tab label (Static) inside FileTab was clicked
        elif isinstance(clicked, Static) and isinstance(clicked.parent, FileTab):
            parent = clicked.parent
            self._set_active_tab(parent.file_path)
            self.post_message(self.TabClicked(parent.file_path))