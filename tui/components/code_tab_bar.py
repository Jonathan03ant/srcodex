from pathlib import Path
from textual.widgets import Static
from textual.containers import Horizontal
from textual.message import Message

class FileTab(Static):
    """Simple file tab displaying filename
        Used in code panel
    """
    def __inti__(self, file_path: str, active: bool = False, **kwargs):
        """ Show just the filename """
        filename = Path(file_path).name
        super().__init__(filename, **kwargs)
        self.file_path = file_path
        if active:
            self.add_class("active")
            