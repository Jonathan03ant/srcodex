from textual.containers import Container
from .file_browser import FileBroswer

class ExplorerView(Container):
    """Explorer View- File Tree Navigation
        Wrapper for FileBroswer
    """
    DEFAULT_CSS = """
    ExplorerView {
        width: 100%;
        height: 100%;
    }
    """

    def __init__(self, root_path: str, **kwargs):
        super().__init__(**kwargs)
        self.root_path = root_path

    def compose(self):
        yield FileBroswer(self.root_path)