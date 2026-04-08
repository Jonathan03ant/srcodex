from textual.widgets import DirectoryTree

class FileBroswer(DirectoryTree):
    """ File Tree Browser componenet
        Left Panel
    """
    DEFAULT_CSS = """
    FileBroswer {
        width: 100%;
        height: 100%;
        overflow-y: scroll;
    }
    """

    def __init__(self, root_path: str, **kwargs):
        super().__init__(root_path, **kwargs)
