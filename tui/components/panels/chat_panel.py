from textual.widgets import Static

class ChatPanel(Static):
    """ AI chat panel (right)
        Place Holder
    """
    DEFAULT_CSS = """
    ChatPanel {
        width: 100%;
        height: 100%;
        content-align: center middle;
    }
    """








    def __init__(self, **kwargs):
        super().__init__("AI Chat\n(Coming soon)", **kwargs)