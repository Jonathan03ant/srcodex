from textual.widgets import Static
from textual.containers import Horizontal
from textual.message import Message


class ChatHeaderButton(Static):
    def __init__(self, label: str, button_id: str, **kwargs):
        super().__init__(label, **kwargs)
        self.button_id = button_id


class ChatHeader(Horizontal):
    class SettingsClicked(Message):
        pass

    DEFAULT_CSS = """
    ChatHeader {
        height: 1;
        width: 100%;
        align: right middle;
        dock: top;
    }

    ChatHeaderButton {
        width: auto;
        height: 1;
        background: transparent;
        padding: 0 1;
    }

    ChatHeaderButton:hover {
        background: transparent;
    }

    ChatHeaderButton#header-settings {
        margin-right: 4;
    }
    """

    def compose(self):
        yield ChatHeaderButton("✳", "header-chat", id="header-chat")
        yield ChatHeaderButton("⚙", "header-settings", id="header-settings")

    def on_click(self, event) -> None:
        clicked = event.widget
        if isinstance(clicked, ChatHeaderButton):
            if clicked.button_id == "header-settings":
                self.post_message(self.SettingsClicked())
