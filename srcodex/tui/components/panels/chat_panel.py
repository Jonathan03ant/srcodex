from textual.widgets import Static, Markdown, ListView, ListItem, Label
from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.widgets import Input, TextArea
from textual.message import Message
import httpx
import json
import sys
from pathlib import Path

from components.bars.chat_header import ChatHeader

backend_path = Path(__file__).parent.parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path))
from services.session_manager import SessionManager
from services.config_loader import get_config

class ChatInput(TextArea):
    """Custom TextArea that sends message on Enter"""

    class Submit(Message):
        """Posted when user presses Enter (not Shift+Enter)"""
        def __init__(self, text: str):
            super().__init__()
            self.text = text

    def on_key(self, event):
        """Intercept Enter key - use Ctrl+Enter for new line"""
        if event.key == "ctrl+enter":
            # Ctrl+Enter - insert new line manually
            cursor = self.cursor_location
            current_text = self.text
            # Insert newline at cursor position
            line, col = cursor
            lines = current_text.split('\n')
            if line < len(lines):
                lines[line] = lines[line][:col] + '\n' + lines[line][col:]
                self.text = '\n'.join(lines)
                self.cursor_location = (line + 1, 0)
            event.prevent_default()
            event.stop()
        elif event.key == "enter":
            # Plain Enter - submit
            event.prevent_default()
            event.stop()
            text = self.text.strip()
            if text:
                self.post_message(self.Submit(text))
                self.text = ""

class ChatSettingsMenu(Vertical):
    """Dropdown menu for chat settings"""

    DEFAULT_CSS = """
    ChatSettingsMenu {
        width: 20;
        height: auto;
        background: transparent;
        layer: overlay;
        offset-y: 1;
    }

    ChatSettingsMenu ListView {
        height: auto;
        width: 100%;
        background: transparent;
        border: none;
    }

    ChatSettingsMenu ListItem {
        height: 1;
        padding: 0 1;
        background: transparent;
    }

    ChatSettingsMenu ListItem:hover {
        background: $boost;
    }
    """

    def compose(self):
        yield ListView(
            ListItem(Label("Clear History"), id="clear-history"),
            ListItem(Label("Export Chat"), id="export-chat"),
        )


class ChatPanel(Vertical):
    """ AI chat panel (right) - Claude (LLM) conversation interface"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session_input_tokens = 0
        self.session_output_tokens = 0
        self.session_cache_read_tokens = 0
        self.session_cache_write_tokens = 0
        self.last_query_input_tokens = 0
        self.last_query_output_tokens = 0
        self.last_query_cache_read = 0
        self.last_query_cache_write = 0
        self.settings_menu_visible = False

        config = get_config()
        self.session_manager = SessionManager(str(config.project_root))

        self.conversation_history = self.session_manager.load_session()
        self.session_loaded = len(self.conversation_history) > 0

    DEFAULT_CSS = """
    ChatPanel {
        width: 100%;
        height: 100%;
    }

    #settings-menu {
        dock: top;
        align: right top;
        offset-y: 1;
        display: none;
    }

    #settings-menu.visible {
        display: block;
    }

    #conversation-scroll {
        height: 1fr;
        border: none;
    }

    #conversation-history {
        width: 100%;
        padding: 1;
        background: transparent;
    }

    ChatPanel Scrollbar {
        width: 1;
    }

    #chat-input-container {
        width: 100%;
        height: auto;
        padding: 0;
        border: grey;
    }

    #chat-input {
        width: 1fr;
        height: auto;
        min-height: 3;
        border: none;
        margin: 0;
        padding: 0;
        background: transparent;
    }

    #chat-input:focus {
        background: transparent;
    }

    #chat-input > .text-area--cursor-line {
        background: transparent;
    }

    #token-counter {
        height: 2;
        width: 100%;
        background: transparent;
        color: $text-muted;
        text-align: right;
        padding-right: 1;
    }
    """

    def compose(self):
        """Build the chat panel UI"""
        yield ChatHeader()
        yield ChatSettingsMenu(id="settings-menu")
        with VerticalScroll(id="conversation-scroll"):
            yield Markdown("", id="conversation-history")
        yield Static("", id="token-counter")
        with Vertical(id="chat-input-container"):
            yield ChatInput(id="chat-input", show_line_numbers=False)

    def on_mount(self) -> None:
        """When panel is mounted, show welcome message"""
        conversation = self.query_one("#conversation-history", Markdown)

        if self.session_loaded:
            markdown_text = self._build_conversation_markdown()
        else:
            markdown_text = "*Type below and press Enter to chat. Ctrl+L to clear history.*\n\n"

        conversation.update(markdown_text)
        self.set_timer(0.3, self._scroll_to_bottom)

    def _build_conversation_markdown(self) -> str:
        """Build markdown string from conversation history"""
        if not self.conversation_history:
            return "*Ask a question below (Enter to send, Ctrl+L to clear)*"

        lines = []
        for msg in self.conversation_history:
            if msg["role"] == "user":
                lines.append(f"> **You:** {msg['content']}")
                lines.append("")
            elif msg["role"] == "assistant":
                lines.append(f"**Claude:**")
                lines.append(msg['content'])
                lines.append("")

        return "\n".join(lines)

    def on_chat_header_settings_clicked(self, event: ChatHeader.SettingsClicked):
        """Handle settings button click - toggle menu"""
        menu = self.query_one("#settings-menu", ChatSettingsMenu)
        if self.settings_menu_visible:
            menu.remove_class("visible")
            self.settings_menu_visible = False
        else:
            menu.add_class("visible")
            self.settings_menu_visible = True

    def on_list_view_selected(self, event: ListView.Selected):
        """Handle menu item selection"""
        menu = self.query_one("#settings-menu", ChatSettingsMenu)
        menu.remove_class("visible")
        self.settings_menu_visible = False

        if event.item.id == "clear-history":
            self._clear_history()

    def _clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
        self.session_manager.clear_session()
        self.session_input_tokens = 0
        self.session_output_tokens = 0
        self.session_cache_read_tokens = 0
        self.session_cache_write_tokens = 0
        self.last_query_input_tokens = 0
        self.last_query_output_tokens = 0
        self.last_query_cache_read = 0
        self.last_query_cache_write = 0
        self._update_token_display()
        self._refresh_conversation()

    def on_key(self, event):
        """Handle keyboard shortcuts"""
        if event.key == "ctrl+l":
            self._clear_history()
            event.prevent_default()
            event.stop()

    async def on_chat_input_submit(self, event: ChatInput.Submit):
        """Handle message submission from ChatInput"""
        self.conversation_history.append({
            "role": "user",
            "content": event.text
        })
        self._refresh_conversation()
        await self.send_message(event.text)

    async def send_message(self, user_message: str):
        """Send message to Claude backend"""
        conversation = self.query_one("#conversation-history", Markdown)
        token_counter = self.query_one("#token-counter", Static)

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    "http://localhost:8000/api/chat/stream",
                    json={
                        "message": user_message,
                        "conversation_history": self.conversation_history
                    },
                    timeout=None
                ) as response:
                    response.raise_for_status()

                    full_response = ""
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue

                        try:
                            data = json.loads(line)

                            if data["type"] == "text":
                                full_response += data["content"]

                            elif data["type"] == "tokens":
                                self.last_query_input_tokens = data["input"]
                                self.last_query_output_tokens = data["output"]
                                self.last_query_cache_read = data.get("cache_read", 0)
                                self.last_query_cache_write = data.get("cache_write", 0)

                                self.session_input_tokens += data["input"]
                                self.session_output_tokens += data["output"]
                                self.session_cache_read_tokens += data.get("cache_read", 0)
                                self.session_cache_write_tokens += data.get("cache_write", 0)

                                self._update_token_display()

                        except json.JSONDecodeError:
                            continue

                    self.conversation_history.append({
                        "role": "assistant",
                        "content": full_response
                    })
                    self._refresh_conversation()
                    self.set_timer(0.5, self._scroll_to_bottom)

                    max_messages = 10
                    if len(self.conversation_history) > max_messages:
                        self.conversation_history = self.conversation_history[-max_messages:]

                    self.session_manager.save_session(
                        messages=self.conversation_history,
                        metadata={
                            "total_tokens": self.session_input_tokens + self.session_output_tokens,
                            "input_tokens": self.session_input_tokens,
                            "output_tokens": self.session_output_tokens,
                            "cache_read": self.session_cache_read_tokens,
                            "cache_write": self.session_cache_write_tokens
                        }
                    )

        except httpx.HTTPError as e:
            self.conversation_history.append({
                "role": "assistant",
                "content": f"**Error:** {type(e).__name__}: {str(e)}"
            })
            self._refresh_conversation()
        except Exception as e:
            self.conversation_history.append({
                "role": "assistant",
                "content": f"**Error:** {type(e).__name__}: {str(e)}"
            })
            self._refresh_conversation()

    def _refresh_conversation(self):
        """Rebuild and update the conversation display"""
        conversation = self.query_one("#conversation-history", Markdown)
        conversation.update(self._build_conversation_markdown())
        self.call_after_refresh(self._scroll_to_bottom)
        self.set_timer(0.2, self._scroll_to_bottom)

    def _scroll_to_bottom(self):
        """Scroll conversation to bottom"""
        try:
            scroll = self.query_one("#conversation-scroll", VerticalScroll)
            scroll.scroll_end(duration=0)
            scroll.scroll_to(y=scroll.max_scroll_y, animate=False)
        except:
            pass

    def _update_token_display(self):
        """Update the token counter display (2 lines)"""
        token_counter = self.query_one("#token-counter", Static)

        # Format numbers with commas
        last_total = self.last_query_input_tokens + self.last_query_output_tokens
        session_total = self.session_input_tokens + self.session_output_tokens

        # Line 1: Last query metrics
        line1 = f"📊 Last: {last_total:,} tokens ({self.last_query_input_tokens:,} in / {self.last_query_output_tokens:,} out)"
        if self.last_query_cache_read > 0 or self.last_query_cache_write > 0:
            line1 += f" | 💾 {self.last_query_cache_read:,} read, {self.last_query_cache_write:,} write"

        # Line 2: Session totals
        line2 = f"Session: {session_total:,} tokens"
        if self.session_cache_read_tokens > 0:
            line2 += f" | 💾 Cache: {self.session_cache_read_tokens:,} read, {self.session_cache_write_tokens:,} write"

        token_counter.update(f"{line1}\n{line2}")
