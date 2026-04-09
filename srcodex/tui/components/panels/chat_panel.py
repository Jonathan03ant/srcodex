from textual.widgets import Static
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, TextArea, TextArea
from textual.message import Message
import httpx
import json
import sys
from pathlib import Path

# Add backend to path for imports
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

        # Session manager for persistent conversation history
        # Get project root from config (the actual indexed project, not the TUI module location)
        config = get_config()
        self.session_manager = SessionManager(str(config.project_root))

        # Load previous conversation if exists
        self.conversation_history = self.session_manager.load_session()
        self.session_loaded = len(self.conversation_history) > 0

    DEFAULT_CSS = """
    ChatPanel {
        width: 100%;
        height: 100%;
    }

    #conversation-history {
        height: 1fr;
        padding: 1;
        overflow-y: scroll;
        overflow-x: hidden;
        border: none;
        background: transparent;
    }

    #conversation-history:focus {
        border: none;
        background: transparent;
    }

    #conversation-history > .text-area--cursor-line {
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
        # Top: Scrollable conversation history (read-only TextArea for selection support)
        yield TextArea(id="conversation-history", read_only=True, show_line_numbers=False)
        # Token counter
        yield Static("", id="token-counter")
        # Bottom: multi-line input for typing
        with Vertical(id="chat-input-container"):
            yield ChatInput(id="chat-input", show_line_numbers=False)

    def on_mount(self) -> None:
        """When panel is mounted, show welcome message"""
        conversation = self.query_one("#conversation-history", TextArea)

        if self.session_loaded:
            # Show loaded conversation
            conversation.text = "Claude Chat\n[Loaded previous conversation from .srcodex/conversations/]\n\n"
            # Display loaded messages
            for msg in self.conversation_history:
                if msg["role"] == "user":
                    conversation.text += f"→ You: {msg['content']}\n\n"
                elif msg["role"] == "assistant":
                    conversation.text += f"← Claude: {msg['content']}\n\n"
        else:
            conversation.text = "Claude Chat\nType a question below and press Enter.\nPress Ctrl+L to clear conversation history.\n\n"

    def on_key(self, event):
        """Handle keyboard shortcuts"""
        if event.key == "ctrl+l":
            # Clear conversation history (both in-memory and on disk)
            self.conversation_history = []
            self.session_manager.clear_session()
            conversation = self.query_one("#conversation-history", TextArea)
            conversation.text = "Claude Chat\n[Conversation history cleared - starting fresh]\n\n"
            # Reset token counters
            self.session_input_tokens = 0
            self.session_output_tokens = 0
            self.session_cache_read_tokens = 0
            self.session_cache_write_tokens = 0
            self.last_query_input_tokens = 0
            self.last_query_output_tokens = 0
            self.last_query_cache_read = 0
            self.last_query_cache_write = 0
            self._update_token_display()
            event.prevent_default()
            event.stop()

    async def on_chat_input_submit(self, event: ChatInput.Submit):
        """Handle message submission from ChatInput"""
        conversation = self.query_one("#conversation-history", TextArea)
        conversation.text += f"→ You: {event.text}\n\n"

        await self.send_message(event.text)

    async def send_message(self, user_message: str):
        """Send message to Claude backend"""
        conversation = self.query_one("#conversation-history", TextArea)
        token_counter = self.query_one("#token-counter", Static)

        try:
            # Stream response from backend
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

                    # Parse newline-delimited JSON stream
                    full_response = ""
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue

                        try:
                            data = json.loads(line)

                            if data["type"] == "text":
                                full_response += data["content"]

                            elif data["type"] == "tokens":
                                # Update token tracking
                                self.last_query_input_tokens = data["input"]
                                self.last_query_output_tokens = data["output"]
                                self.last_query_cache_read = data.get("cache_read", 0)
                                self.last_query_cache_write = data.get("cache_write", 0)

                                self.session_input_tokens += data["input"]
                                self.session_output_tokens += data["output"]
                                self.session_cache_read_tokens += data.get("cache_read", 0)
                                self.session_cache_write_tokens += data.get("cache_write", 0)

                                # Update token counter display
                                self._update_token_display()

                        except json.JSONDecodeError:
                            # Skip malformed JSON
                            continue

                    # Display response
                    conversation.text += f"← Claude: {full_response}\n\n"

                    # Add to conversation history (for next turn)
                    self.conversation_history.append({
                        "role": "user",
                        "content": user_message
                    })
                    self.conversation_history.append({
                        "role": "assistant",
                        "content": full_response
                    })

                    # Limit conversation history to last 10 messages
                    # This prevents token explosion from accumulated tool results
                    max_messages = 10
                    if len(self.conversation_history) > max_messages:
                        self.conversation_history = self.conversation_history[-max_messages:]

                    # Save conversation to disk (auto-persist across restarts)
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
            conversation.text += f"Error: HTTP Error: {type(e).__name__}: {str(e)}\n\n"
        except Exception as e:
            conversation.text += f"Error: {type(e).__name__}: {str(e)}\n\n"

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
