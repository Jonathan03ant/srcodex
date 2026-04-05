from textual.widgets import Static
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, RichLog, TextArea
from textual.message import Message
import httpx

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

    DEFAULT_CSS = """
    ChatPanel {
        width: 100%;
        height: 100%;
    }

    #conversation-history {
        height: 1fr;
        padding: 1;
        overflow-y: scroll;
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
    """

    def compose(self):
        """Build the chat panel UI"""
        # Top: Scrollable conversation history
        yield RichLog(id="conversation-history", highlight=True, markup=True)
        # Bottom: multi-line input for typing
        with Vertical(id="chat-input-container"):
            yield ChatInput(id="chat-input", show_line_numbers=False)

    def on_mount(self) -> None:
        """When panel is mounted, show welcome message"""
        conversation = self.query_one("#conversation-history", RichLog)
        conversation.write("[bold cyan]Claude Chat[/bold cyan]")
        conversation.write("Type a question below and press Enter.")
        conversation.write("")

    async def on_chat_input_submit(self, event: ChatInput.Submit):
        """Handle message submission from ChatInput"""
        conversation = self.query_one("#conversation-history", RichLog)
        conversation.write(f"[bold cyan]You:[/bold cyan] {event.text}")

        # Send to backend
        await self.send_message(event.text)

    async def send_message(self, user_message: str):
        """Send message to Claude backend"""
        conversation = self.query_one("#conversation-history", RichLog)

        try:
            # Stream response from backend
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    "http://localhost:8000/api/chat/stream",
                    json={"message": user_message},
                    timeout=60.0
                ) as response:
                    response.raise_for_status()

                    # Collect chunks and build the full response
                    full_response = ""
                    async for chunk in response.aiter_text():
                        if chunk:
                            full_response += chunk

                    # Write the complete response at once
                    conversation.write(f"[bold green]Claude:[/bold green] {full_response}")
                    conversation.write("")

        except httpx.HTTPError as e:
            conversation.write(f"[bold red]Error:[/bold red] Failed to connect to backend: {e}")
            conversation.write("")
        except Exception as e:
            conversation.write(f"[bold red]Error:[/bold red] {e}")
            conversation.write("")
