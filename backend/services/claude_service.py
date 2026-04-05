import os
from anthropic import Anthropic

class ClaudeService:
    """Wrapper for Claude API Calls"""
    def __init__(self):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")

        self.client = Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4"

    def send_message(self, message):
        """Send Message to Claude and get response"""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[
                {"role": "user", "content": message}
            ]
        )

        return response.content[0].text

    def stream_message(self, message):
        """Stream message to Claude and yield text chunks"""
        with self.client.messages.stream(
            model=self.model,
            max_tokens=6000,
            messages=[{"role": "user", "content": message}]
        ) as stream:
            for text in stream.text_stream:
                yield text