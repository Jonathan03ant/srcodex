"""
Conversation Session Manager
Handles saving and loading conversation history across TUI restarts.
"""
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional


class SessionManager:
    """Manages persistent conversation sessions"""

    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.sessions_dir = self.project_root / ".srcodex" / "conversations"
        self.current_session_file = self.sessions_dir / "latest.json"

        # Create directories if they don't exist
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

    def save_session(self, messages: List[Dict], metadata: Optional[Dict] = None):
        """
        Save conversation history to disk

        Args:
            messages: List of conversation messages [{"role": "user", "content": "..."}]
            metadata: Optional metadata (token counts, timestamps, etc.)
        """
        session_data = {
            "created_at": datetime.now().isoformat(),
            "message_count": len(messages),
            "metadata": metadata or {},
            "messages": messages
        }

        with open(self.current_session_file, 'w') as f:
            json.dump(session_data, f, indent=2)

    def load_session(self) -> List[Dict]:
        """
        Load last conversation history from disk

        Returns:
            List of messages, or empty list if no session exists
        """
        if not self.current_session_file.exists():
            return []

        try:
            with open(self.current_session_file, 'r') as f:
                session_data = json.load(f)
                return session_data.get("messages", [])
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def clear_session(self):
        """Delete current session file"""
        if self.current_session_file.exists():
            self.current_session_file.unlink()

    def session_exists(self) -> bool:
        """Check if a saved session exists"""
        return self.current_session_file.exists()
