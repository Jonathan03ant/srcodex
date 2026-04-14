"""
Status tracker for Claude query execution

Tracks iteration progress, tool calls, and elapsed time for live UI updates.
"""
import time
from typing import Optional, Dict, Any


class StatusTracker:
    """Tracks query execution status for live UI updates"""

    def __init__(self):
        self.query_start_time: Optional[float] = None
        self.current_iteration: int = 0
        self.max_iterations: int = 5
        self.current_status: str = ""
        self.is_active: bool = False

    def start_query(self):
        """Start tracking a new query"""
        self.query_start_time = time.time()
        self.current_iteration = 0
        self.current_status = ""
        self.is_active = True

    def end_query(self):
        """Stop tracking the query"""
        self.is_active = False
        self.current_status = ""

    def start_iteration(self, iteration: int):
        """Start a new iteration"""
        self.current_iteration = iteration

    def set_tool_status(self, tool_name: str, total_tools: int = 1):
        """
        Update status with current tool being called

        Args:
            tool_name: Name of the tool (e.g., 'search_symbols')
            total_tools: Total number of tools in this batch
        """
        if total_tools > 1:
            self.current_status = f"Calling {tool_name}() +{total_tools-1} more"
        else:
            self.current_status = f"Calling {tool_name}()"

    def set_processing_status(self, total_tools: int):
        """Update status to show processing multiple tools"""
        self.current_status = f"Processing {total_tools} tools..."

    def set_preparing_answer(self):
        """Update status to show Claude is preparing the answer"""
        self.current_status = "Preparing answer..."

    def set_complete(self):
        """Mark query as complete"""
        self.current_status = "Complete"

    def get_elapsed_seconds(self) -> int:
        """Get elapsed time in whole seconds"""
        if not self.query_start_time:
            return 0
        return int(time.time() - self.query_start_time)

    def get_status_message(self) -> Dict[str, Any]:
        """
        Get current status as a dict for streaming to frontend

        Returns:
            {"type": "status", "content": "Calling search_symbols() +34 more", "elapsed": 5}
        """
        return {
            "type": "status",
            "content": self.current_status,
            "elapsed": self.get_elapsed_seconds()
        }

    def format_status_line(self) -> str:
        """
        Format status line for display

        Returns:
            "Calling search_symbols() +34 more | 5s"
        """
        if not self.is_active or not self.current_status:
            return ""

        elapsed = self.get_elapsed_seconds()
        return f"{self.current_status} | {elapsed}s"
