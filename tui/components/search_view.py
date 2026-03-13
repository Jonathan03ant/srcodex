from textual.containers import Container
from textual.widgets import Input, OptionList, Static, Label
from textual.widgets.option_list import Option
from pathlib import Path
from textual.message import Message
import sys

project_root = Path(__file__).parent.parent.parent  # /utg/pmfwex
sys.path.insert(0, str(project_root))

from backend.services.file_tree import FileTreeService

class SearchView(Container):
    """Search View - File and sybole search
    """
    class FileSelected(Message):
        """Posted when file is selected from search results
           Used by other files
        """
        def __init__(self, file_path: str):
            super().__init__()
            self.file_path = file_path

    # Hardcoded DB path for now
    # (TODO: Move to .srcodex/ config later)
    DB_PATH = "/utg/pmfwex/data/pmfw_main.db"

    DEFAULT_CSS = """
    SearchView {
        width: 100%;
        height: 100%;
        padding: 0 1;
    }

    SearchView Input {
        margin: 1 0 1 0;
        border: tall white;
        height: 3;
    }

    SearchView #results-label {
        color: $text-muted;
        margin: 0 0 1 0;
        height: 1;
    }

    SearchView OptionList {
        height: 1fr;
        border: tall $accent;
    }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize FileTreeService
        self.file_tree_service = FileTreeService(self.DB_PATH)

    def compose(self):
        yield Input(placeholder="Search files...")
        yield Label("Results:", id="results-label")
        yield OptionList(id="search-results")

    def on_input_changed(self, event: Input.Changed):
        """Handle search input changes - search as user types"""
        query = event.value.strip()

        # Only search if query has 2+ characters
        if len(query) < 2:
            self.clear_results()
            return

        # Query database
        results = self.file_tree_service.search_file(query)
        self.update_results(results)

    def clear_results(self):
        """Clear the results list"""
        results_list = self.query_one("#search-results", OptionList)
        results_list.clear_options()

    def update_results(self, results: list):
        """Update the results list with search results"""
        results_list = self.query_one("#search-results", OptionList)
        results_list.clear_options()

        if not results:
            results_list.add_option(Option("No results found", disabled=True))
            return

        # Add each result as an option
        for result in results:
            name = result['name']
            path = result['path']
            symbol_count = result.get('symbol_count', 0)

            label = f"{name} - {symbol_count} symbols"
            results_list.add_option(Option(label, id=path))

    def on_option_list_option_selected(self, event: OptionList.OptionSelected):
        """handle when search result is clicked"""
        file_path = event.option_id
        self.post_message(self.FileSelected(file_path))