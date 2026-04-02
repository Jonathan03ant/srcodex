from textual.containers import Container
from textual.widgets import Input, OptionList, Label
from textual.widgets.option_list import Option
from pathlib import Path
from textual.message import Message
import sys

project_root = Path(__file__).parent.parent.parent.parent  # /utg/pmfwex
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

    class SymbolSelected(Message):
        """Posted when symbol is selected from search results"""
        def __init__(self, file_path: str, line_number: int):
            super().__init__()
            self.file_path = file_path
            self.line_number = line_number

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
        margin: 1 0 0 0;
        height: auto;
        min-height: 3;
    }

    SearchView #results-label {
        color: $text-muted;
        margin: 1 0 0 0;
        height: 1;
    }

    SearchView OptionList {
        height: 1fr;
        border: tall $accent;
    }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_tree_service = FileTreeService(self.DB_PATH)

    def compose(self):
        yield Input(placeholder="Search files...", id="file-input")
        yield Input(placeholder="Search symbols...", id="symbol-input")
        yield Label("Results:", id="results-label")
        yield OptionList(id="search-results")

    def on_input_changed(self, event: Input.Changed):
        """Handle search input changes - search as user types"""
        query = event.value.strip()

        # Only search if query has 2+ characters
        if len(query) < 2:
            self.clear_results()
            return

        # Determine which input triggered this
        if event.input.id == "file-input":
            results = self.file_tree_service.search_file(query)
            self.update_file_results(results)
        elif event.input.id == "symbol-input":
            results = self.file_tree_service.search_symbol_global(query)
            self.update_symbol_results(results)

    def clear_results(self):
        """Clear the results list"""
        results_list = self.query_one("#search-results", OptionList)
        results_list.clear_options()

    def update_file_results(self, results: list):
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

    def update_symbol_results(self, results: list):
        """Update the results list with symbol search results"""
        results_list = self.query_one("#search-results", OptionList)
        results_list.clear_options()

        if not results:
            results_list.add_option(Option("No results found", disabled=True))
            return

        # Add each symbol result
        for result in results:
            name = result['name']
            symbol_type = result['type']
            file_path = result['file_path']
            line_number = result['line_number']

            # Format: "symbol_name (type) - file.c:42"
            label = f"{name} ({symbol_type}) - {file_path}:{line_number}"
            # Store both file_path and line_number in id (use JSON or custom format)
            option_id = f"{file_path}::{line_number}"
            results_list.add_option(Option(label, id=option_id))


    def on_option_list_option_selected(self, event: OptionList.OptionSelected):
        """Handle when search result is clicked"""
        option_id = event.option_id

        if "::" in option_id:
            # Symbol result: "file_path::line_number"
            file_path, line_str = option_id.split("::")
            line_number = int(line_str)
            self.post_message(self.SymbolSelected(file_path, line_number))
        else:
            # File result: just file path
            file_path = option_id
            self.post_message(self.FileSelected(file_path))