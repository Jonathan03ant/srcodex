from pathlib import Path
from textual.widgets import TextArea
from textual.containers import Container
from components.bars.code_tab_bar import CodeTabBar
from components.widgets.find_box import FindBox
from components.logger import logger


class CodePanel(Container):
    """ Code Viewer Panel (Middle)
        Displays file contents using TextArea widget with line numbers
    """
    DEFAULT_CSS = """
        CodePanel {
            width: 100%;
            height: 100%;
        }

        CodePanel TextArea {
            width: 100%;
            height: 100%;
        }

        CodePanel FindBox {
            dock: top;
            align: right top;
            margin: 2 0 0 0;
        }
        """
    def __init__(self, source_root: str, **kwargs):
        super().__init__(**kwargs)
        self.source_root = Path(source_root)
        self.text_area = None
        self.find_box = None
        self.find_visible = False
        self.current_file = None
        self.find_matches = []  # List of (line, col) tuples
        self.current_match_index = 0  # Which match we're on
        self.current_query = ""  # Current search query for highlighting

    def compose(self):
        self.tab_bar = CodeTabBar()
        yield self.tab_bar
        self.text_area = TextArea(
            text="",
            show_line_numbers=True,
            read_only=True,
        )
        yield self.text_area

    def open_file(self, file_path: str, line_number: int = None):
        """Open file in viewer and optionally jump to line"""
        self.tab_bar.add_tab(file_path, set_active=True)
        self.current_file = file_path
        full_path = self.source_root / file_path
        try:
            with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            self.text_area.load_text(content)

            # Jump to line if specified
            if line_number is not None:
                self.text_area.cursor_location = (line_number - 1, 0)
                self.text_area.focus()
        except Exception as e:
            self.text_area.load_text(f"Error loading {file_path}: {e}")

    def toggle_find(self):
        """Toggle find box visibility"""
        if self.find_visible:
            # Hide find box
            if self.find_box:
                self.find_box.remove()
                self.find_box = None
            self.find_visible = False
            self.text_area.focus()
        else:
            # Show find box - make it very visible for debugging
            self.find_box = FindBox()
            self.mount(self.find_box)
            self.find_visible = True
            self.log(f"Find box mounted: {self.find_box}")

    def on_find_box_find_next(self, event: FindBox.FindNext):
        """Handle find query - find all occurrences and show match count"""
        query = event.query.lower()
        logger.debug(f"[FIND] Query received: '{query}'")

        if not query:
            logger.debug("[FIND] Empty query, clearing selection")
            self.find_matches = []
            self.current_match_index = 0
            # Don't set selection to None - just leave it as is
            return

        # Find ALL occurrences
        content = str(self.text_area.text)
        lines = content.split('\n')
        logger.debug(f"[FIND] Searching in {len(lines)} lines")

        self.find_matches = []
        for line_idx, line in enumerate(lines):
            # Find all occurrences in this line
            start_pos = 0
            while True:
                pos = line.lower().find(query, start_pos)
                if pos == -1:
                    break
                self.find_matches.append((line_idx, pos))
                start_pos = pos + 1  # Continue searching after this match

        logger.info(f"[FIND] Found {len(self.find_matches)} occurrences of '{query}'")

        if self.find_matches:
            # Reset to first match
            self.current_match_index = 0
            self.current_query = query  # Store query for navigation
            self._highlight_current_match(query)

            # Update find box to show count
            self.find_box.update_match_count(1, len(self.find_matches))
            logger.debug(f"[FIND] Showing match 1 of {len(self.find_matches)}")
        else:
            logger.warning(f"[FIND] Query '{query}' not found in file")
            # Don't set selection to None - it causes TypeError
            # Just leave the previous selection or do nothing
            if self.find_box:
                self.find_box.update_match_count(0, 0)

    def on_find_box_next_match(self, event: FindBox.NextMatch):
        """Go to next match (down arrow)"""
        if not self.find_matches:
            return

        # Move to next match, wrap around to start
        self.current_match_index = (self.current_match_index + 1) % len(self.find_matches)
        self._highlight_current_match(self.current_query)

        # Update counter
        self.find_box.update_match_count(self.current_match_index + 1, len(self.find_matches))
        logger.debug(f"[FIND] Next match: {self.current_match_index + 1}/{len(self.find_matches)}")

    def on_find_box_prev_match(self, event: FindBox.PrevMatch):
        """Go to previous match (up arrow)"""
        if not self.find_matches:
            return

        # Move to previous match, wrap around to end
        self.current_match_index = (self.current_match_index - 1) % len(self.find_matches)
        self._highlight_current_match(self.current_query)

        # Update counter
        self.find_box.update_match_count(self.current_match_index + 1, len(self.find_matches))
        logger.debug(f"[FIND] Previous match: {self.current_match_index + 1}/{len(self.find_matches)}")

    def _highlight_current_match(self, query):
        """Highlight the current match from find_matches list"""
        if not self.find_matches or self.current_match_index >= len(self.find_matches):
            return

        line_idx, col = self.find_matches[self.current_match_index]
        logger.debug(f"[FIND] Highlighting match {self.current_match_index + 1}/{len(self.find_matches)} at line {line_idx}, col {col}")

        # Move cursor and select
        self.text_area.cursor_location = (line_idx, col)
        selection = ((line_idx, col), (line_idx, col + len(query)))
        self.text_area.selection = selection

    def on_key(self, event):
        """Handle keyboard shortcuts"""
        # Ctrl+F - toggle find
        if event.key == "ctrl+f":
            self.toggle_find()
            event.prevent_default()
            event.stop()
        # Esc - close find if open
        elif event.key == "escape" and self.find_visible:
            self.toggle_find()
            event.prevent_default()
            event.stop()

    def on_code_tab_bar_tab_clicked(self, message: CodeTabBar.TabClicked):
        """Handle tab clicks"""
        self.open_file(message.file_path)

    def on_code_tab_bar_tab_closed(self, message: CodeTabBar.TabClosed):
        """Handle tab close - clear viewer if no tabs left"""
        if not self.tab_bar.open_files:
            self.text_area.load_text("")
        elif self.tab_bar.active_file:
            # Switch to the new active tab
            self.open_file(self.tab_bar.active_file)