from pathlib import Path
from textual.widgets import TextArea
from textual.containers import Container
from components.bars.code_tab_bar import CodeTabBar


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
        """
    def __init__(self, source_root: str, **kwargs):
        super().__init__(**kwargs)
        self.source_root = Path(source_root)
        self.text_area = None

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