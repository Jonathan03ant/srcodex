from pathlib import Path
from textual.widgets import TextArea
from textual.containers import Container
from components.code_tab_bar import CodeTabBar


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

    def open_file(self, file_path: str):
        """
        open file in viewer
        add/switch tab
        """
        self.tab_bar.add_tab(file_path, set_active=True)
        full_path = self.source_root / file_path
        try:
            with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            self.text_area.load_text(content)
        except Exception as e:
            self.text_area.load_text(f"Error loading {file_path}: {e}")

    def on_code_tab_bar_clicked(self, message: CodeTabBar.TabClicked):
        """handle tab clicks"""
        self.open_file(message.file_path)