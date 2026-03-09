from textual.app import App
from textual.widgets import Label
from components.side_panel import SidePanel
from components.file_browser import FileBroswer
from components.code_panel import CodePanel
from components.chat_panel import ChatPanel



class SrcodexApp(App):
    CSS_PATH = "app.tcss"
    def compose(self):
        yield SidePanel("/utg/pmfwex/pmfw_source", id="left")
        yield CodePanel(id="middle")
        yield ChatPanel(id="right")








if __name__ == "__main__":
    app = SrcodexApp()
    app.run()
