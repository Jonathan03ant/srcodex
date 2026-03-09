from textual.app import App
from textual.widgets import Label, Static
from components.file_browser import FileBroswer



class SrcodexApp(App):
    CSS_PATH = "app.tcss"
    def compose(self):
        yield FileBroswer("/utg/pmfwex/pmfw_source", id="left")
        yield Static("Code Viewer", id="middle")
        yield Static("AI Chat", id="right")








if __name__ == "__main__":
    app = SrcodexApp()
    app.run()
