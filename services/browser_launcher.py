"""Opens the default browser shortly after the server starts."""

import threading
import webbrowser


class BrowserLauncher:
    def __init__(self, url: str, delay_seconds: float = 1.0) -> None:
        self._url: str = url
        self._delay: float = delay_seconds

    def launch_async(self) -> None:
        timer = threading.Timer(self._delay, lambda: webbrowser.open(self._url))
        timer.daemon = True
        timer.start()
