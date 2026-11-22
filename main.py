"""Flink Academy entry point: wires settings, persistence and the HTTP server together."""

from __future__ import annotations

import sys
from pathlib import Path

from config.settings import Settings, SettingsFactory
from services.browser_launcher import BrowserLauncher
from services.http_server import AcademyServer
from services.progress_repository import ProgressRepository


class Application:
    def __init__(self, project_root: Path) -> None:
        self._settings: Settings = SettingsFactory(project_root).create()

    def run(self) -> int:
        repository = ProgressRepository(self._settings.progress_file)
        try:
            server = AcademyServer(self._settings, repository)
        except OSError as exc:
            print(f"Could not start on port {self._settings.port}: {exc}")
            print("Change FA_PORT in .env and try again.")
            return 1

        print("=" * 60)
        print("  Flink Academy is running")
        print(f"  Open: {self._settings.base_url}")
        print("  Progress file:", self._settings.progress_file)
        print("  Press Ctrl+C to stop")
        print("=" * 60)

        if self._settings.open_browser:
            BrowserLauncher(self._settings.base_url).launch_async()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping Flink Academy...")
        finally:
            server.shutdown()
        return 0


if __name__ == "__main__":
    sys.exit(Application(Path(__file__).resolve().parent).run())
