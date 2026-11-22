"""Typed application settings, sourced from .env / environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from utils.env_loader import EnvLoader


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    open_browser: bool
    project_root: Path
    web_root: Path
    data_dir: Path

    @property
    def progress_file(self) -> Path:
        return self.data_dir / "progress.json"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/"


class SettingsFactory:
    """Builds Settings from the environment after loading the project's .env file."""

    DEFAULT_HOST: str = "127.0.0.1"
    DEFAULT_PORT: int = 8765

    def __init__(self, project_root: Path) -> None:
        self._project_root: Path = project_root

    def create(self) -> Settings:
        EnvLoader(self._project_root / ".env").load()
        return Settings(
            host=os.environ.get("FA_HOST", self.DEFAULT_HOST),
            port=self._parse_int(os.environ.get("FA_PORT"), self.DEFAULT_PORT),
            open_browser=os.environ.get("FA_OPEN_BROWSER", "true").lower() in ("1", "true", "yes"),
            project_root=self._project_root,
            web_root=self._project_root / "web",
            data_dir=self._project_root / "data",
        )

    @staticmethod
    def _parse_int(value: str | None, default: int) -> int:
        try:
            return int(value) if value else default
        except ValueError:
            return default
