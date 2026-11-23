"""Minimal .env loader (stdlib only, so the tool runs with zero pip installs)."""

import os
from pathlib import Path


class EnvLoader:
    """Reads KEY=VALUE pairs from a .env file into os.environ without overriding existing values."""

    def __init__(self, env_path: Path) -> None:
        self._env_path: Path = env_path

    def load(self) -> bool:
        """Load the file if present. Returns True when a file was found and parsed."""
        if not self._env_path.is_file():
            return False
        for raw_line in self._env_path.read_text(encoding="utf-8").splitlines():
            line: str = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        return True
