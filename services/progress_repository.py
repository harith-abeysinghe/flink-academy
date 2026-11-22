"""Thread-safe JSON file persistence for learner progress."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from models.progress import ProgressDocument


class ProgressRepository:
    def __init__(self, file_path: Path) -> None:
        self._file_path: Path = file_path
        self._lock: threading.Lock = threading.Lock()
        self._file_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> ProgressDocument | None:
        with self._lock:
            if not self._file_path.is_file():
                return None
            try:
                raw: dict = json.loads(self._file_path.read_text(encoding="utf-8"))
                doc = ProgressDocument.from_payload(raw)
                doc.updated_at = raw.get("updatedAt", doc.updated_at)
                return doc
            except (json.JSONDecodeError, ValueError):
                return None

    def save(self, document: ProgressDocument) -> None:
        with self._lock:
            tmp_path: Path = self._file_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(document.to_dict(), indent=2), encoding="utf-8")
            tmp_path.replace(self._file_path)

    def reset(self) -> None:
        with self._lock:
            if self._file_path.is_file():
                backup: Path = self._file_path.with_suffix(".bak.json")
                self._file_path.replace(backup)
