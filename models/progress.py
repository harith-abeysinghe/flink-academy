"""Progress document model. The browser owns the schema; the server validates the envelope only."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar


@dataclass
class ProgressDocument:
    data: dict[str, Any] = field(default_factory=dict)
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    MAX_BYTES: ClassVar[int] = 1_000_000

    def to_dict(self) -> dict[str, Any]:
        return {"data": self.data, "updatedAt": self.updated_at}

    @classmethod
    def from_payload(cls, payload: Any) -> "ProgressDocument":
        if not isinstance(payload, dict):
            raise ValueError("Progress payload must be a JSON object")
        inner: Any = payload.get("data", payload)
        if not isinstance(inner, dict):
            raise ValueError("Progress 'data' must be a JSON object")
        return cls(data=inner)
