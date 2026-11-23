"""Progress document model. The browser owns the schema; the server validates the envelope only."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional


class ProgressDocument:
    MAX_BYTES: int = 1_000_000

    def __init__(self, data: Optional[Dict[str, Any]] = None, updated_at: Optional[str] = None) -> None:
        self.data: Dict[str, Any] = data if data is not None else {}
        self.updated_at: str = updated_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "updatedAt": self.updated_at}

    @classmethod
    def from_payload(cls, payload: Any) -> "ProgressDocument":
        if not isinstance(payload, dict):
            raise ValueError("Progress payload must be a JSON object")
        inner: Any = payload.get("data", payload)
        if not isinstance(inner, dict):
            raise ValueError("Progress 'data' must be a JSON object")
        return cls(data=inner)
