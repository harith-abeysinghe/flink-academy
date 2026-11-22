"""Static file server + tiny JSON API for progress persistence."""

from __future__ import annotations

import json
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from config.settings import Settings
from models.progress import ProgressDocument
from services.progress_repository import ProgressRepository


class AcademyRequestHandler(SimpleHTTPRequestHandler):
    """Serves /web and handles /api/progress (GET, PUT) and /api/reset (POST)."""

    repository: ProgressRepository  # injected via subclassing in AcademyServer

    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".js": "text/javascript",
        ".css": "text/css",
        ".svg": "image/svg+xml",
        ".json": "application/json",
    }

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        if args and isinstance(args[0], str) and args[0].startswith(("GET /api", "PUT /api", "POST /api")):
            return
        super().log_message(format, *args)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self) -> None:
        if self.path.split("?")[0] == "/api/progress":
            doc = self.repository.load()
            self._send_json(HTTPStatus.OK, doc.to_dict() if doc else {"data": None})
            return
        if self.path.split("?")[0] == "/api/health":
            self._send_json(HTTPStatus.OK, {"ok": True})
            return
        super().do_GET()

    def do_PUT(self) -> None:
        if self.path.split("?")[0] != "/api/progress":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        try:
            payload: Any = self._read_json_body()
            doc = ProgressDocument.from_payload(payload)
            self.repository.save(doc)
            self._send_json(HTTPStatus.OK, {"ok": True, "updatedAt": doc.updated_at})
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:
        if self.path.split("?")[0] == "/api/reset":
            self.repository.reset()
            self._send_json(HTTPStatus.OK, {"ok": True})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _read_json_body(self) -> Any:
        length: int = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > ProgressDocument.MAX_BYTES:
            raise ValueError("Invalid body size")
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON: {exc.msg}") from exc

    def _send_json(self, status: HTTPStatus, body: dict[str, Any]) -> None:
        encoded: bytes = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class AcademyServer:
    def __init__(self, settings: Settings, repository: ProgressRepository) -> None:
        self._settings: Settings = settings
        handler_cls = type("BoundAcademyHandler", (AcademyRequestHandler,), {"repository": repository})
        handler = partial(handler_cls, directory=str(settings.web_root))
        self._httpd: ThreadingHTTPServer = ThreadingHTTPServer((settings.host, settings.port), handler)

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        self._httpd.server_close()
