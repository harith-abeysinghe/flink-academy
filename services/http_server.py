"""Static file server + tiny JSON API for progress persistence."""

import json
import os
import posixpath
import urllib.parse
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Any, Dict

from config.settings import Settings
from models.progress import ProgressDocument
from services.progress_repository import ProgressRepository


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Same as http.server.ThreadingHTTPServer, which only exists on Python 3.7+."""

    daemon_threads = True


class AcademyRequestHandler(SimpleHTTPRequestHandler):
    """Serves /web and handles /api/progress (GET, PUT) and /api/reset (POST)."""

    # Both injected via subclassing in AcademyServer.
    repository: ProgressRepository
    web_root: str

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

    def translate_path(self, path: str) -> str:
        """Map a URL path to a file under web_root.

        Python 3.7+ can do this with SimpleHTTPRequestHandler(directory=...), but 3.6 always serves
        the current working directory, so this mirrors the stdlib logic with web_root as the base.
        """
        path = path.split("?", 1)[0].split("#", 1)[0]
        trailing_slash: bool = path.rstrip().endswith("/")
        try:
            path = urllib.parse.unquote(path, errors="surrogatepass")
        except UnicodeDecodeError:
            path = urllib.parse.unquote(path)
        result: str = self.web_root
        for word in posixpath.normpath(path).split("/"):
            if not word or os.path.dirname(word) or word in (os.curdir, os.pardir):
                continue
            result = os.path.join(result, word)
        if trailing_slash:
            result += "/"
        return result

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

    def _send_json(self, status: HTTPStatus, body: Dict[str, Any]) -> None:
        encoded: bytes = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class AcademyServer:
    def __init__(self, settings: Settings, repository: ProgressRepository) -> None:
        self._settings: Settings = settings
        handler_cls = type(
            "BoundAcademyHandler",
            (AcademyRequestHandler,),
            {"repository": repository, "web_root": str(settings.web_root)},
        )
        self._httpd: ThreadingHTTPServer = ThreadingHTTPServer((settings.host, settings.port), handler_cls)

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        self._httpd.server_close()
