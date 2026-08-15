"""Minimal A2A JSON-RPC mock for the A2A Python reference scenario."""

from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def _task(
    task_id: str = "task-calendar-summary",
    context_id: str = "ctx-calendar",
    state: str = "TASK_STATE_COMPLETED",
) -> dict[str, Any]:
    return {
        "id": task_id,
        "contextId": context_id,
        "status": {"state": state},
        "artifacts": [
            {
                "artifactId": "art-001",
                "parts": [{"text": "Calendar summary artifact."}],
            }
        ],
    }


def _response(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class A2ARequestHandler(BaseHTTPRequestHandler):
    """Serve only the JSON-RPC operations exercised by the scenario."""

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(HTTPStatus.OK)
            self.end_headers()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if self.path != "/a2a":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        content_length = int(self.headers["Content-Length"])
        request = json.loads(self.rfile.read(content_length))
        request_id = request.get("id")
        method = request.get("method")

        if method == "SendMessage":
            self._send_json(
                _response(
                    request_id,
                    {
                        "task": {
                            **_task(),
                            "history": [
                                {
                                    "messageId": "msg-agent-1",
                                    "role": "ROLE_AGENT",
                                    "taskId": "task-calendar-summary",
                                    "parts": [{"text": "Your afternoon is open."}],
                                }
                            ],
                        }
                    },
                )
            )
            return

        if method == "SendStreamingMessage":
            self._send_stream(request_id)
            return

        if method == "GetTask":
            params = request.get("params") or {}
            self._send_json(_response(request_id, _task(task_id=params.get("id", "task-calendar-summary"))))
            return

        self._send_json({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "Method not found"}})

    def _send_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_stream(self, request_id: Any) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for state in ("TASK_STATE_WORKING", "TASK_STATE_COMPLETED"):
            event = _response(
                request_id,
                {
                    "statusUpdate": {
                        "taskId": "task-streaming",
                        "contextId": "ctx-streaming",
                        "status": {"state": state},
                    }
                },
            )
            self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), A2ARequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
