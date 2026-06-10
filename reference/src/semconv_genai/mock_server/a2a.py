"""A2A-compatible mock endpoint for reference scenarios."""

import json

from flask import Blueprint, Response, request

from ._common import sse

bp = Blueprint("a2a", __name__)


def _task(task_id="task-calendar-summary", context_id="ctx-calendar", state="TASK_STATE_COMPLETED"):
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


def _jsonrpc_response(request_id, result):
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result,
    }


def _streaming_events(request_id):
    events = [
        {
            "statusUpdate": {
                "taskId": "task-streaming",
                "contextId": "ctx-streaming",
                "status": {"state": "TASK_STATE_WORKING"},
            }
        },
        {
            "statusUpdate": {
                "taskId": "task-streaming",
                "contextId": "ctx-streaming",
                "status": {"state": "TASK_STATE_COMPLETED"},
            }
        },
    ]
    for event in events:
        yield sse(_jsonrpc_response(request_id, event))


@bp.route("/a2a", methods=["POST"])
def handle_a2a_jsonrpc():
    body = request.get_json(force=True)
    request_id = body.get("id")
    method = body.get("method")

    if method == "SendMessage":
        return _jsonrpc_response(
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

    if method == "SendStreamingMessage":
        return Response(_streaming_events(request_id), mimetype="text/event-stream")

    if method == "GetTask":
        params = body.get("params") or {}
        task_id = params.get("id", "task-calendar-summary")
        return _jsonrpc_response(request_id, _task(task_id=task_id))

    return (
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": "Method not found"},
            }
        ),
        200,
        {"Content-Type": "application/json"},
    )
