"""Reference implementation for A2A Python SDK."""

import time
from functools import cache

from a2a import types as a2a_types
from opentelemetry import metrics
from reference_shared import (
    flush_and_shutdown,
    mock_server_host_port,
    reference_tracer,
    setup_otel,
)

MOCK_A2A_URL = "http://127.0.0.1:8080/a2a"
PROTOCOL_VERSION = "1.0"

_reference_tracer = reference_tracer()

ROLE_USER = a2a_types.Role.Value("ROLE_USER")
ROLE_AGENT = a2a_types.Role.Value("ROLE_AGENT")
TASK_STATE_SUBMITTED = a2a_types.TaskState.Value("TASK_STATE_SUBMITTED")
TASK_STATE_WORKING = a2a_types.TaskState.Value("TASK_STATE_WORKING")
TASK_STATE_COMPLETED = a2a_types.TaskState.Value("TASK_STATE_COMPLETED")
TASK_STATE_FAILED = a2a_types.TaskState.Value("TASK_STATE_FAILED")
TASK_STATE_CANCELED = a2a_types.TaskState.Value("TASK_STATE_CANCELED")
TASK_STATE_INPUT_REQUIRED = a2a_types.TaskState.Value("TASK_STATE_INPUT_REQUIRED")
TASK_STATE_REJECTED = a2a_types.TaskState.Value("TASK_STATE_REJECTED")
TASK_STATE_AUTH_REQUIRED = a2a_types.TaskState.Value("TASK_STATE_AUTH_REQUIRED")


def _meter():
    return metrics.get_meter("gen_ai.reference")


@cache
def _metric_instruments():
    meter = _meter()
    return {
        "operation_duration": meter.create_histogram("a2a.client.operation.duration", unit="s"),
        "response_body_size": meter.create_histogram("a2a.client.response.body.size", unit="By"),
        "time_to_first_event": meter.create_histogram("a2a.client.response.time_to_first_event", unit="s"),
        "sse_event_count": meter.create_histogram("a2a.client.response.sse.event.count", unit="{event}"),
    }


def _message(text: str, *, message_id: str, role: int, task_id: str = ""):
    return a2a_types.Message(
        message_id=message_id,
        parts=[a2a_types.Part(text=text)],
        role=role,
        task_id=task_id,
    )


def _task(task_id: str, context_id: str, state: int, history: list | None = None):
    task = a2a_types.Task(
        id=task_id,
        context_id=context_id,
        status=a2a_types.TaskStatus(state=state),
    )
    if history:
        task.history.extend(history)
    return task


def _task_state_value(state: int) -> str:
    return {
        TASK_STATE_SUBMITTED: "submitted",
        TASK_STATE_WORKING: "working",
        TASK_STATE_COMPLETED: "completed",
        TASK_STATE_FAILED: "failed",
        TASK_STATE_CANCELED: "canceled",
        TASK_STATE_INPUT_REQUIRED: "input-required",
        TASK_STATE_REJECTED: "rejected",
        TASK_STATE_AUTH_REQUIRED: "auth-required",
    }.get(state, "unknown")


def _a2a_span_attrs(method: str, *, request_id: str, task=None, context_id: str | None = None) -> dict:
    host, port = mock_server_host_port(MOCK_A2A_URL)
    attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "jsonrpc.request.id": request_id,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
    }
    if host:
        attrs["server.address"] = host
    if port is not None:
        attrs["server.port"] = port
    if task:
        attrs["a2a.task.id"] = task.id
        attrs["a2a.task.state"] = _task_state_value(task.status.state)
        attrs["a2a.context.id"] = task.context_id
        attrs["gen_ai.conversation.id"] = task.context_id
    elif context_id:
        attrs["a2a.context.id"] = context_id
        attrs["gen_ai.conversation.id"] = context_id
    return attrs


def _record_operation_metrics(
    method: str,
    duration: float,
    body_size: int,
    *,
    task_state: str | None = None,
    gen_ai_operation_name: str | None = None,
) -> None:
    attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
    }
    host, port = mock_server_host_port(MOCK_A2A_URL)
    if host:
        attrs["server.address"] = host
    if port is not None:
        attrs["server.port"] = port
    if task_state:
        attrs["a2a.task.state"] = task_state
    if gen_ai_operation_name:
        attrs["gen_ai.operation.name"] = gen_ai_operation_name

    instruments = _metric_instruments()
    instruments["operation_duration"].record(duration, attrs)
    instruments["response_body_size"].record(body_size, attrs)


def run_message_send_reference() -> None:
    """Scenario: A2A JSON-RPC message/send with a task response."""
    print("  [message_send] A2A JSON-RPC message/send")
    method = "message/send"
    request_id = "req-message-send-1"
    request = a2a_types.SendMessageRequest(
        message=_message(
            "Summarize my calendar.",
            message_id="msg-user-1",
            role=ROLE_USER,
        ),
    )
    task = _task(
        "task-calendar-summary",
        "ctx-calendar",
        TASK_STATE_COMPLETED,
        history=[
            request.message,
            _message(
                "Your afternoon is open.",
                message_id="msg-agent-1",
                role=ROLE_AGENT,
                task_id="task-calendar-summary",
            ),
        ],
    )
    response = a2a_types.SendMessageResponse(task=task)
    start = time.perf_counter()
    attrs = _a2a_span_attrs(method, request_id=request_id, task=task)
    attrs["gen_ai.operation.name"] = "invoke_agent"
    with _reference_tracer.start_as_current_span(method, attributes=attrs) as span:
        span.set_attribute("gen_ai.request.stream", False)
    duration = time.perf_counter() - start
    _record_operation_metrics(
        method,
        duration,
        response.ByteSize(),
        task_state=_task_state_value(task.status.state),
        gen_ai_operation_name="invoke_agent",
    )
    print(f"    -> {task.id} {_task_state_value(task.status.state)}")


def run_message_stream_reference() -> None:
    """Scenario: A2A JSON-RPC message/stream with SSE task status events."""
    print("  [message_stream] A2A JSON-RPC message/stream")
    method = "message/stream"
    request_id = "req-message-stream-1"
    events = [
        a2a_types.StreamResponse(
            status_update=a2a_types.TaskStatusUpdateEvent(
                task_id="task-streaming",
                context_id="ctx-streaming",
                status=a2a_types.TaskStatus(state=TASK_STATE_WORKING),
            )
        ),
        a2a_types.StreamResponse(
            status_update=a2a_types.TaskStatusUpdateEvent(
                task_id="task-streaming",
                context_id="ctx-streaming",
                status=a2a_types.TaskStatus(state=TASK_STATE_COMPLETED),
            )
        ),
    ]
    start = time.perf_counter()
    first_event_at = None
    attrs = _a2a_span_attrs(
        method,
        request_id=request_id,
        context_id="ctx-streaming",
    )
    attrs["a2a.task.id"] = "task-streaming"
    attrs["a2a.task.state"] = "completed"
    attrs["gen_ai.operation.name"] = "invoke_agent"
    with _reference_tracer.start_as_current_span(method, attributes=attrs) as span:
        span.set_attribute("gen_ai.request.stream", True)
        response_size = 0
        for event in events:
            if first_event_at is None:
                first_event_at = time.perf_counter()
            response_size += event.ByteSize()
    duration = time.perf_counter() - start
    ttfb = (first_event_at or start) - start

    metric_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.task.state": "completed",
        "gen_ai.operation.name": "invoke_agent",
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
    }
    host, port = mock_server_host_port(MOCK_A2A_URL)
    if host:
        metric_attrs["server.address"] = host
    if port is not None:
        metric_attrs["server.port"] = port

    instruments = _metric_instruments()
    instruments["operation_duration"].record(duration, metric_attrs)
    instruments["time_to_first_event"].record(ttfb, metric_attrs)
    instruments["sse_event_count"].record(len(events), metric_attrs)
    instruments["response_body_size"].record(response_size, metric_attrs)
    print(f"    -> {len(events)} events")


def run_tasks_get_reference() -> None:
    """Scenario: A2A JSON-RPC tasks/get."""
    print("  [tasks_get] A2A JSON-RPC tasks/get")
    method = "tasks/get"
    request_id = "req-tasks-get-1"
    request = a2a_types.GetTaskRequest(
        id="task-calendar-summary",
    )
    task = _task("task-calendar-summary", "ctx-calendar", TASK_STATE_COMPLETED)
    response = task
    start = time.perf_counter()
    attrs = _a2a_span_attrs(method, request_id=request_id, task=task)
    with _reference_tracer.start_as_current_span(method, attributes=attrs):
        duration = time.perf_counter() - start
    _record_operation_metrics(
        method,
        duration,
        response.ByteSize(),
        task_state=_task_state_value(task.status.state),
    )
    print(f"    -> {request.id} {_task_state_value(task.status.state)}")


def main() -> None:
    print("=== Reference Implementation: A2A Python SDK ===")

    tp, lp, mp = setup_otel()

    run_message_send_reference()
    run_message_stream_reference()
    run_tasks_get_reference()

    flush_and_shutdown(tp, lp, mp)


if __name__ == "__main__":
    main()
