"""Reference implementation for A2A Python SDK."""

import asyncio
import json
import os
import time
from functools import cache

import httpx
from a2a import types as a2a_types
from a2a.client import ClientConfig, create_client, minimal_agent_card
from a2a.utils.constants import TransportProtocol
from opentelemetry import metrics
from reference_shared import (
    flush_and_shutdown,
    mock_server_host_port,
    reference_tracer,
    setup_otel,
)

MOCK_A2A_URL = f"{os.environ.get('MOCK_LLM_URL', 'http://127.0.0.1:8080').rstrip('/')}/a2a"
PROTOCOL_VERSION = "1.0"

_reference_tracer = reference_tracer()

ROLE_USER = a2a_types.Role.Value("ROLE_USER")
TASK_STATE_COMPLETED = a2a_types.TaskState.Value("TASK_STATE_COMPLETED")


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


def _message(text: str, *, message_id: str):
    return a2a_types.Message(
        message_id=message_id,
        parts=[a2a_types.Part(text=text)],
        role=ROLE_USER,
    )


def _task_state_value(state: int) -> str:
    return {
        TASK_STATE_COMPLETED: "completed",
        a2a_types.TaskState.Value("TASK_STATE_SUBMITTED"): "submitted",
        a2a_types.TaskState.Value("TASK_STATE_WORKING"): "working",
        a2a_types.TaskState.Value("TASK_STATE_FAILED"): "failed",
        a2a_types.TaskState.Value("TASK_STATE_CANCELED"): "canceled",
        a2a_types.TaskState.Value("TASK_STATE_INPUT_REQUIRED"): "input-required",
        a2a_types.TaskState.Value("TASK_STATE_REJECTED"): "rejected",
        a2a_types.TaskState.Value("TASK_STATE_AUTH_REQUIRED"): "auth-required",
    }.get(state, "unknown")


def _capture_jsonrpc_request(observed_requests: dict[str, str]):
    async def capture_request(request: httpx.Request) -> None:
        payload = json.loads(request.content.decode("utf-8"))
        observed_requests[payload["method"]] = str(payload["id"])

    return capture_request


async def _create_a2a_client(*, streaming: bool, observed_requests: dict[str, str]):
    httpx_client = httpx.AsyncClient(event_hooks={"request": [_capture_jsonrpc_request(observed_requests)]})
    card = minimal_agent_card(MOCK_A2A_URL, [TransportProtocol.JSONRPC])
    card.capabilities.streaming = streaming
    return await create_client(
        card,
        ClientConfig(streaming=streaming, httpx_client=httpx_client),
    )


def _server_attrs():
    host, port = mock_server_host_port(MOCK_A2A_URL)
    attrs = {}
    if host:
        attrs["server.address"] = host
    if port is not None:
        attrs["server.port"] = port
    return attrs


async def run_message_send_reference() -> None:
    """Scenario: A2A JSON-RPC message/send with a task response."""
    print("  [message_send] A2A JSON-RPC message/send")
    method = "message/send"
    observed_requests = {}
    request = a2a_types.SendMessageRequest(
        message=_message(
            "Summarize my calendar.",
            message_id="msg-user-1",
        ),
    )

    start = time.perf_counter()
    response = None
    task = None
    span_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "gen_ai.operation.name": "invoke_agent",
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    with _reference_tracer.start_as_current_span(method, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False, observed_requests=observed_requests) as client:
            response = await anext(client.send_message(request))
        task = response.task
        task_state = _task_state_value(task.status.state)
        span.set_attribute("jsonrpc.request.id", observed_requests["SendMessage"])
        span.set_attribute("a2a.task.id", task.id)
        span.set_attribute("a2a.task.state", task_state)
        span.set_attribute("a2a.context.id", task.context_id)
        span.set_attribute("gen_ai.conversation.id", task.context_id)
        duration = time.perf_counter() - start

    metric_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.task.state": task_state,
        "gen_ai.operation.name": "invoke_agent",
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    instruments = _metric_instruments()
    instruments["operation_duration"].record(duration, metric_attrs)
    instruments["response_body_size"].record(response.ByteSize(), metric_attrs)
    print(f"    -> {task.id} {task_state}")


async def run_message_stream_reference() -> None:
    """Scenario: A2A JSON-RPC message/stream with SSE task status events."""
    print("  [message_stream] A2A JSON-RPC message/stream")
    method = "message/stream"
    observed_requests = {}
    request = a2a_types.SendMessageRequest(
        message=_message(
            "Track this task.",
            message_id="msg-user-2",
        ),
    )

    start = time.perf_counter()
    first_event_at = None
    event_count = 0
    response_size = 0
    task_id = None
    context_id = None
    task_state = None
    span_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.request.stream": True,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    with _reference_tracer.start_as_current_span(method, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=True, observed_requests=observed_requests) as client:
            async for event in client.send_message(request):
                if first_event_at is None:
                    first_event_at = time.perf_counter()
                event_count += 1
                response_size += event.ByteSize()
                if event.HasField("status_update"):
                    task_id = event.status_update.task_id
                    context_id = event.status_update.context_id
                    task_state = _task_state_value(event.status_update.status.state)

        span.set_attribute("jsonrpc.request.id", observed_requests["SendStreamingMessage"])
        span.set_attribute("a2a.task.id", task_id)
        span.set_attribute("a2a.task.state", task_state)
        span.set_attribute("a2a.context.id", context_id)
        span.set_attribute("gen_ai.conversation.id", context_id)
        duration = time.perf_counter() - start
        ttfb = (first_event_at or start) - start

    metric_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.task.state": task_state,
        "gen_ai.operation.name": "invoke_agent",
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    instruments = _metric_instruments()
    instruments["operation_duration"].record(duration, metric_attrs)
    instruments["time_to_first_event"].record(ttfb, metric_attrs)
    instruments["sse_event_count"].record(event_count, metric_attrs)
    instruments["response_body_size"].record(response_size, metric_attrs)
    print(f"    -> {event_count} events")


async def run_tasks_get_reference() -> None:
    """Scenario: A2A JSON-RPC tasks/get."""
    print("  [tasks_get] A2A JSON-RPC tasks/get")
    method = "tasks/get"
    observed_requests = {}
    request = a2a_types.GetTaskRequest(
        id="task-calendar-summary",
    )

    start = time.perf_counter()
    span_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    with _reference_tracer.start_as_current_span(method, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False, observed_requests=observed_requests) as client:
            task = await client.get_task(request)
        task_state = _task_state_value(task.status.state)
        span.set_attribute("jsonrpc.request.id", observed_requests["GetTask"])
        span.set_attribute("a2a.task.id", task.id)
        span.set_attribute("a2a.task.state", task_state)
        span.set_attribute("a2a.context.id", task.context_id)
        span.set_attribute("gen_ai.conversation.id", task.context_id)
        duration = time.perf_counter() - start

    metric_attrs = {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.task.state": task_state,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }
    instruments = _metric_instruments()
    instruments["operation_duration"].record(duration, metric_attrs)
    instruments["response_body_size"].record(task.ByteSize(), metric_attrs)
    print(f"    -> {request.id} {task_state}")


async def run_scenarios() -> None:
    await run_message_send_reference()
    await run_message_stream_reference()
    await run_tasks_get_reference()


def main() -> None:
    print("=== Reference Implementation: A2A Python SDK ===")

    tp, lp, mp = setup_otel()

    asyncio.run(run_scenarios())

    flush_and_shutdown(tp, lp, mp)


if __name__ == "__main__":
    main()
