"""Reference implementation for A2A Python SDK."""

import asyncio
import json
import os

import httpx
from a2a import types as a2a_types
from a2a.client import ClientConfig, create_client, minimal_agent_card
from a2a.utils.constants import TransportProtocol
from reference_shared import (
    flush_and_shutdown,
    mock_server_host_port,
    reference_tracer,
    setup_otel,
)

MOCK_A2A_URL = f"{os.environ.get('MOCK_LLM_URL', 'http://127.0.0.1:8080').rstrip('/')}/a2a"
PROTOCOL_VERSION = "1.0"
PROTOCOL_BINDING = "JSONRPC"
AGENT_NAME = "CalendarAgent"
AGENT_CARD_URL = f"{MOCK_A2A_URL}/.well-known/agent-card.json"
REQUESTED_EXTENSIONS = ["https://a2a-protocol.org/example/extensions/auth-forward/v1"]

_reference_tracer = reference_tracer()

ROLE_USER = a2a_types.Role.Value("ROLE_USER")
TASK_STATE_COMPLETED = a2a_types.TaskState.Value("TASK_STATE_COMPLETED")


def _message(text: str, *, message_id: str, reference_task_ids: list[str] | None = None):
    return a2a_types.Message(
        message_id=message_id,
        parts=[a2a_types.Part(text=text)],
        role=ROLE_USER,
        extensions=REQUESTED_EXTENSIONS,
        reference_task_ids=reference_task_ids or [],
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
    card.name = AGENT_NAME
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


def _base_span_attrs(method: str):
    return {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.protocol.binding": PROTOCOL_BINDING,
        "a2a.agent.card.url": AGENT_CARD_URL,
        "gen_ai.agent.name": AGENT_NAME,
        "network.protocol.name": "http",
        "network.transport": "tcp",
        "rpc.system.name": "jsonrpc",
        **_server_attrs(),
    }


def _set_task_response_attrs(span, task_id, task_state, context_id, artifact_ids=None):
    span.set_attribute("a2a.task.id", task_id)
    span.set_attribute("a2a.task.state", task_state)
    span.set_attribute("a2a.context.id", context_id)
    span.set_attribute("gen_ai.conversation.id", context_id)
    if artifact_ids:
        span.set_attribute("a2a.task.artifact_ids", artifact_ids)


async def run_message_send_reference() -> None:
    """Scenario: A2A JSON-RPC message/send with a task response."""
    print("  [message_send] A2A JSON-RPC message/send")
    method = "message/send"
    observed_requests = {}
    reference_task_ids = ["task-calendar-summary"]
    request = a2a_types.SendMessageRequest(
        message=_message(
            "Summarize my calendar.",
            message_id="msg-user-1",
            reference_task_ids=reference_task_ids,
        ),
    )

    span_attrs = {
        **_base_span_attrs(method),
        "a2a.message.id": "msg-user-1",
        "a2a.message.referenced_task_ids": reference_task_ids,
        "a2a.protocol.requested_extensions": REQUESTED_EXTENSIONS,
        "gen_ai.operation.name": "invoke_agent",
    }
    with _reference_tracer.start_as_current_span(f"{method} {AGENT_NAME}", attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False, observed_requests=observed_requests) as client:
            response = await anext(client.send_message(request))
        task = response.task
        task_state = _task_state_value(task.status.state)
        span.set_attribute("jsonrpc.request.id", observed_requests["SendMessage"])
        _set_task_response_attrs(
            span,
            task.id,
            task_state,
            task.context_id,
            [artifact.artifact_id for artifact in task.artifacts],
        )
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

    event_count = 0
    task_id = None
    context_id = None
    task_state = None
    span_attrs = {
        **_base_span_attrs(method),
        "a2a.message.id": "msg-user-2",
        "a2a.protocol.requested_extensions": REQUESTED_EXTENSIONS,
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.request.stream": True,
    }
    with _reference_tracer.start_as_current_span(f"{method} {AGENT_NAME}", attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=True, observed_requests=observed_requests) as client:
            async for event in client.send_message(request):
                event_count += 1
                if event.HasField("status_update"):
                    task_id = event.status_update.task_id
                    context_id = event.status_update.context_id
                    task_state = _task_state_value(event.status_update.status.state)

        span.set_attribute("jsonrpc.request.id", observed_requests["SendStreamingMessage"])
        _set_task_response_attrs(span, task_id, task_state, context_id)
    print(f"    -> {event_count} events")


async def run_tasks_get_reference() -> None:
    """Scenario: A2A JSON-RPC tasks/get."""
    print("  [tasks_get] A2A JSON-RPC tasks/get")
    method = "tasks/get"
    observed_requests = {}
    request = a2a_types.GetTaskRequest(
        id="task-calendar-summary",
    )

    span_attrs = _base_span_attrs(method)
    with _reference_tracer.start_as_current_span(f"{method} {AGENT_NAME}", attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False, observed_requests=observed_requests) as client:
            task = await client.get_task(request)
        task_state = _task_state_value(task.status.state)
        span.set_attribute("jsonrpc.request.id", observed_requests["GetTask"])
        _set_task_response_attrs(
            span,
            task.id,
            task_state,
            task.context_id,
            [artifact.artifact_id for artifact in task.artifacts],
        )
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
