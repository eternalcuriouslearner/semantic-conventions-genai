"""Reference implementation for A2A Python SDK."""

import asyncio
import os

import httpx
from a2a import types as a2a_types
from a2a.client import ClientConfig, create_client, minimal_agent_card
from a2a.utils.constants import TransportProtocol
from opentelemetry.trace import SpanKind
from reference_shared import (
    flush_and_shutdown,
    mock_server_host_port,
    reference_tracer,
    setup_otel,
)

MOCK_A2A_URL = f"{os.environ.get('MOCK_LLM_URL', 'http://127.0.0.1:8080').rstrip('/')}/a2a"
PROTOCOL_VERSION = "1.0"
PROTOCOL_BINDING = "JSONRPC"
AGENT_CARD_URL = f"{MOCK_A2A_URL}/.well-known/agent-card.json"

_reference_tracer = reference_tracer()

ROLE_USER = a2a_types.Role.Value("ROLE_USER")


def _message(text: str, *, message_id: str, reference_task_ids: list[str] | None = None):
    return a2a_types.Message(
        message_id=message_id,
        parts=[a2a_types.Part(text=text)],
        role=ROLE_USER,
        reference_task_ids=reference_task_ids or [],
    )


def _task_state_value(state: int) -> str:
    return a2a_types.TaskState.Name(state)


async def _create_a2a_client(*, streaming: bool):
    httpx_client = httpx.AsyncClient()
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


def _base_span_attrs(method: str):
    return {
        "a2a.method.name": method,
        "a2a.protocol.version": PROTOCOL_VERSION,
        "a2a.protocol.binding": PROTOCOL_BINDING,
        "a2a.agent_card.url": AGENT_CARD_URL,
        **_server_attrs(),
    }


def _set_task_response_attrs(span, task_id, task_state, context_id):
    span.set_attribute("a2a.task.id", task_id)
    span.set_attribute("a2a.task.state", task_state)
    span.set_attribute("gen_ai.conversation.id", context_id)


async def run_message_send_reference() -> None:
    """Scenario: A2A JSON-RPC send_message with a task response."""
    print("  [send_message] A2A JSON-RPC send_message")
    method = "send_message"
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
        "a2a.message.reference_task_ids": reference_task_ids,
        "gen_ai.operation.name": "invoke_agent",
    }
    with _reference_tracer.start_as_current_span(method, kind=SpanKind.CLIENT, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False) as client:
            response = await anext(client.send_message(request))
        task = response.task
        task_state = _task_state_value(task.status.state)
        _set_task_response_attrs(
            span,
            task.id,
            task_state,
            task.context_id,
        )
    print(f"    -> {task.id} {task_state}")


async def run_message_stream_reference() -> None:
    """Scenario: A2A JSON-RPC send_streaming_message with SSE task status events."""
    print("  [send_streaming_message] A2A JSON-RPC send_streaming_message")
    method = "send_streaming_message"
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
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.request.stream": True,
    }
    with _reference_tracer.start_as_current_span(method, kind=SpanKind.CLIENT, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=True) as client:
            async for event in client.send_message(request):
                event_count += 1
                if event.HasField("status_update"):
                    task_id = event.status_update.task_id
                    context_id = event.status_update.context_id
                    task_state = _task_state_value(event.status_update.status.state)

        _set_task_response_attrs(span, task_id, task_state, context_id)
    print(f"    -> {event_count} events")


async def run_tasks_get_reference() -> None:
    """Scenario: A2A JSON-RPC get_task."""
    print("  [get_task] A2A JSON-RPC get_task")
    method = "get_task"
    request = a2a_types.GetTaskRequest(
        id="task-calendar-summary",
    )

    span_attrs = _base_span_attrs(method)
    with _reference_tracer.start_as_current_span(method, kind=SpanKind.CLIENT, attributes=span_attrs) as span:
        async with await _create_a2a_client(streaming=False) as client:
            task = await client.get_task(request)
        task_state = _task_state_value(task.status.state)
        _set_task_response_attrs(
            span,
            task.id,
            task_state,
            task.context_id,
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
