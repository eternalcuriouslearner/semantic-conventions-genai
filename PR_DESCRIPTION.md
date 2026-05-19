## Description

Adds `gen_ai.conversation.compacted`, a boolean GenAI conversation attribute
that indicates whether the effective conversation history used for an operation
is a compacted view of a longer prior conversation.

The attribute is added under the existing `gen_ai.conversation.*` namespace and
is included as `Recommended` when known on:

- `gen_ai.inference.client`
- `gen_ai.invoke_agent.client`
- `gen_ai.invoke_agent.internal`

The span-specific notes clarify layer ownership:

- `invoke_agent` spans report orchestrator or agent-framework compaction.
- `inference` spans report provider or client-SDK compaction before a model
  invocation.

If the attribute is unset, consumers should treat the compaction state as
unknown.

## Motivation

Long-running chat and agentic workloads routinely exceed model context windows.
To keep going, systems often compact conversation history by summarizing prior
turns, pruning older messages, applying a sliding window, using hierarchical
memory, or relying on provider-managed context reduction.

Today, OpenTelemetry GenAI spans can correlate turns with
`gen_ai.conversation.id`, but they do not indicate whether the model or agent
operated on the full conversation history or on a compacted representation.
That leaves a material observability gap for long-horizon GenAI workloads.

**User journey:** incident triage, quality evaluation, and cost analysis for
long-running conversations and agent runs.

When answer quality degrades late in a conversation, an operator needs to
distinguish model, prompt, retrieval, tool, and context-loss issues. If a span
has `gen_ai.conversation.compacted=true`, the operator can immediately filter
or group traces where context compaction may have affected the result, without
inspecting sensitive message payloads.

The same signal helps token and capacity analysis. A post-compaction turn may
have a much smaller `gen_ai.usage.input_tokens` value than the original
conversation length would suggest. Marking compacted turns gives cost and
capacity teams a first-class way to interpret token usage in long-running
sessions.

Evaluation and replay tooling also benefits from a queryable predicate: evaluate
only uncompacted turns, focus on compacted turns, or compare quality before and
after compaction starts.

**Prior art:** the concept exists today across both provider-managed and
framework-managed GenAI systems, even though it is not standardized in
OpenTelemetry.

Provider APIs and managed services may maintain server-side conversation state,
managed memory, context-window handling, or context reduction before invoking a
model. Agent frameworks and orchestrators such as LangChain, OpenAI Agents,
Claude Agent SDK-style coding assistants, Bedrock Agents, and similar systems
may compact between steps in a multi-turn run before issuing the next model
call.

This attribute gives providers, SDKs, and frameworks one low-cardinality,
non-sensitive signal for reporting that behavior consistently.

## Prototype

Reference scenarios were updated to demonstrate that the attribute can be
produced from existing library-facing instrumentation surfaces and consumed in
the reference support reports.

Updated scenarios:

- `reference/scenarios/openai/scenario.py`
  - emits `gen_ai.conversation.compacted=true` on inference spans
  - emits it on `gen_ai.client.inference.operation.details`
- `reference/scenarios/anthropic/scenario.py`
  - emits it on inference spans
  - emits it on `gen_ai.client.inference.operation.details`
- `reference/scenarios/claude-agent-sdk/scenario.py`
  - emits it on inference spans
- `reference/scenarios/openai-agents/scenario.py`
  - emits it on the `invoke_agent` span
  - emits it on the child inference span
- `reference/scenarios/aws-bedrock-agent/scenario.py`
  - emits it on the `invoke_agent` span
- `reference/scenarios/langchain/scenario.py`
  - emits it on an inference span

The generated reports now show support in:

- `reference/reports/inference-span.md`
- `reference/reports/invoke-agent-client-span.md`
- `reference/reports/invoke-agent-internal-span.md`
- `reference/reports/gen-ai-client-inference-operation-details-event.md`

The scenarios were run against the local mock server and validated with Weaver
live-check. The checked-in `data.json` files and reports were regenerated.

## Checklist

- [x] Motivation section filled in above
- [x] Reference scenarios updated for affected libraries
- [x] Changelog entry added under `Unreleased` in `CHANGELOG.md` for any change to the conventions that a consumer would care about. Editorial changes (typos, pure rewording, repo tooling) don't need an entry.

See [CONTRIBUTING.md](CONTRIBUTING.md).
