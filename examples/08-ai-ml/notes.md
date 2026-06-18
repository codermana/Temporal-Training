# Temporal for AI / ML

Why this fits a Temporal course: an AI agent loop, a tool call to an LLM, and an
ML training pipeline are all just **long-running, failure-prone, stateful
orchestration** — exactly what Temporal Workflows and Activities are for. Nothing
here is a new primitive. It is the same Workflow + Activity + Signal + Query +
retry/heartbeat model from Days 1–3, pointed at LLM and ML work.

These are teaching snippets (intentionally incomplete — see `examples/README.md`
§Format). Python is the primary stack because the AI/agent/MCP ecosystem is
Python-first; Java and Go versions cover the two patterns that map cleanly to the
core Temporal concepts the course already teaches (the agent loop and the ML
pipeline).

## Source material

| Resource | What it shows | Snippet(s) |
|---|---|---|
| [temporal.io/solutions/ai](https://temporal.io/solutions/ai) | The pitch: durable execution for AI workflows, agents, and pipelines | this file |
| [temporal-community/temporal-ai-agent](https://github.com/temporal-community/temporal-ai-agent) | A conversational agent as a Workflow: LLM + tools as Activities, user turns as Signals, state via Query | `*/ai_agent_workflow.*`, `python/llm_activity.py` |
| [temporal-mcp-server](https://temporal.io/code-exchange/temporal-mcp-server) | An MCP server that lets an AI assistant *drive* Temporal (start/signal/query Workflows) | `python/mcp_temporal_tool.py` (notes below) |
| [Long-running interactive MCP tools](https://temporal.io/blog/building-long-running-interactive-mcp-tools-temporal) | An MCP *tool* backed by a durable Workflow so a tool call can run for minutes/hours and stay interactive | `python/mcp_temporal_tool.py` |
| "AI/ML pipeline within Temporal?" | Ingest → preprocess → train → evaluate → deploy, each an Activity, with heartbeats + retries | `*/ml_pipeline_workflow.*` |

## Why Temporal for AI (temporal.io/solutions/ai)

LLM and ML systems fail in exactly the ways Temporal already handles:

- **Flaky model APIs.** A `client.messages.create(...)` call times out, rate-limits
  (429), or 529s. Wrap it in an **Activity** and you get automatic retries with
  backoff for free — the Workflow code never sees the transient failure. (See
  `python/llm_activity.py`.)
- **Long-running work.** Agent conversations span hours/days of human think-time;
  training jobs run for hours. Workflows are durable: the process can restart, the
  worker can be redeployed, and the Workflow resumes from history. Human turns are
  just **Signals** the Workflow `await`s — no polling, no DB of half-finished
  conversations.
- **Expensive, non-idempotent steps.** A completed LLM call or a finished training
  epoch is recorded in history and **never re-run on replay**. You don't pay for
  the same generation twice because a downstream step failed.
- **Visibility & control.** Query the live conversation or pipeline state; Signal a
  cancellation; inspect exactly which Activities ran in the Web UI. The orchestration
  is code you can test and version, not a tangle of queues and cron jobs.

Mental model: **the LLM is non-deterministic, so it can never run inside Workflow
code.** Every model call, embedding, vector-DB query, tool execution, and training
step is an **Activity**. The Workflow is the deterministic conductor that sequences
them, holds the conversation/pipeline state, and decides what happens next.

## The agent pattern (temporal-ai-agent)

A conversational agent is a Workflow that loops:

```
loop:
  user turn arrives  ──► Signal appends to the message history
  call the LLM       ──► Activity returns the next assistant message (+ maybe a tool request)
  if tool requested  ──► Activity executes the tool, result appended to history
  expose history     ──► Query so a UI can render the live conversation
  end when the LLM says "done" or the user ends the session
```

Key design points the reference repo makes:

- The **message history lives in Workflow state** (a list field). It is the durable
  source of truth; it survives worker restarts and is what you replay against.
- **`continue-as-new`** keeps history bounded. A chatty agent would grow an unbounded
  event history; periodically continue-as-new with the current messages as input
  (the same Day-5 pattern used for long-lived entities).
- **Human-in-the-loop confirmation** (e.g. "run this tool? y/n") is just another
  Signal the Workflow `await`s before executing a sensitive tool Activity — the same
  shape as the Day-3 human-approval example.

## The MCP angle (mcp-server + long-running interactive tools)

Two distinct ideas, easy to conflate:

1. **Temporal MCP server** — an MCP server whose *tools are Temporal operations*
   (`start_workflow`, `signal_workflow`, `query_workflow`, `describe`). It lets an AI
   assistant operate your Temporal namespace in natural language. Temporal is the
   thing being controlled.

2. **Long-running interactive MCP tools** (the blog) — flip it around. An MCP *tool
   call* is normally request/response and must return fast, which rules out work that
   takes minutes or needs follow-up input. Back the tool with a Temporal Workflow and
   the tool handler becomes: **start (or signal) a Workflow, then return a handle.**
   The durable work continues in Temporal; later tool calls `signal`/`query` the same
   Workflow by ID. The agent gets a tool that can run for hours and stay interactive,
   and Temporal owns the durability. This is the same start/signal/query API surface
   the agent loop already uses — `python/mcp_temporal_tool.py` shows the handler.

## The ML pipeline pattern

A training/inference pipeline is a plain Workflow sequencing Activities:

```
ingest ──► preprocess ──► train (heartbeating) ──► evaluate ──► gate ──► deploy
```

What Temporal buys you over a DAG scheduler (the Day-1 Airflow-vs-Temporal point,
applied to ML):

- **Heartbeats + checkpoints** on the long training Activity: report epoch progress,
  resume from the last checkpoint on retry instead of restarting from epoch 0
  (Day-2 `heartbeat_resume_from_checkpoint`).
- **A quality gate in code.** "Deploy only if eval accuracy ≥ threshold" is an `if`
  in the Workflow, with a Signal for manual approval when it's borderline — not a
  separate approval service.
- **Per-step retry policy.** Ingest from a flaky source retries differently than a
  GPU training step you don't want to blindly re-run; each Activity sets its own
  `RetryOptions`/timeouts.
- **Idempotency.** Pass a deterministic run/model ID into Activities so a retried
  deploy or upload doesn't duplicate artifacts.

## Models in the snippets

Examples call Claude via the Anthropic SDK using `claude-opus-4-8` (current Opus;
swap to `claude-sonnet-4-6` for cheaper high-volume calls). The provider is
incidental — the Temporal pattern is identical for any LLM or ML framework. The one
rule that never changes: **the SDK call goes in an Activity, never in Workflow code.**
