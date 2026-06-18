from anthropic import Anthropic
from temporalio import activity

# The LLM call is non-deterministic and network-bound, so it MUST be an Activity,
# never Workflow code. As an Activity it gets Temporal's retries for free: a 429,
# 529, or timeout from the model API is just a failed attempt that Temporal backs
# off and re-runs. The Workflow never sees the transient failure. The Anthropic
# SDK also retries internally (429/5xx); Temporal is the durable outer loop on top.
_client = Anthropic()  # reads ANTHROPIC_API_KEY


@activity.defn
async def call_llm(messages: list[dict]) -> dict:
    # `messages` is the full conversation history the Workflow owns and replays.
    # We return a plain dict (role/content) so the result lands in Workflow history
    # as durable, replay-safe state.
    resp = _client.messages.create(
        model="claude-opus-4-8",
        max_tokens=4096,
        messages=messages,
    )
    text = next((b.text for b in resp.content if b.type == "text"), "")
    return {"role": "assistant", "content": text}


# A long generation (or a streamed agentic call) should heartbeat so the server can
# detect a dead worker and so a Workflow cancel reaches it. Same heartbeat pattern
# as the Day-2 long-Activity example — the work just happens to be an LLM call.
@activity.defn
async def call_llm_streaming(messages: list[dict]) -> dict:
    chunks: list[str] = []
    with _client.messages.stream(
        model="claude-opus-4-8", max_tokens=8192, messages=messages
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
            activity.heartbeat(len(chunks))  # progress + liveness
    return {"role": "assistant", "content": "".join(chunks)}
