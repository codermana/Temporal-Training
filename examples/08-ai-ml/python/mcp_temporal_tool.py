from mcp.server.fastmcp import FastMCP
from temporalio.client import Client

# Long-running interactive MCP tools, backed by Temporal.
#
# An MCP tool call is request/response and must return quickly, so it can't itself
# run a job that takes minutes or needs follow-up input. The fix: the tool handler
# starts (or signals) a Temporal Workflow and returns a handle. The durable work
# continues in Temporal; later tool calls signal/query the SAME Workflow by ID.
# This is the exact start/signal/query surface the agent loop already uses — here
# it's just exposed as MCP tools instead of called from Python.

mcp = FastMCP("temporal-agent")


async def _client() -> Client:
    return await Client.connect("localhost:7233")


@mcp.tool()
async def start_research(topic: str) -> str:
    """Kick off a long-running research agent. Returns immediately with a handle."""
    client = await _client()
    handle = await client.start_workflow(
        "AgentWorkflow",
        f"Research this topic thoroughly and use tools as needed: {topic}",
        id=f"research-{topic}",
        task_queue="agent-tq",
    )
    # Return fast — the Workflow runs durably for as long as it needs.
    return f"started:{handle.id}"


@mcp.tool()
async def send_message(workflow_id: str, message: str) -> str:
    """Send a follow-up turn to a running agent — keeps the tool interactive."""
    client = await _client()
    await client.get_workflow_handle(workflow_id).signal("user_says", message)
    return "delivered"


@mcp.tool()
async def get_conversation(workflow_id: str) -> list[dict]:
    """Read the live conversation/state without blocking on completion."""
    client = await _client()
    return await client.get_workflow_handle(workflow_id).query("conversation")


# Note the contrast with the *Temporal MCP server* (temporal-mcp-server): that
# exposes generic Temporal operations (start/signal/query/describe any Workflow) so
# an assistant can operate your namespace. This file is the inverse — a purpose-built
# tool whose durability happens to be Temporal. Both lean on the same client API.
