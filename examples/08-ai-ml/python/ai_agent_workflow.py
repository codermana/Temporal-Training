from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from llm_activity import call_llm
    from tools import run_tool  # Activity: executes one tool, returns a result dict


# A conversational agent IS a Workflow: it owns the message history as durable
# state, blocks on a Signal for each human turn, drives the LLM + tools as
# Activities, and exposes the live conversation via Query. This is the
# temporal-ai-agent pattern, reduced to its skeleton.
@workflow.defn
class AgentWorkflow:
    def __init__(self) -> None:
        self._messages: list[dict] = []
        self._pending_user_turn = False
        self._ended = False

    @workflow.run
    async def run(self, system_prompt: str) -> list[dict]:
        self._messages.append({"role": "user", "content": system_prompt})

        while not self._ended:
            # Each LLM call is an Activity — retried, recorded, never re-run on replay.
            assistant = await workflow.execute_activity(
                call_llm, self._messages, start_to_close_timeout=timedelta(minutes=2)
            )
            self._messages.append(assistant)

            # If the model asked for a tool, run it as an Activity and feed the
            # result back. A real agent parses a tool_use block; kept literal here.
            tool_request = _extract_tool_request(assistant)
            if tool_request:
                result = await workflow.execute_activity(
                    run_tool,
                    tool_request,
                    start_to_close_timeout=timedelta(minutes=5),
                )
                self._messages.append({"role": "user", "content": result})
                continue  # let the model react to the tool result

            # No tool to run: wait for the next human turn (Signal) or session end.
            self._pending_user_turn = False
            await workflow.wait_condition(
                lambda: self._pending_user_turn or self._ended
            )

            # Keep history bounded on long chats: continue-as-new with current
            # messages so the event history doesn't grow without limit (Day-5).
            if len(self._messages) > 200 and not self._ended:
                workflow.continue_as_new(args=[system_prompt])

        return self._messages

    @workflow.signal
    def user_says(self, message: str) -> None:
        self._messages.append({"role": "user", "content": message})
        self._pending_user_turn = True

    @workflow.signal
    def end_session(self) -> None:
        self._ended = True

    @workflow.query
    def conversation(self) -> list[dict]:
        # A UI polls this to render the live conversation without touching a DB.
        return self._messages


def _extract_tool_request(_assistant: dict) -> dict | None:
    return None  # placeholder: real code inspects the model's tool_use block
