// A conversational agent as a Workflow: it owns the message history as durable
// state, blocks on a Signal for each human turn, drives the LLM (and tools) as
// Activities, and exposes the live conversation via Query. The LLM call is
// non-deterministic, so callLlm() is an Activity, never Workflow code.
@WorkflowInterface
interface AgentWorkflow {
  @WorkflowMethod
  List<Message> run(String systemPrompt);

  @SignalMethod
  void userSays(String message);

  @SignalMethod
  void endSession();

  @QueryMethod
  List<Message> conversation();
}

class AgentWorkflowImpl implements AgentWorkflow {
  private final LlmActivities llm =
      Workflow.newActivityStub(
          LlmActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(2))
              .build());

  private final List<Message> messages = new ArrayList<>();
  private boolean pendingUserTurn = false;
  private boolean ended = false;

  @Override
  public List<Message> run(String systemPrompt) {
    messages.add(new Message("user", systemPrompt));

    while (!ended) {
      // Each LLM call is recorded in history and never re-run on replay.
      Message assistant = llm.callLlm(messages);
      messages.add(assistant);

      // No tool to run (a real agent would branch on a tool_use block): wait for
      // the next human turn delivered by Signal, or for the session to end.
      pendingUserTurn = false;
      Workflow.await(() -> pendingUserTurn || ended);

      // Keep history bounded on long chats — continue-as-new (Day-5 pattern).
      if (messages.size() > 200 && !ended) {
        Workflow.continueAsNew(systemPrompt);
      }
    }
    return messages;
  }

  @Override
  public void userSays(String message) {
    messages.add(new Message("user", message));
    pendingUserTurn = true;
  }

  @Override
  public void endSession() {
    ended = true;
  }

  @Override
  public List<Message> conversation() {
    return messages; // a UI polls this to render the live conversation
  }
}
