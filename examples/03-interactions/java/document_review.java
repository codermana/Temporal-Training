// Real-world human-in-the-loop: a document moves through review. Reviewers push
// comments via an Update (synchronous, validated, returns the running count)
// while the final publish/reject decision arrives as a Signal. run() blocks on a
// single Workflow.await that either decision satisfies, with an escalation
// timeout so a stalled review doesn't hang forever.
@WorkflowInterface
public interface DocumentReviewWorkflow {
  @WorkflowMethod
  String run(String docId);

  @UpdateMethod
  int addComment(String reviewer, String comment);

  @UpdateValidatorMethod(updateName = "addComment")
  void validateComment(String reviewer, String comment);

  @SignalMethod
  void publish(String approver);

  @SignalMethod
  void reject(String reason);

  @QueryMethod
  List<String> pendingComments();
}

class DocumentReviewWorkflowImpl implements DocumentReviewWorkflow {
  private final List<String> comments = new ArrayList<>();
  private String decision;

  @Override
  public String run(String docId) {
    // Auto-reject if no decision lands within the SLA window.
    boolean decided = Workflow.await(Duration.ofDays(3), () -> decision != null);
    if (!decided) {
      decision = "REJECTED: review SLA expired";
    }
    return docId + " -> " + decision + " (" + comments.size() + " comments)";
  }

  @Override
  public int addComment(String reviewer, String comment) {
    comments.add(reviewer + ": " + comment);
    return comments.size();
  }

  @Override
  public void validateComment(String reviewer, String comment) {
    if (comment == null || comment.isBlank()) {
      throw new IllegalArgumentException("comment must not be empty");
    }
  }

  @Override
  public void publish(String approver) {
    decision = "PUBLISHED by " + approver;
  }

  @Override
  public void reject(String reason) {
    decision = "REJECTED: " + reason;
  }

  @Override
  public List<String> pendingComments() {
    return List.copyOf(comments);
  }
}
