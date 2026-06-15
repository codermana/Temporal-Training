package training.temporal.approval;

import io.temporal.workflow.Workflow;

public class ApprovalWorkflowImpl implements ApprovalWorkflow {
  private String requestId;
  private String status = "WAITING";
  private String note = "initial request";

  @Override
  public String run(String requestId) {
    this.requestId = requestId;
    Workflow.await(() -> status.startsWith("APPROVED") || status.startsWith("REJECTED"));
    return requestId + " " + status + " (" + note + ")";
  }

  @Override
  public void approve(String approver) {
    status = "APPROVED by " + approver;
  }

  @Override
  public void reject(String reason) {
    status = "REJECTED: " + reason;
  }

  @Override
  public String currentState() {
    return requestId + " " + status + " (" + note + ")";
  }

  @Override
  public String changeNote(String note) {
    this.note = note;
    return currentState();
  }

  @Override
  public void validateNote(String note) {
    if (note == null || note.isBlank()) {
      throw new IllegalArgumentException("note is required");
    }
  }
}

