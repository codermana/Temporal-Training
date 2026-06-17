// Routing Activities to different Worker pools by Task Queue. One Workflow, but
// each Activity stub names the Task Queue whose pool registers that Activity.
// The Workflow itself runs on "orders"; charge() runs on the payments pool;
// render() runs on the media (GPU) pool. No single Worker registers all three —
// the unit of "who registers what" is the Task Queue, not the Worker.
//
// Variants:
//   - Omit setTaskQueue          -> the Activity runs on the Workflow's own queue.
//   - Same queue, more replicas  -> every replica registers the SAME full set.
//   - Route to a queue no pool registers -> the task retries until it times out.
//
// Runnable end-to-end: examples/runnable/15-task-queue-routing (make run-routing).
class OrderWorkflowImpl implements OrderWorkflow {

  // payments pool: cheap, high-replica; registers only PaymentActivities.
  private final PaymentActivities pay =
      Workflow.newActivityStub(
          PaymentActivities.class,
          ActivityOptions.newBuilder()
              .setTaskQueue("payments")
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .build());

  // media pool: scarce GPU boxes; registers only MediaActivities.
  private final MediaActivities media =
      Workflow.newActivityStub(
          MediaActivities.class,
          ActivityOptions.newBuilder()
              .setTaskQueue("media")
              .setStartToCloseTimeout(Duration.ofMinutes(5))
              .build());

  @Override
  public String process(String orderId) {
    String charged = pay.charge(orderId);   // dispatched to the "payments" queue
    String receipt = media.render(orderId); // dispatched to the "media" queue
    return orderId + ": " + charged + " / " + receipt;
  }
}
