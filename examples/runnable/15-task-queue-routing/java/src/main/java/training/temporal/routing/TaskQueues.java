package training.temporal.routing;

/**
 * The three Task Queues this demo routes work across. Each name maps to one
 * Worker pool that registers only the subset of work routed to it:
 *
 * <ul>
 *   <li>{@link #ORDERS} - the orchestrator pool (registers the Workflow only).
 *   <li>{@link #PAYMENTS} - the payments pool (registers PaymentActivities only).
 *   <li>{@link #MEDIA} - the media/GPU pool (registers MediaActivities only).
 * </ul>
 *
 * The unit of "who registers what" is the Task Queue, not the Worker.
 */
final class TaskQueues {
  static final String ORDERS = "orders";
  static final String PAYMENTS = "payments";
  static final String MEDIA = "media";

  private TaskQueues() {}
}
