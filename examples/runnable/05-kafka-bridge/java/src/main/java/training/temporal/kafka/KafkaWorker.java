package training.temporal.kafka;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.util.concurrent.CountDownLatch;

/**
 * Day 3 lab entrypoint. Starts a Temporal Worker on the {@code orders} Task Queue
 * and a {@link KafkaSignalBridge} that consumes the {@code orders} topic and
 * {@code signalWithStart}s one Workflow per order key.
 *
 * <pre>
 *   make stack-kafka        # KRaft broker on :9092
 *   make run-kafka          # this Worker + bridge
 *
 *   kcat -b localhost:9092 -t orders -P -k "order-1" &lt;&lt;&lt; 'NEW:line-item-A'
 *   kcat -b localhost:9092 -t order-outcomes -C -o end -f 'key=%k value=%s\n'
 * </pre>
 */
public class KafkaWorker {
  private static final String TASK_QUEUE = "orders";

  public static void main(String[] args) throws InterruptedException {
    String bootstrapServers =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    String inputTopic = System.getenv().getOrDefault("KAFKA_ORDERS_TOPIC", "orders");
    String outcomeTopic = System.getenv().getOrDefault("KAFKA_OUTCOMES_TOPIC", "order-outcomes");

    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);
    worker.registerActivitiesImplementations(
        new KafkaOutcomeActivities(bootstrapServers, outcomeTopic));
    factory.start();

    KafkaSignalBridge bridge =
        new KafkaSignalBridge(client, TASK_QUEUE, bootstrapServers, inputTopic);
    Thread bridgeThread = new Thread(bridge, "kafka-signal-bridge");
    bridgeThread.setDaemon(true);
    bridgeThread.start();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  bridgeThread.interrupt();
                  factory.shutdown();
                }));

    System.out.printf(
        "Kafka bridge running. brokers=%s in=%s out=%s taskQueue=%s. Ctrl+C to stop.%n",
        bootstrapServers, inputTopic, outcomeTopic, TASK_QUEUE);

    new CountDownLatch(1).await();
  }
}
