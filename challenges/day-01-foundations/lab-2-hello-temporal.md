# Lab 1.2 — Hello Temporal: your first Workflow

**Time:** ~50 min · **Difficulty:** ★ · **Stack:** Temporal dev server

## Scenario

You'll build the smallest meaningful Temporal application: a Workflow that calls
a single Activity and returns a greeting. It looks trivial, but wiring the four
moving parts — Activity, Workflow, Worker, and starter — is the foundation for
everything else.

## Learning goals

- Define an Activity and a Workflow with the SDK annotations.
- Register both on a Worker bound to a Task Queue.
- Start a Workflow from a client and read its result.
- See why business logic lives in **Activities**, not in Workflow code.

> **Coming from Airflow `[airflow]`:** your Workflow is the DAG, the Activity is
> an Operator, the Worker is the Executor, and `WorkflowClient.start(...)` is the
> scheduler firing a DAG run. Unlike a DAG, the Workflow is plain Java — you get
> loops, conditionals, and try/catch for free.

## Prerequisites

- Lab 1.1 complete; `make temporal` running.

## Starter code

Create a fresh Maven module. From the repo root:

```bash
mkdir -p work/day-01/hello/src/main/java/training/temporal/hello
```

**`work/day-01/hello/pom.xml`**

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>training.temporal</groupId>
  <artifactId>hello-temporal</artifactId>
  <version>1.0-SNAPSHOT</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <temporal.version>1.32.1</temporal.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>io.temporal</groupId>
      <artifactId>temporal-sdk</artifactId>
      <version>${temporal.version}</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.13</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.13.0</version>
        <configuration>
          <release>17</release>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>3.3.0</version>
        <configuration>
          <mainClass>training.temporal.hello.HelloWorker</mainClass>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

The **contracts** are given (interfaces are not the "solution" — the
implementations are). Paste these two files as-is:

**`GreetingActivities.java`**

```java
package training.temporal.hello;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface GreetingActivities {
  @ActivityMethod
  String composeGreeting(String name);
}
```

**`GreetingWorkflow.java`**

```java
package training.temporal.hello;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface GreetingWorkflow {
  @WorkflowMethod
  String greet(String name);
}
```

Now the three files you must complete. Stubs with `// TODO`:

**`GreetingActivitiesImpl.java`**

```java
package training.temporal.hello;

public class GreetingActivitiesImpl implements GreetingActivities {
  @Override
  public String composeGreeting(String name) {
    // TODO: return a greeting string built from `name`.
    // This is ordinary Java — I/O, randomness, and clocks are all allowed here.
    throw new UnsupportedOperationException("TODO");
  }
}
```

**`GreetingWorkflowImpl.java`**

```java
package training.temporal.hello;

import io.temporal.workflow.Workflow;

public class GreetingWorkflowImpl implements GreetingWorkflow {

  // TODO: create an Activity stub with Workflow.newActivityStub(...).
  //       You MUST set at least a startToCloseTimeout via ActivityOptions.

  @Override
  public String greet(String name) {
    // TODO: call the Activity through the stub and return its result.
    //       Do NOT call GreetingActivitiesImpl directly — go through the stub.
    throw new UnsupportedOperationException("TODO");
  }
}
```

The Worker and the starter (client) are **two separate, standalone
processes** — exactly as you'd deploy them in production. They never talk to
each other directly; both connect to the Temporal server and agree on the
`hello-temporal` Task Queue. You'll run the Worker in one terminal and the
starter in another.

**`HelloWorker.java`** — the standalone Worker (registers + polls forever):

```java
package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.WorkerFactory;

public class HelloWorker {
  private static final String TASK_QUEUE = "hello-temporal";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    // TODO 1: create a Worker on TASK_QUEUE.
    // TODO 2: register GreetingWorkflowImpl as a workflow implementation type.
    // TODO 3: register a new GreetingActivitiesImpl() as an activities implementation.
    // TODO 4: start the factory.

    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}
```

**`HelloStarter.java`** — the standalone client (starts one Workflow, prints
its result, then exits):

```java
package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class HelloStarter {
  private static final String TASK_QUEUE = "hello-temporal";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    // TODO 5: create a typed workflow stub with WorkflowOptions
    //         (set the task queue, and a workflowId of your choice).
    // TODO 6: call greet("Ada"), print the result, then System.exit(0).
  }
}
```

> The Worker doesn't have `factory.shutdown()` — it stays alive polling the
> queue. The starter is short-lived: it fires the Workflow and exits.

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference solution: [`examples/runnable/01-hello-temporal/python`](../../examples/runnable/01-hello-temporal/python)
and [`.../go`](../../examples/runnable/01-hello-temporal/go).

Same split: the Worker definitions live in a shared module, with a worker
process and a separate starter process importing them.

**Python** (`temporalio`) — shared defs in `greeting.py`, then `worker.py` and
`starter.py`:

```python
# greeting.py — shared by both processes
from temporalio import activity, workflow

TASK_QUEUE = "hello-temporal"

@activity.defn
async def compose_greeting(name: str) -> str:
    return f"Hello, {name} from a Temporal Activity"

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def greet(self, name: str) -> str:
        # TODO: execute_activity(compose_greeting, name, start_to_close_timeout=...)
        ...

# worker.py — standalone Worker (polls forever)
async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")
    worker = Worker(client, task_queue=TASK_QUEUE,
                    workflows=[GreetingWorkflow], activities=[compose_greeting])
    print(f"Worker started on task queue '{TASK_QUEUE}'. Ctrl-C to stop.")
    await worker.run()

# starter.py — standalone client (starts one Workflow, prints, exits)
async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")
    # TODO: execute_workflow(GreetingWorkflow.greet, "Ada", id=..., task_queue=TASK_QUEUE)
```

**Go** (`go.temporal.io/sdk`) — shared defs in a `hello` package, with `./worker`
and `./starter` command dirs importing it:

```go
// greeting.go (package hello) — shared by both commands
const TaskQueue = "hello-temporal"

func ComposeGreeting(ctx context.Context, name string) (string, error) {
    return "Hello, " + name + " from a Temporal Activity", nil
}

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Second,
    })
    var greeting string
    // TODO: ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &greeting)
    return greeting, nil
}
// worker/main.go:  client.Dial → worker.New + Register* → w.Run(worker.InterruptCh())
// starter/main.go: client.Dial → ExecuteWorkflow → run.Get → print
```

The three pieces — Workflow, Activity, Worker — are the same everywhere; only the
SDK surface differs.

</details>

## Tasks

1. Implement `composeGreeting` to return something like `"Hello, Ada from a
   Temporal Activity"`.
2. In `GreetingWorkflowImpl`, build the Activity stub with `ActivityOptions`
   (set a `startToCloseTimeout`, e.g. 10s) and call the Activity from `greet`.
3. Fill in TODOs 1–4 in `HelloWorker` to register and start the Worker.
4. Fill in TODOs 5–6 in `HelloStarter` to start the Workflow and print the
   result.
5. Run the Worker, then the starter, and confirm the greeting prints.
6. Open the Web UI and find your Workflow execution.

## Verification

The Worker and starter are separate processes — run them in two terminals:

```bash
# Terminal 3, from work/day-01/hello — the long-lived Worker (default mainClass)
mvn -q compile exec:java

# Terminal 4, from work/day-01/hello — the starter
mvn -q compile exec:java -Dexec.mainClass=training.temporal.hello.HelloStarter
```

Expected: the starter prints the greeting string to stdout. (Order doesn't
matter — start the Workflow first and the server holds it on the queue until the
Worker polls.) Then:

```bash
temporal workflow list                 # your workflow appears, Status Completed
```

In the Web UI, click the execution. You should see `WorkflowExecutionStarted`,
an `ActivityTaskScheduled`/`Started`/`Completed` trio, and
`WorkflowExecutionCompleted`. (Lab 1.3 dissects this.)

## Definition of done

- [ ] The Worker starts and polls the `hello-temporal` Task Queue (and stays up).
- [ ] The starter is a separate process that starts the Workflow and prints the
      greeting.
- [ ] The Workflow shows **Completed** in `temporal workflow list` and the UI.
- [ ] The greeting was produced by the Activity, called via the stub (not a
      direct method call on the impl).

## Hints

<details><summary>Hint 1 — what does newActivityStub need?</summary>

`Workflow.newActivityStub(GreetingActivities.class, options)` where `options`
comes from `ActivityOptions.newBuilder().setStartToCloseTimeout(...).build()`.
Without a timeout the SDK refuses to create the stub.
</details>

<details><summary>Hint 2 — starting the workflow</summary>

For a synchronous call, just invoke `workflow.greet("Ada")` on the typed stub —
it blocks and returns the result. To start without blocking, use
`WorkflowClient.start(workflow::greet, "Ada")` then fetch the result via
`WorkflowStub.fromTyped(workflow).getResult(String.class)`.
</details>

<details><summary>Hint 3 — "Workflow type already registered" or nothing happens</summary>

Make sure you `registerWorkflowImplementationTypes(GreetingWorkflowImpl.class)`
(the impl, not the interface) and `registerActivitiesImplementations(new
GreetingActivitiesImpl())` (an instance, not the class). The Worker must be
created and the factory started **before** you start the Workflow.
</details>

## Stretch goals

- Move the literal name into a second Activity that "looks up" a display name,
  so the Workflow now orchestrates **two** Activities in sequence.
- Try calling `GreetingActivitiesImpl` directly from the Workflow (bypassing the
  stub). Run it, then explain why this breaks Temporal's durability guarantees
  even though it "works."
- Throw a `RuntimeException` from the Activity on the first attempt only. Watch
  Temporal retry it automatically — you didn't write any retry code.
