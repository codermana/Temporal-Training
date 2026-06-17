# Lab 5.2 — Saga in Spring Boot

**Time:** ~70 min · **Difficulty:** ★★★ · **Stack:** Temporal dev server

## Scenario

The hand-rolled `SagaWorker` from Lab 5.1 works, but real services live in Spring
Boot. You'll move the same saga into a Spring application: Workflow/Activity
beans, a `WorkflowClient` bean, Worker registration on startup, a REST (and/or
Kafka) trigger, and graceful shutdown. Then you'll inject a failure at each step
and verify compensation still fires.

## Learning goals

- Wire Temporal into Spring Boot using `temporal-spring-boot-starter`.
- Register Activities as `@Component` beans and the Worker on context start.
- Trigger sagas via a REST endpoint and/or a `@KafkaListener`.
- Drive sync (block for result) and async (fire-and-forget + query) interactions.
- Shut down the Worker cleanly on context close.

## Prerequisites

- **Lab 5.1 complete** — you'll reuse `OrderSagaWorkflow` / `OrderActivities`.
- `make temporal` running.

## Starter code

New Spring Boot module. `pom.xml` adds the starter (plus Spring Web, and Spring
Kafka if you wire the Kafka trigger):

```xml
<dependency>
  <groupId>io.temporal</groupId>
  <artifactId>temporal-spring-boot-starter</artifactId>
  <version>1.32.1</version>
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

**`application.yml`** — point the starter at the dev server and declare a Worker:

```yaml
spring:
  temporal:
    connection:
      target: 127.0.0.1:7233
    namespace: default
    workers:
      - task-queue: orders
        # the starter auto-discovers @WorkflowImpl / @ActivityImpl beans
    workers-auto-discovery:
      packages:
        - training.temporal.saga.spring
```

> The exact property names can drift between starter versions — confirm against
> the starter you pulled (`mvn dependency:tree`) and its docs. The shape above is
> the intent: one Worker on the `orders` queue, beans auto-discovered.

> **Known-good minimal reference:** [`examples/runnable/16-spring-boot`](../../examples/runnable/16-spring-boot)
> is a complete, runnable starter app (`make run-spring`) verified against
> `temporal-spring-boot-starter:1.32.1` + Spring Boot 3.3. Copy its `application.yml`
> and `@WorkflowImpl` / `@Component @ActivityImpl` annotations if the property
> names give you trouble. With auto-discovery you don't need the `workers:` list
> at all — the annotations declare the task queue.

**Structure to build (stubs):**

```java
// Reuse OrderSagaWorkflow + OrderSagaWorkflowImpl from Lab 5.1 (annotate the impl
// so the starter registers it as a workflow type on the worker).

// OrderActivitiesImpl as a Spring @Component:
@Component
public class OrderActivitiesImpl implements OrderActivities {
  // TODO: same logic as Lab 5.1, but now a managed bean (can @Autowired other
  //       services — a real PaymentClient, InventoryRepository, etc.)
}

// REST trigger:
@RestController
public class OrderController {
  private final WorkflowClient client;   // TODO: inject the starter-provided bean

  @PostMapping("/orders/{id}")
  public String submit(@PathVariable String id) {
    // TODO (sync): build an OrderSagaWorkflow stub on task queue "orders",
    //              call process(id) and return the result.
    // OR (async): WorkflowClient.start(...) and return the workflowId immediately.
    throw new UnsupportedOperationException("TODO");
  }

  @GetMapping("/orders/{id}")
  public String status(@PathVariable String id) {
    // TODO (async pattern): query the running workflow's state, or describe it.
    throw new UnsupportedOperationException("TODO");
  }
}
```

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Spring Boot autoconfig is **Java-only** — there is no direct equivalent in the
other SDKs. The idiomatic analogue is:

- **Python:** a FastAPI/Flask **lifespan** that owns the client + worker (no DI
  container; you register workflows/activities explicitly). See the module factory
  in [`examples/06-saga-spring/python/worker.py`](../../examples/06-saga-spring/python/worker.py).
- **Go:** a plain service **`main`** that dials the client, builds the worker, and
  registers everything. See [`examples/06-saga-spring/go/worker_setup.go`](../../examples/06-saga-spring/go/worker_setup.go).

Reuse the saga itself from Lab 5.1
([`examples/runnable/07-saga/python`](../../examples/runnable/07-saga/python) /
[`.../go`](../../examples/runnable/07-saga/go)); this lab only changes the
*trigger* and *lifecycle*.

**Python** — start the worker in a FastAPI lifespan; trigger via a route:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from temporalio.client import Client
from temporalio.worker import Worker
from saga import OrderSagaWorkflow, authorize_payment, ship  # ...etc

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = await Client.connect("127.0.0.1:7233")
    worker = Worker(client, task_queue="orders",
                    workflows=[OrderSagaWorkflow],
                    activities=[authorize_payment, ship])  # ...register all
    async with worker:          # graceful drain on shutdown is automatic
        app.state.client = client
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/orders/{order_id}")            # SYNC: block for the outcome
async def submit(order_id: str):
    return await app.state.client.execute_workflow(
        OrderSagaWorkflow.process, order_id,
        id=f"order-{order_id}", task_queue="orders")

@app.post("/orders/{order_id}/async")      # ASYNC: start + return id, query later
async def submit_async(order_id: str):
    h = await app.state.client.start_workflow(
        OrderSagaWorkflow.process, order_id,
        id=f"order-{order_id}", task_queue="orders")
    return {"workflow_id": h.id}
```

**Go** — a plain `main` with an HTTP handler that calls the client:

```go
func main() {
    c, _ := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
    defer c.Close()

    w := worker.New(c, "orders", worker.Options{})
    w.RegisterWorkflow(OrderSagaWorkflow)
    w.RegisterActivity(AuthorizePayment) // ...register all
    _ = w.Start(); defer w.Stop()        // Stop() drains in-flight work

    http.HandleFunc("/orders/", func(rw http.ResponseWriter, req *http.Request) {
        id := strings.TrimPrefix(req.URL.Path, "/orders/")
        run, _ := c.ExecuteWorkflow(req.Context(),
            client.StartWorkflowOptions{ID: "order-" + id, TaskQueue: "orders"},
            OrderSagaWorkflow, id)
        var result string
        _ = run.Get(req.Context(), &result)   // SYNC; use Start + no Get for ASYNC
        fmt.Fprintln(rw, result)
    })
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

**Inject failures** the same way in every SDK: an `orderId` containing `"fail"`
makes `ship` throw, so compensation fires — no per-language failure flag needed.

</details>

## Tasks

1. Stand up the Spring app so the Worker registers on the `orders` queue at
   startup (check logs for "Worker started"/poller threads).
2. Implement the REST trigger for a **synchronous** saga (caller blocks for
   `COMPLETED`/`COMPENSATED`).
3. Add an **asynchronous** path: `POST` starts and returns the Workflow ID;
   `GET` queries status.
4. **Inject a failure at each step** (param or header that makes payment /
   inventory / ship throw) and verify the right compensations run each time.
5. Confirm **graceful shutdown**: stopping the app drains in-flight Activities
   (`Worker.shutdown()` on context close — the starter wires this for you;
   verify it).

## Verification

```bash
mvn -q spring-boot:run        # in the module

# Happy path (sync) -> COMPLETED
curl -s -X POST localhost:8080/orders/order-1001

# Failure at ship -> COMPENSATED
curl -s -X POST localhost:8080/orders/fail-at-ship

# Async: start + poll
curl -s -X POST 'localhost:8080/orders/order-async?async=true'
curl -s localhost:8080/orders/order-async
```

Cross-check each run in the Web UI — compensations should appear for every
injected failure.

## Definition of done

- [ ] Spring Boot starts a Worker on `orders` via the starter (no manual
      `WorkerFactory` wiring).
- [ ] A REST call runs the saga synchronously and returns the outcome.
- [ ] An async call returns immediately; a follow-up query reports status.
- [ ] Failure injected at payment, inventory, **and** ship each triggers correct
      compensation.
- [ ] Stopping the app drains rather than hard-killing in-flight work.

## Pitfalls

- **Don't `new` a `WorkflowClient`.** Inject the starter-provided bean so it
  uses the configured connection/namespace.
- **Activities as beans, Workflows as types.** Activity *instances* are beans
  (so they can use other Spring beans); Workflow *implementations* are registered
  as types and must stay free of injected mutable state used inside Workflow code
  (determinism). Inject services into **Activities**, not Workflow impls.
- Spring Kafka `@KafkaListener` is just another trigger — it should call the
  `WorkflowClient`, not contain saga logic.

## Hints

<details><summary>Hint 1 — sync vs async at the client</summary>

Sync: call the typed method directly (`workflow.process(id)`) — it blocks. Async:
`WorkflowClient.start(workflow::process, id)` returns immediately; read later via
a Query or `WorkflowStub.getResult`.
</details>

<details><summary>Hint 2 — verifying graceful shutdown</summary>

Start a slow saga (add a `Workflow.sleep`/slow Activity), then stop the app
mid-flight. With graceful shutdown the in-flight Activity completes or the
Workflow resumes on next start — it is **not** lost.
</details>

## Stretch goals

- Add a Spring Kafka `@KafkaListener` on an `orders` topic that triggers the saga
  — the bridge from Day 3, now idiomatic Spring.
- Expose Actuator health and a custom metric (ties to Day 4) so the saga app is
  production-observable.
- Add `Workflow.continueAsNew` for a subscription-style saga that runs
  indefinitely without unbounded history.
