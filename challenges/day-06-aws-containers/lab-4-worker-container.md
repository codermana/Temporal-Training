# Lab 6.4 — Containerize the Worker

**Time:** ~45 min · **Difficulty:** ★★ · **Stack:** Docker + Temporal

## Scenario

A Temporal Worker is a stateless, long-running process that polls a Task Queue
and makes only **outbound** connections — no inbound ports, no Ingress. That
makes it an ideal container workload. You'll write a multi-stage Dockerfile,
tune the JVM for containers, wire graceful shutdown, and add a health signal so
Kubernetes (Lab 6.5) can probe it.

## Learning goals

- Build a small Worker image with a multi-stage Dockerfile.
- Set container-aware JVM flags (`UseContainerSupport`, `MaxRAMPercentage`).
- Handle `SIGTERM` → `WorkerFactory.shutdown()` to drain in-flight work.
- Provide a health signal (process check or an HTTP `/health`).

## Prerequisites

- Docker running. A Worker `main` that reads config from env vars (the Import
  Worker from Labs 6.1–6.3 is perfect).

## Starter code

Use a Worker entrypoint that is **env-driven** (so the same image serves any
Task Queue):

```java
// WorkerMain.java (shape — you likely already have this from Labs 6.1-6.3)
public final class WorkerMain {
  public static void main(String[] args) {
    String target    = System.getenv().getOrDefault("TEMPORAL_ADDRESS", "127.0.0.1:7233");
    String namespace = System.getenv().getOrDefault("TEMPORAL_NAMESPACE", "default");
    String taskQueue = System.getenv().getOrDefault("TASK_QUEUE", "transform");
    // build stubs(target) -> client(namespace) -> factory -> worker(taskQueue)
    // register ImportWorkflowImpl + ImportActivitiesImpl
    // TODO: Runtime.getRuntime().addShutdownHook(new Thread(factory::shutdown));
    // factory.start();
  }
}
```

**Dockerfile — complete the stubs:**

```dockerfile
# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
# TODO 1: package the app, skipping tests, producing the runnable jar

# ---- runtime stage ----
FROM eclipse-temurin:17-jre
WORKDIR /app
# TODO 2: copy the built jar from the build stage to /app/worker.jar
# TODO 3: set JAVA_TOOL_OPTIONS for container awareness:
#         -XX:+UseContainerSupport -XX:MaxRAMPercentage=75
# TODO 4: ENTRYPOINT java -jar /app/worker.jar
```

> If your jar isn't runnable on its own, add the Maven Shade plugin (or set the
> `Main-Class` manifest) so `java -jar worker.jar` works.

## Tasks

1. Make the jar runnable (`java -jar` finds `WorkerMain`).
2. Complete the multi-stage Dockerfile.
3. Add the graceful-shutdown hook to `WorkerMain`.
4. Build the image and run it against your local Temporal dev server.
5. Send `SIGTERM` (`docker stop`) and confirm the Worker logs a clean shutdown
   (factory drains rather than aborting mid-Activity).

## Verification

```bash
docker build -t temporal-transform-worker:latest .

# Run it against the host's Temporal dev server.
# Linux: --add-host or --network host; macOS: host.docker.internal.
docker run --rm \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  -e TASK_QUEUE=transform \
  temporal-transform-worker:latest
```

Expected: logs show `Worker started ... taskQueue=transform`. Start a Workflow
on `transform` (`make start-workflow QUEUE=transform ID=1`) and watch the
containerized Worker pick it up. Then `docker stop <id>` and confirm the shutdown
hook ran.

## Definition of done

- [ ] Multi-stage build produces a small runtime image (JRE, not full JDK+Maven).
- [ ] JVM uses container memory limits (`MaxRAMPercentage`), not a fixed `-Xmx`.
- [ ] `docker stop` triggers `factory.shutdown()` (graceful drain), visible in logs.
- [ ] The containerized Worker executes a Workflow started on its Task Queue.

## Pitfalls

- **No inbound port needed.** Don't `EXPOSE` a service port expecting traffic —
  the Worker dials *out* to the Frontend. (A `/health` port is optional and only
  for probes.)
- **`host.docker.internal`** reaches the host on macOS/Windows; on Linux use
  `--network host` or `--add-host=host.docker.internal:host-gateway`.
- **Don't hardcode `-Xmx`.** Let `UseContainerSupport` + `MaxRAMPercentage`
  derive the heap from the container's memory limit, so K8s `resources.limits`
  actually control it.

## Hints

<details><summary>Hint 1 — runnable jar</summary>

Add the Shade plugin so `package` produces a fat jar with the right `Main-Class`,
then `COPY --from=build /src/target/<artifact>.jar /app/worker.jar`.
</details>

<details><summary>Hint 2 — health endpoint (for Lab 6.5)</summary>

A plain `com.sun.net.httpserver.HttpServer` returning `200` on `/health` only
**after** `factory.start()` succeeds is enough for a readiness probe. Spring
Actuator's `/actuator/health` is the richer option if you're in Spring Boot.
</details>

## Stretch goals

- Add an HTTP `/health` that flips to `200` only post-`start()`, so Lab 6.5 can
  use an `httpGet` readiness probe instead of a process check.
- Shrink the image: try a distroless or `-alpine` JRE base and compare size.
