# Day 4 — Production engineering

Today is about running Temporal safely in production: see what your Workers are
doing (observability), prove your Workflows are correct without a server (unit
testing), and catch determinism regressions before they hit prod (replay
testing).

## Required stack

| Lab | Stack |
|---|---|
| 1 — Observability | `make temporal` + `make stack-obs` (Prometheus :9091, Grafana :3000) |
| 2 — Testing | **None.** Runs fully in-process via `TestWorkflowEnvironment`. |
| 3 — Replay testing | A captured history JSON (no live server needed to replay). |

```bash
make temporal      # labs 1
make stack-obs     # lab 1 only: Prometheus + Grafana
```

Grafana: <http://127.0.0.1:3000> (admin / admin) · Prometheus:
<http://127.0.0.1:9091>

## Labs

| # | Lab | Time | Difficulty |
|---|-----|------|-----------|
| 1 | [Observability — metrics & dashboards](lab-1-observability.md) | 50 min | ★★ |
| 2 | [Testing Workflows](lab-2-testing-workflows.md) | 45 min | ★★ |
| 3 | [Replay testing](lab-3-replay-testing.md) | 40 min | ★★★ |

## Background you should already have from the lecture

- **Versioning:** `Workflow.getVersion()` patching vs.
  `@WorkflowVersioningBehavior` (`Pinned` / `AutoUpgrade`). Replay testing
  (lab 3) is how you *verify* a version change is safe.
- **Worker tuning:** `WorkerOptions` slot sizes, `ResourceBasedTuner`,
  `CompositeTuner`, virtual threads on JVM 21+. The observability lab is where
  you'd watch the effect of these.
