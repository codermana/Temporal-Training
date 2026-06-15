# Hello Temporal, anywhere — runnable lab (Java · Python · Go)

The same Hello workflow, but the **connection is env-driven** so one binary
targets a local dev server (Lab 1.2b Docker) or Temporal Cloud (Lab 1.2c) with no
code change. The connection mode is chosen by which variables are set:

| Variables set | Mode |
| --- | --- |
| _none_ | Plaintext to `TEMPORAL_ADDRESS` (default `127.0.0.1:7233`) — local / Docker |
| `TEMPORAL_API_KEY` | Temporal Cloud over TLS via API key |
| `TEMPORAL_TLS_CERT` + `TEMPORAL_TLS_KEY` | Temporal Cloud over mTLS |

Shared: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` (default `default`).

## Run

```bash
# Java
scripts/run-example.sh connect                       # == cd java && mvn -q compile exec:java

# Python
cd python && pip install -r requirements.txt && python worker.py
#   scripts/run-example.sh connect python

# Go
cd go && go run .
#   scripts/run-example.sh connect go
```

Cloud example:

```bash
TEMPORAL_ADDRESS=us-east-1.aws.api.temporal.io:7233 \
TEMPORAL_NAMESPACE=your-ns.acct \
TEMPORAL_API_KEY=$(cat key.txt) \
  python worker.py
```

The connection helpers (`Connections.java`, `connections.py`, `connections.go`)
are the only thing that differs from `01-hello-temporal`.
