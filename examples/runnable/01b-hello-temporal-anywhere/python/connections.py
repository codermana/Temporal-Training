"""Build a Temporal Client from environment variables so the same Worker targets
a local dev server, a Dockerized cluster, or Temporal Cloud without code changes.
Python mirror of the Java Connections.fromEnv(). Connection mode is selected by
which variables are set:

  * API key (Cloud)   — TEMPORAL_API_KEY set; TLS to the regional gRPC endpoint.
  * mTLS (Cloud)      — TEMPORAL_TLS_CERT + TEMPORAL_TLS_KEY set; namespace endpoint.
  * Plaintext (local) — no credentials; defaults to 127.0.0.1:7233 / namespace default.
"""

import os

from temporalio.client import Client, TLSConfig


async def from_env() -> Client:
    address = os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    tls_cert = os.environ.get("TEMPORAL_TLS_CERT")
    tls_key = os.environ.get("TEMPORAL_TLS_KEY")

    if api_key:
        # --- Temporal Cloud via API key (simplest) ---
        print(f"Connecting with API key to {address} (namespace {namespace})")
        return await Client.connect(
            address, namespace=namespace, api_key=api_key, tls=True
        )

    if tls_cert and tls_key:
        # --- Temporal Cloud via mTLS client certificate ---
        print(f"Connecting with mTLS to {address} (namespace {namespace})")
        with open(tls_cert, "rb") as c, open(tls_key, "rb") as k:
            tls = TLSConfig(client_cert=c.read(), client_private_key=k.read())
        return await Client.connect(address, namespace=namespace, tls=tls)

    # --- Local dev server (no credentials) ---
    print(f"Connecting to local server at {address} (namespace {namespace})")
    return await Client.connect(address, namespace=namespace)
