package main

import (
	"crypto/tls"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
)

// dialFromEnv builds a Temporal Client from environment variables so the same
// Worker targets a local dev server, a Dockerized cluster, or Temporal Cloud
// without code changes. Go mirror of the Java Connections.fromEnv().
//
//	API key (Cloud)   — TEMPORAL_API_KEY set; TLS to the regional gRPC endpoint.
//	mTLS (Cloud)      — TEMPORAL_TLS_CERT + TEMPORAL_TLS_KEY set; namespace endpoint.
//	Plaintext (local) — no credentials; defaults to 127.0.0.1:7233 / namespace default.
func dialFromEnv() (client.Client, error) {
	address := getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := getenv("TEMPORAL_NAMESPACE", "default")
	apiKey := os.Getenv("TEMPORAL_API_KEY")
	tlsCert := os.Getenv("TEMPORAL_TLS_CERT")
	tlsKey := os.Getenv("TEMPORAL_TLS_KEY")

	opts := client.Options{HostPort: address, Namespace: namespace}

	switch {
	case apiKey != "":
		// --- Temporal Cloud via API key (simplest) ---
		fmt.Printf("Connecting with API key to %s (namespace %s)\n", address, namespace)
		opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		opts.ConnectionOptions = client.ConnectionOptions{TLS: &tls.Config{}}

	case tlsCert != "" && tlsKey != "":
		// --- Temporal Cloud via mTLS client certificate ---
		fmt.Printf("Connecting with mTLS to %s (namespace %s)\n", address, namespace)
		cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
		if err != nil {
			return nil, fmt.Errorf("load mTLS cert/key: %w", err)
		}
		opts.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{Certificates: []tls.Certificate{cert}},
		}

	default:
		// --- Local dev server (no credentials) ---
		fmt.Printf("Connecting to local server at %s (namespace %s)\n", address, namespace)
	}

	return client.Dial(opts)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
