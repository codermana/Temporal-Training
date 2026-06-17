// A containerized Worker needs config (Temporal address/namespace, task queue,
// downstream endpoints) and secrets (an API key) without baking them into the
// image or shipping them as plaintext env vars. In AWS this is "env vars /
// mounted secrets file"; here it's SSM Parameter Store read at WORKER STARTUP.
// GetParametersByPath loads a whole "/temporal-training/worker/" tree at boot,
// WithDecryption decrypts SecureString params via KMS, and the values build the
// Temporal Client + Worker.
//
// The boundary that matters for Temporal: this read happens in plain process
// (bootstrap) code, NOT in a Workflow — an SSM call is non-deterministic I/O and
// the value can change between replays. When a Workflow STEP needs a secret, it
// reads it inside the FetchAPIKey Activity, never in Workflow code. In ECS/EKS
// the SSM client authenticates via the task role / IRSA, so there are no static
// keys; the LocalStack endpoint stands in for that locally.
//
// The Go port of ssm_parameter_config.java. The SSM calls sit behind a small
// SsmAPI interface so the Temporal usage compiles without the AWS SDK; wire the
// real aws-sdk-go-v2 ssm.Client (endpoint http://127.0.0.1:4566, us-east-1,
// test/test creds) to that interface for LocalStack.
package aws

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// SsmAPI is the slice of the AWS SSM client this bootstrap needs. withDecryption
// decrypts SecureString params via KMS.
type SsmAPI interface {
	// GetByPath returns name->value for every param under path, following
	// pagination internally. withDecryption decrypts SecureString values.
	GetByPath(ctx context.Context, path string, withDecryption bool) (map[string]string, error)
	// GetOne returns a single (optionally decrypted) parameter value.
	GetOne(ctx context.Context, name string, withDecryption bool) (string, error)
}

// WorkerConfig is the immutable snapshot of the config tree, cached at boot — we
// read SSM once at startup, not per task, so we don't hammer Parameter Store.
type WorkerConfig struct {
	TemporalAddress string
	Namespace       string
	TaskQueue       string
	OrderAPIURL     string
}

// LoadConfig reads the whole config tree at startup with decryption on.
func LoadConfig(ctx context.Context, ssm SsmAPI, path string) (WorkerConfig, error) {
	params, err := ssm.GetByPath(ctx, path, true) // "/temporal-training/worker/"
	if err != nil {
		return WorkerConfig{}, err
	}
	ns := params["namespace"]
	if ns == "" {
		ns = "default"
	}
	return WorkerConfig{
		TemporalAddress: params["temporal-address"],
		Namespace:       ns,
		TaskQueue:       params["task-queue"],
		OrderAPIURL:     params["order-api-url"],
	}, nil
}

// SecretActivities wraps an SsmAPI so the Activity stays testable.
type SecretActivities struct{ Ssm SsmAPI }

// FetchAPIKey is what a Workflow step calls when it needs the key — NOT SSM
// directly. The read is non-deterministic I/O and belongs behind the Activity
// boundary. The plaintext key stays inside the Activity; never log it and avoid
// returning it into Workflow history (pass it straight to the downstream call).
func (a *SecretActivities) FetchAPIKey(ctx context.Context) (string, error) {
	return a.Ssm.GetOne(ctx, "/temporal-training/worker/api-key", true)
}

// Bootstrap is process startup code — reading SSM here is fine. Cache the config
// once at boot, then build the Temporal Client + Worker from it.
func Bootstrap(ctx context.Context, ssm SsmAPI) (worker.Worker, error) {
	cfg, err := LoadConfig(ctx, ssm, "/temporal-training/worker/")
	if err != nil {
		return nil, err
	}
	c, err := client.Dial(client.Options{HostPort: cfg.TemporalAddress, Namespace: cfg.Namespace})
	if err != nil {
		return nil, err
	}
	w := worker.New(c, cfg.TaskQueue, worker.Options{})
	w.RegisterActivity((&SecretActivities{Ssm: ssm}).FetchAPIKey)
	// w.RegisterWorkflow(...)
	return w, nil
}
