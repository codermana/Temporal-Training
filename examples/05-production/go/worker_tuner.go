package production

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Worker tuning with a Tuner. NOTE on the difference vs. Java/Python: the Go SDK
// (v1.32) exposes fixed and composite tuners, but NOT the resource-based slot
// supplier that Java's ResourceBasedTuner / Python's WorkerTuner.create_resource_based
// provide. For CPU/memory-driven autosizing in Go you currently size slots
// explicitly (or use a CompositeTuner of fixed suppliers). Here we build a fixed
// tuner — the closest portable equivalent.
func StartTunedWorker(c client.Client) (worker.Worker, error) {
	tuner, err := worker.NewFixedSizeTuner(worker.FixedSizeTunerOptions{
		NumWorkflowSlots:      20,
		NumActivitySlots:      200,
		NumLocalActivitySlots: 20,
	})
	if err != nil {
		return nil, err
	}

	w := worker.New(c, "payments", worker.Options{Tuner: tuner})
	return w, nil
}
