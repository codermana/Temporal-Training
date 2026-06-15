package production

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// CompositeTuner mixes slot strategies on one Worker, the Go analogue of Java's
// CompositeTuner. (See worker_tuner.go for why these are fixed-size suppliers in
// Go rather than resource-based.) You'd reach for this to give different slot
// pools to workflow tasks vs. activities on a single worker.
func StartCompositeTunedWorker(c client.Client) (worker.Worker, error) {
	workflowSlots, err := worker.NewFixedSizeSlotSupplier(20) // workflow task slots
	if err != nil {
		return nil, err
	}
	activitySlots, err := worker.NewFixedSizeSlotSupplier(200) // activity slots (the heavy work)
	if err != nil {
		return nil, err
	}
	localActivitySlots, err := worker.NewFixedSizeSlotSupplier(20) // local activity slots
	if err != nil {
		return nil, err
	}
	nexusSlots, err := worker.NewFixedSizeSlotSupplier(20)
	if err != nil {
		return nil, err
	}

	tuner, err := worker.NewCompositeTuner(worker.CompositeTunerOptions{
		WorkflowSlotSupplier:      workflowSlots,
		ActivitySlotSupplier:      activitySlots,
		LocalActivitySlotSupplier: localActivitySlots,
		NexusSlotSupplier:         nexusSlots,
	})
	if err != nil {
		return nil, err
	}

	w := worker.New(c, "mixed", worker.Options{Tuner: tuner})
	return w, nil
}
