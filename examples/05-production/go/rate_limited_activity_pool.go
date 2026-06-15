package production

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// NEW production snippet: rate-limit a pool of Workers calling a fragile
// downstream (a legacy API, a vendor with a QPS cap). Two complementary knobs on
// worker.Options:
//
//	WorkerActivitiesPerSecond    -> per-Worker cap
//	TaskQueueActivitiesPerSecond -> GLOBAL cap across every Worker polling this
//	                                task queue (the server enforces it)
//
// Use the task-queue-wide limit to protect the dependency no matter how many
// Worker replicas you scale to. Analogue of Python's
// max_task_queue_activities_per_second / Java's setMaxTaskQueueActivitiesPerSecond.
func StartRateLimitedWorker(c client.Client) worker.Worker {
	return worker.New(c, "legacy-api-calls", worker.Options{
		// Cap THIS worker to 50 activity starts/sec...
		WorkerActivitiesPerSecond: 50,
		// ...and the whole task queue (all replicas combined) to 100/sec, so the
		// vendor never sees more than 100 QPS even at 20 worker replicas.
		TaskQueueActivitiesPerSecond: 100,
	})
}
