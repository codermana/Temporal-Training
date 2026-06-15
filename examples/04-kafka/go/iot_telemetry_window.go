package kafka

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// One long-lived Workflow per IoT device. Each Kafka telemetry record arrives as
// a Signal on the "onReading" channel; the Workflow buffers readings and, every
// fixed window, publishes a rollup to a `device-rollups` topic. workflow.Sleep is
// the replay-safe timer that closes the window — never time.Sleep.
func TelemetryWindowWorkflow(ctx workflow.Context, deviceID string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
	})

	readingCh := workflow.GetSignalChannel(ctx, "onReading")
	var readings []float64

	for windows := 0; ; windows++ {
		_ = workflow.Sleep(ctx, time.Minute)

		// Drain everything signalled during the window (non-blocking).
		var value float64
		for readingCh.ReceiveAsync(&value) {
			readings = append(readings, value)
		}

		if len(readings) > 0 {
			var sum float64
			for _, v := range readings {
				sum += v
			}
			avg := sum / float64(len(readings))
			_ = workflow.ExecuteActivity(
				ctx, "PublishRollup", "device-rollups", deviceID, avg, len(readings)).Get(ctx, nil)
			readings = readings[:0]
		}

		if windows+1 >= 60 {
			return workflow.NewContinueAsNewError(ctx, TelemetryWindowWorkflow, deviceID)
		}
	}
}
