package foundations

import (
	"math/rand"
	"os"
	"strconv"
	"time"

	"go.temporal.io/sdk/workflow"
)

func BadReplayWorkflow(ctx workflow.Context, batchDate string) error {
	// Bad: replay re-runs Workflow code. This value changes on replay.
	now := time.Now()

	// Bad: this random value is not recorded in Workflow history.
	shard := rand.Intn(10)

	// Bad: direct I/O in Workflow code can run again during replay.
	return os.WriteFile("/tmp/workflow.log",
		[]byte(strconv.FormatInt(now.UnixMilli(), 10)+":"+strconv.Itoa(shard)), 0o644)
}
