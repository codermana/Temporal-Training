// Command starter starts one waiting approval-demo Workflow (standalone client).
// Run the Worker first with `go run ./worker` in another terminal.
//
//	go run ./starter        // needs a Temporal dev server on 127.0.0.1:7233
//
// The Workflow blocks in workflow.Await until an approve/reject Signal arrives,
// so you can drive it from the CLI in another shell:
//
//	temporal workflow query  --workflow-id approval-demo --type currentState
//	temporal workflow update execute --workflow-id approval-demo --name changeNote \
//	    --input '"expedite before close of business"'
//	temporal workflow signal --workflow-id approval-demo --name approve \
//	    --input '"manager@example.com"'
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"training.temporal/approval"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	_, err = c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: approval.WorkflowID, TaskQueue: approval.TaskQueue},
		approval.ApprovalWorkflow,
		"PO-1001",
	)
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		log.Printf("Workflow '%s' is already running; reusing it.", approval.WorkflowID)
	} else if err != nil {
		log.Fatalln("unable to start workflow:", err)
	} else {
		log.Printf("Started Workflow '%s', waiting for a decision.", approval.WorkflowID)
	}

	log.Println("Try, in another terminal:")
	log.Println("  temporal workflow query  --workflow-id approval-demo --type currentState")
	log.Println("  temporal workflow update execute --workflow-id approval-demo --name changeNote" +
		" --input '\"expedite before close of business\"'")
	log.Println("  temporal workflow signal --workflow-id approval-demo --name approve" +
		" --input '\"manager@example.com\"'")
}
