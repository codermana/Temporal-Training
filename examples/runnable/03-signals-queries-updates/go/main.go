// Command worker runs a long-lived Worker and starts one waiting approval-demo
// Workflow, mirroring ApprovalWorker.java.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
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
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(ApprovalWorkflow)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	_, err = c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: WorkflowID, TaskQueue: TaskQueue},
		ApprovalWorkflow,
		"PO-1001",
	)
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		log.Printf("Workflow '%s' is already running; reusing it.", WorkflowID)
	} else if err != nil {
		log.Fatalln("unable to start workflow:", err)
	} else {
		log.Printf("Started Workflow '%s', waiting for a decision.", WorkflowID)
	}

	log.Println("Try, in another terminal:")
	log.Println("  temporal workflow query  --workflow-id approval-demo --type currentState")
	log.Println("  temporal workflow update execute --workflow-id approval-demo --name changeNote" +
		" --input '\"expedite before close of business\"'")
	log.Println("  temporal workflow signal --workflow-id approval-demo --name approve" +
		" --input '\"manager@example.com\"'")
	log.Printf("Worker polling '%s'. Ctrl+C to stop.", TaskQueue)

	// Block forever so the Worker keeps polling and the Workflow stays queryable.
	select {}
}
