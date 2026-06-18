package aiml

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// A conversational agent as a Workflow: it owns the message history as durable
// state, reads a Signal channel for each human turn, drives the LLM (and tools)
// as Activities, and serves the live conversation via a query handler. The LLM
// call is non-deterministic, so CallLLM is an Activity, never Workflow code.
func AgentWorkflow(ctx workflow.Context, systemPrompt string) ([]Message, error) {
	messages := []Message{{Role: "user", Content: systemPrompt}}
	pendingUserTurn := false
	ended := false

	// A UI polls this query to render the conversation without touching a DB.
	if err := workflow.SetQueryHandler(ctx, "conversation", func() ([]Message, error) {
		return messages, nil
	}); err != nil {
		return nil, err
	}

	// Drain Signals in background goroutines and flip Workflow state; the main
	// loop reacts via workflow.Await (the Go equivalent of Workflow.await).
	userTurns := workflow.GetSignalChannel(ctx, "userSays")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var msg string
			userTurns.Receive(ctx, &msg)
			messages = append(messages, Message{Role: "user", Content: msg})
			pendingUserTurn = true
		}
	})
	endCh := workflow.GetSignalChannel(ctx, "endSession")
	workflow.Go(ctx, func(ctx workflow.Context) {
		endCh.Receive(ctx, nil)
		ended = true
	})

	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	})

	for !ended {
		// Each LLM call is recorded in history and never re-run on replay.
		var assistant Message
		if err := workflow.ExecuteActivity(ao, CallLLM, messages).Get(ctx, &assistant); err != nil {
			return nil, err
		}
		messages = append(messages, assistant)

		// No tool to run (a real agent branches on a tool_use block): wait for the
		// next human turn delivered by Signal, or for the session to end.
		pendingUserTurn = false
		_ = workflow.Await(ctx, func() bool { return pendingUserTurn || ended })

		// Keep history bounded on long chats — continue-as-new (Day-5 pattern).
		if len(messages) > 200 && !ended {
			return nil, workflow.NewContinueAsNewError(ctx, AgentWorkflow, systemPrompt)
		}
	}
	return messages, nil
}
