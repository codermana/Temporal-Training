"""Start one waiting approval-demo Workflow (standalone client).

Run the Worker first (worker.py) in another terminal, then:

    python starter.py        # needs a Temporal dev server on 127.0.0.1:7233

The Workflow blocks in wait_condition until an approve/reject Signal arrives, so
you can drive it from the CLI in another shell:

  temporal workflow query  --workflow-id approval-demo --type current_state
  temporal workflow update execute --workflow-id approval-demo --name change_note \\
      --input '"expedite before close of business"'
  temporal workflow signal --workflow-id approval-demo --name approve \\
      --input '"manager@example.com"'
"""

import asyncio

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from approval import TASK_QUEUE, WORKFLOW_ID, ApprovalWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    try:
        await client.start_workflow(
            ApprovalWorkflow.run,
            "PO-1001",
            id=WORKFLOW_ID,
            task_queue=TASK_QUEUE,
        )
        print(f"Started Workflow '{WORKFLOW_ID}', waiting for a decision.")
    except WorkflowAlreadyStartedError:
        print(f"Workflow '{WORKFLOW_ID}' is already running; reusing it.")

    print("Try, in another terminal:")
    print("  temporal workflow query  --workflow-id approval-demo --type current_state")
    print(
        "  temporal workflow update execute --workflow-id approval-demo --name change_note"
        " --input '\"expedite before close of business\"'"
    )
    print(
        "  temporal workflow signal --workflow-id approval-demo --name approve"
        " --input '\"manager@example.com\"'"
    )


if __name__ == "__main__":
    asyncio.run(main())
