# Lab 5.3: Capstone: design & build a transactional saga

**Time:** ~90 min · **Difficulty:** ★★★ · **Stack:** Temporal (+ optional Kafka)

## Scenario

Your team is handed a multi-step, Kafka-triggered Airflow DAG that represents a
business transaction. Redesign and implement it as a Temporal Saga in Spring Boot,
end to end. This is open-ended: there's no single right answer, and that's the
point. You'll make and defend design decisions.

## The brief

You receive (or pick) a DAG like one of these:

- **Loan origination:** `validate_application → run_credit_check →
  reserve_funds → disburse → notify`. A downstream failure must release the
  reserved funds and reverse any partial disbursement.
- **Travel booking:** `book_flight → book_hotel → charge_card → issue_itinerary`.
  Any failure cancels the bookings already made and voids the charge.
- **Subscription change:** `validate_plan → prorate_billing → switch_plan →
  update_entitlements → notify`. Failures restore the previous plan and reverse
  proration.

Pick one (or bring a real DAG from your own systems, encouraged).

<details><summary><b>Doing this capstone in Python or Go?</b> Starter scaffolds</summary>

The capstone is language-agnostic: pick the SDK you'll ship in. Two **new
real-world saga** references are provided in all three languages to copy the shape
from:

- **Travel booking** (flight → hotel → car, cancel in reverse):
  [`examples/06-saga-spring/python/travel_booking_saga.py`](../../examples/06-saga-spring/python/travel_booking_saga.py)
  · [`.../go/travel_booking_saga.go`](../../examples/06-saga-spring/go/travel_booking_saga.go)
  · [`.../java/saga_compensation.java`](../../examples/06-saga-spring/java/saga_compensation.java)
- **Money-transfer ledger** (debit → credit, refund on failure):
  [`.../python/money_transfer_saga.py`](../../examples/06-saga-spring/python/money_transfer_saga.py)
  · [`.../go/money_transfer_saga.go`](../../examples/06-saga-spring/go/money_transfer_saga.go)

Wiring/lifecycle and triggers follow Lab 5.2's polyglot scaffold (FastAPI/Flask
lifespan for Python, plain service `main` for Go; Spring Boot autoconfig is
Java-only). The compensation pattern is identical to Lab 5.1: a manual stack
(Python `list`) or slice of closures (Go) unwound in reverse.

The `continueAsNew` stretch maps directly:
- **Python:** `workflow.continue_as_new(args=[...])`
  (see [`examples/runnable/12-continue-as-new/python`](../../examples/runnable/12-continue-as-new/python)).
- **Go:** `return workflow.NewContinueAsNewError(ctx, Workflow, ...)`
  (see [`.../12-continue-as-new/go`](../../examples/runnable/12-continue-as-new/go)).

For the required **test**, use each SDK's in-process test env:
`WorkflowEnvironment` (Python, `temporalio.testing`) or
`testsuite.TestWorkflowEnvironment` (Go) to assert a failure at a chosen step runs
the correct compensations.

</details>

## Requirements

Your implementation **must** include:

1. **A compensating Activity for every forward step**, unwinding in reverse on
   failure.
2. **A Kafka trigger** via a Spring `@KafkaListener` (or the Day 3 bridge) that
   starts the saga from an event. (If your machine is tight on resources, a REST
   trigger is an acceptable substitute; note the trade-off.)
3. **At least one synchronous** interaction (caller blocks for the outcome) **and
   one asynchronous** interaction (fire-and-forget + Query/Signal for status).
4. **Bounded retries** so permanent failures reach compensation.
5. **Tests** (Day 4): at least one `TestWorkflowEnvironment` test asserting that
   a failure at a chosen step runs the correct compensations.

Stretch (pick any): observability (Day 4 metrics), `continueAsNew` for a
long-running variant, replay test against a captured history, versioning a
mid-flight change.

## Suggested approach

1. **Model it first (15 min, no code).** List the forward steps, the
   compensation for each, the failure modes, and which interactions are sync vs
   async. Sketch the saga diagram.
2. **Define contracts.** `@WorkflowInterface` + `@ActivityInterface` with forward
   and compensating methods. Decide return types (you need IDs to compensate).
3. **Implement the Workflow** with the compensation stack (Lab 5.1 pattern).
4. **Wire Spring Boot** (Lab 5.2): beans, Worker, trigger, shutdown.
5. **Inject failures** at each step and verify compensation.
6. **Write the test(s).**

No starter files are provided; assembling the pieces from Labs 5.1 and 5.2 is
the exercise. Reuse those modules as templates.

## Definition of done

- [ ] Every forward step has a compensation; failures unwind in reverse.
- [ ] A Kafka (or REST) trigger starts the saga from an event.
- [ ] Both a sync and an async interaction pattern are demonstrated.
- [ ] Retries are bounded; a permanent failure compensates rather than hanging.
- [ ] At least one in-process test proves compensation for an injected failure.
- [ ] You can walk through the design choices and their trade-offs.

## Review checklist (for the capstone review session)

Be ready to discuss:

- **What got simpler** moving off the DAG? (Usually: state passing, retries,
  visibility.)
- **What required more thought?** (Usually: compensation design, idempotency,
  saga boundaries.)
- **Consistency:** what does Temporal guarantee vs. what you still own (external
  system idempotency, exactly-once side effects)?
- **Observability & auditability:** can you prove, from history alone, that every
  compensation ran?
- **Saga boundaries:** is this one saga, or should it be a parent + child
  Workflows?

## Pitfalls to avoid

- Putting business logic in the Kafka listener instead of Activities.
- Compensations that aren't idempotent (they get retried too).
- Forgetting to bound retries (the saga hangs instead of compensating).
- Injecting Spring services into Workflow impls (breaks determinism) instead of
  into Activities.

## If you finish early

- Add a human-in-the-loop approval step (Signal/Update from Day 2) before an
  irreversible action.
- Make a compensation fail and design the escalation path.
- Add a replay test (Day 4.3) and a metrics dashboard (Day 4.1) so your capstone
  is genuinely production-shaped.
