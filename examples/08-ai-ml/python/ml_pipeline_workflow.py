from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    # Each stage is an Activity — the heavy/flaky/non-deterministic work lives here.
    from ml_activities import ingest, preprocess, train, evaluate, deploy


# An ML pipeline is a plain Workflow sequencing Activities. The win over a DAG
# scheduler (the Day-1 Airflow-vs-Temporal point): per-step retry policy, a quality
# gate written in code, heartbeat-based resume on the long training step, and
# idempotency via a deterministic run_id passed into every Activity.
@workflow.defn
class MLPipelineWorkflow:
    def __init__(self) -> None:
        self._approved = False

    @workflow.run
    async def run(self, dataset_uri: str, accuracy_threshold: float) -> str:
        run_id = workflow.info().workflow_id  # deterministic; makes Activities idempotent

        # Flaky source: retry generously.
        raw = await workflow.execute_activity(
            ingest,
            dataset_uri,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(maximum_attempts=10),
        )
        features = await workflow.execute_activity(
            preprocess, raw, start_to_close_timeout=timedelta(minutes=30)
        )

        # Long GPU job: heartbeats let it resume from the last checkpoint on retry
        # instead of restarting from epoch 0 (Day-2 heartbeat_resume pattern). Don't
        # auto-retry blindly — cap attempts so a broken run doesn't loop on GPUs.
        model_uri = await workflow.execute_activity(
            train,
            args=[features, run_id],
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        metrics = await workflow.execute_activity(
            evaluate, model_uri, start_to_close_timeout=timedelta(minutes=20)
        )

        # Quality gate, in code. Auto-deploy if clearly good; otherwise wait for a
        # human approval Signal before promoting a borderline model.
        if metrics["accuracy"] < accuracy_threshold:
            await workflow.wait_condition(lambda: self._approved)

        return await workflow.execute_activity(
            deploy,
            args=[model_uri, run_id],
            start_to_close_timeout=timedelta(minutes=15),
        )

    @workflow.signal
    def approve_deploy(self) -> None:
        self._approved = True
