from datetime import timedelta

from temporalio import workflow

# The same shape as the Airflow DAG in ../airflow_dag_before.py, expressed as
# durable application code: the dependency graph is just sequential awaits, and
# the data passed between steps is ordinary return values — no XComs.
# `extract`, `transform`, `load` are module-level @activity.defn functions.


@workflow.defn
class OrdersWorkflow:
    @workflow.run
    async def run(self, batch_date: str) -> None:
        opts = dict(start_to_close_timeout=timedelta(minutes=10))
        raw_uri = await workflow.execute_activity(extract, batch_date, **opts)
        clean_uri = await workflow.execute_activity(transform, raw_uri, **opts)
        await workflow.execute_activity(load, clean_uri, **opts)
