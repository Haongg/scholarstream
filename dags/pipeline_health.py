"""Daily health check of the whole pipeline (see monitoring/health.py for the checks).

Runs at 06:00 UTC, well after the 00:15 ingest chain has finished. It looks at results
(gold, the run log, the silver markers), not at task states, so it also catches a pipeline
that stopped without failing. Any problem fails the task, and its failure callback logs an
ALERT like any other failed task. No retries: a stale gold day does not heal in 10 minutes.

Trigger manually with param as_of (YYYY-MM-DD) to judge as if it were 06:00 UTC that day,
e.g. to check that a gap of a few days would be caught.
"""

from datetime import UTC, date, datetime

from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from monitoring.callbacks import TASK_CALLBACKS


@dag(
    dag_id="pipeline_health",
    schedule=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={**TASK_CALLBACKS, "retries": 0},
    params={
        "as_of": Param(None, type=["null", "string"], format="date", description="judge as of 06:00 UTC that day"),
    },
    tags=["scholarstream", "monitoring"],
    doc_md=__doc__,
)
def pipeline_health():
    @task
    def check() -> None:
        from monitoring.health import as_of_moment, health, summary

        as_of = get_current_context()["params"].get("as_of")
        results = health(as_of_moment(date.fromisoformat(as_of) if as_of else None))
        if any(results.values()):
            raise AirflowFailException(f"Pipeline unhealthy: {summary(results)}")

    check()


pipeline_health()
