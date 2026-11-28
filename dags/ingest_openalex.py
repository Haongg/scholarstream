"""Daily OpenAlex -> bronze load. Emits BRONZE_WORKS, which triggers bronze_to_silver.

Runs at 00:15 UTC, just after OpenAlex resets the daily budget (00:00 UTC). It loads up to
the day before the run's logical date, so a backfill run for an old date loads that date's
window instead of "yesterday by the wall clock". The lookback window (INGEST_LOOKBACK_DAYS)
re-fetches recent days to catch late-indexed works.

Trigger manually with params start/end (YYYY-MM-DD) for the first load or a backfill.
"""

import logging
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from scholarstream_assets import BRONZE_WORKS

from monitoring.callbacks import TASK_CALLBACKS

log = logging.getLogger(__name__)


@dag(
    dag_id="ingest_openalex",
    schedule=CronTriggerTimetable("15 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # two runs would race on the same partitions and watermark
    default_args={**TASK_CALLBACKS, "retries": 1, "retry_delay": timedelta(minutes=10)},  # a count mismatch may clear up
    params={
        "start": Param(None, type=["null", "string"], format="date", description="first day; default: lookback"),
        "end": Param(None, type=["null", "string"], format="date", description="last day; default: day before run"),
    },
    tags=["scholarstream", "bronze"],
    doc_md=__doc__,
)
def ingest_openalex():
    @task(outlets=[BRONZE_WORKS])
    def ingest() -> list[str]:
        from ingestion.run import (
            ingest as load,  # imported here to keep DAG parsing light
        )

        context = get_current_context()
        params = context["params"]
        run_date = (context.get("logical_date") or context["dag_run"].run_after).date()
        start = date.fromisoformat(params["start"]) if params.get("start") else None
        end = date.fromisoformat(params["end"]) if params.get("end") else run_date - timedelta(days=1)

        from monitoring.metrics import record_ingest

        # NoWatermark / CountMismatch raise and fail the task, so no event is emitted.
        result = load(start, end)
        record_ingest(context, result)  # best-effort: never fails the task
        if result.budget_exhausted:
            # Not a failure: retrying before 00:00 UTC is pointless, and the next scheduled
            # run starts right after the reset and continues from the watermark.
            log.warning("API budget spent after %d day(s); the rest waits for the next run", len(result.loaded))
        days = [d.isoformat() for d in result.loaded]
        context["outlet_events"][BRONZE_WORKS].extra = {"days": days, "budget_exhausted": result.budget_exhausted}
        return days

    ingest()


ingest_openalex()
