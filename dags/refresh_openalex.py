"""Weekly re-fetch of days older than the daily lookback window. Emits BRONZE_WORKS.

OpenAlex keeps adding works to a publication day (and moving works to another day) for
weeks; on 2026-11-22 about 6% of the works of 10-01..11-14 had appeared or changed after
their day left the 7-day lookback. The daily run only re-fetches that window, so this DAG
re-fetches the INGEST_REFRESH_DAYS days before it (by default the days 8..60 back).

Runs Sundays at 01:00 UTC, after the daily 00:15 run, so the two never write bronze at the
same time and the daily load has the budget first. A rewritten bronze day is newer than its
silver, so bronze_to_silver re-transforms and re-checks it like any other stale day.
Days left unfinished by a spent budget are covered again next week (the windows overlap).
"""

import logging
from datetime import UTC, datetime, timedelta

from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from scholarstream_assets import BRONZE_WORKS

from monitoring.callbacks import TASK_CALLBACKS

log = logging.getLogger(__name__)


@dag(
    dag_id="refresh_openalex",
    schedule=CronTriggerTimetable("0 1 * * 0", timezone="UTC"),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={**TASK_CALLBACKS, "retries": 1, "retry_delay": timedelta(minutes=10)},  # a count mismatch may clear up
    tags=["scholarstream", "bronze"],
    doc_md=__doc__,
)
def refresh_openalex():
    @task(outlets=[BRONZE_WORKS])
    def refresh() -> list[str]:
        from ingestion.run import ingest, lookback_days, refresh_days, refresh_window
        from monitoring.metrics import record_ingest

        context = get_current_context()
        run_date = (context.get("logical_date") or context["dag_run"].run_after).date()
        window = refresh_window(run_date - timedelta(days=1), lookback_days(), refresh_days())
        days: list[str] = []
        budget_exhausted = False
        if window is None:
            log.info("Nothing to refresh: the lookback window covers INGEST_REFRESH_DAYS")
        else:
            log.info("Refreshing %s..%s", *window)
            result = ingest(*window)  # CountMismatch raises and fails the task, so no event
            record_ingest(context, result)  # best-effort: never fails the task
            if result.budget_exhausted:
                log.warning("API budget spent after %d day(s); the rest is refreshed next week", len(result.loaded))
            days = [d.isoformat() for d in result.loaded]
            budget_exhausted = result.budget_exhausted
        context["outlet_events"][BRONZE_WORKS].extra = {
            "days": days,
            "budget_exhausted": budget_exhausted,
            "refresh": True,
        }
        return days

    refresh()


refresh_openalex()
