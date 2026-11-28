"""Bronze -> silver, then the quality gate. Triggered by BRONZE_WORKS; emits SILVER_WORKS.

Which days to transform comes from storage, not from the triggering event: a day is stale
when its bronze _SUCCESS marker is newer than its silver files. So days loaded by an ingest
run that later failed (and never emitted an event) are still picked up by the next run, and
several bronze events collapsed into one run need no merging.

SILVER_WORKS is emitted by the quality gate, not by transform, so downstream (gold) only
starts on silver that passed. A failed gate fails the run without retrying.

The gate checks every silver day without a _QUALITY_OK marker, not just the days this run
transformed: transform has already written a failed day's silver, so on the next run it is
no longer stale, yet it must still not count as passed.

A young day that is only short of works is deferred by the gate (see quality.run): the run
succeeds, but the day stays unmarked and is left out of the event until it settles.
"""

import logging
from datetime import UTC, datetime

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from scholarstream_assets import BRONZE_WORKS, SILVER_WORKS

log = logging.getLogger(__name__)


@dag(
    dag_id="bronze_to_silver",
    schedule=[BRONZE_WORKS],
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # events arriving meanwhile are folded into the next run
    tags=["scholarstream", "silver"],
    doc_md=__doc__,
)
def bronze_to_silver():
    @task
    def transform() -> list[str]:
        from ingestion.bronze_writer import s3_client
        from monitoring.metrics import DayMetric, record_days
        from transform.run import stale_days, transform_days

        s3 = s3_client()
        days = stale_days(s3)
        counts = transform_days(s3, days)
        record_days(
            get_current_context(),
            "silver",
            [
                DayMetric(day, works, "transformed", f"{work_topics} work_topics, {topics} topics")
                for day, (works, work_topics, topics) in counts.items()
            ],
        )
        return [d.isoformat() for d in days]

    @task(outlets=[SILVER_WORKS])
    def quality_gate(transformed: list[str]) -> None:
        from ingestion.bronze_writer import s3_client
        from monitoring.metrics import DayMetric, record_days
        from quality.run import PASSED, gate, unchecked_days

        s3 = s3_client()
        pending = unchecked_days(s3)
        log.info("Transformed %d day(s); %d silver day(s) await the gate", len(transformed), len(pending))
        if not pending:
            raise AirflowSkipException("Every silver day has passed the gate")  # no event
        result = gate(s3, pending)
        record_days(  # before failing, so a failed day's problems are on record too
            get_current_context(),
            "gate",
            [
                DayMetric(day, v.works, v.verdict, "; ".join(v.problems + v.warnings) or None)
                for day, v in result.days.items()
            ],
        )
        if not result.ok:
            raise AirflowFailException("Quality gate failed; see log. Failed days stay unchecked.")
        # Deferred days (young, still settling) pass the gate unmarked; announce only marked ones.
        passed = [d.isoformat() for d in result.with_verdict(PASSED)]
        if not passed:
            raise AirflowSkipException("Only settling days were checked; nothing new for gold")
        get_current_context()["outlet_events"][SILVER_WORKS].extra = {"days": passed}

    quality_gate(transform())


bronze_to_silver()
