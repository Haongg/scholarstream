"""Bronze -> silver, then the quality gate. Triggered by BRONZE_WORKS; emits SILVER_WORKS.

Which days to transform comes from storage, not from the triggering event: a day is stale
when its bronze _SUCCESS marker is newer than its silver files. So days loaded by an ingest
run that later failed (and never emitted an event) are still picked up by the next run, and
several bronze events collapsed into one run need no merging.

SILVER_WORKS is emitted by the quality gate, not by transform, so downstream (gold) only
starts on silver that passed. A failed gate fails the run without retrying.
"""

from datetime import UTC, date, datetime

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from scholarstream_assets import BRONZE_WORKS, SILVER_WORKS


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
        from transform.run import stale_days, transform_days

        s3 = s3_client()
        days = stale_days(s3)
        transform_days(s3, days)
        return [d.isoformat() for d in days]

    @task(outlets=[SILVER_WORKS])
    def quality_gate(days: list[str]) -> None:
        from ingestion.bronze_writer import s3_client
        from quality.run import gate

        if not days:
            raise AirflowSkipException("No stale days: silver is already up to date")  # no event
        if not gate(s3_client(), [date.fromisoformat(d) for d in days]):
            raise AirflowFailException(f"Quality gate failed for {days}; see log. Not promoting.")
        get_current_context()["outlet_events"][SILVER_WORKS].extra = {"days": days}

    quality_gate(transform())


bronze_to_silver()
