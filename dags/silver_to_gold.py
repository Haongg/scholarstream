"""Silver -> gold with dbt. Triggered by SILVER_WORKS; emits GOLD.

`dbt build` rebuilds every gold table in full from the silver days marked _QUALITY_OK, then
runs the dbt tests. So the event's list of days is not needed: a full rebuild is idempotent,
and it also picks up days marked by a run whose event was folded into this one.

GOLD is emitted only if the build and every test pass (a non-zero exit fails the task), so
consumers downstream (ML) never start on gold that failed its tests.

dbt runs from its own venv in the image (/opt/dbt), apart from Airflow's pinned libraries.
The project is mounted read-only, so dbt's target/, logs/ and DuckDB catalog go to /tmp;
the catalog only holds views over gold's Parquet, which lives in the gold bucket.
"""

from datetime import UTC, datetime

from airflow.sdk import dag, task
from scholarstream_assets import GOLD, SILVER_WORKS

DBT = "/opt/dbt/bin/dbt"
PROJECT = "/opt/airflow/project/dbt"
SCRATCH = "/tmp/dbt"


@dag(
    dag_id="silver_to_gold",
    schedule=[SILVER_WORKS],
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # one writer of the gold files at a time
    tags=["scholarstream", "gold"],
    doc_md=__doc__,
)
def silver_to_gold():
    @task.bash(
        outlets=[GOLD],
        env={
            "DBT_DUCKDB_PATH": f"{SCRATCH}/scholarstream.duckdb",
            "DBT_TARGET_PATH": f"{SCRATCH}/target",
            "DBT_LOG_PATH": f"{SCRATCH}/logs",
        },
        append_env=True,  # keep MinIO credentials and S3_ENDPOINT_URL from the container
    )
    def dbt_build() -> str:
        return f"cd {PROJECT} && {DBT} build --profiles-dir ."

    dbt_build()


silver_to_gold()
