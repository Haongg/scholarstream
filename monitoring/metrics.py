"""Pipeline run metrics in Postgres, next to the ingestion watermark.

    pipeline_task_runs    one row per task run: status, timing, API credits
    pipeline_day_metrics  one row per (task run, layer, publication day): rows and verdict

Written by the DAGs only (tasks record what they processed, callbacks record status and
timing), never by the CLIs. Every write is an upsert on the run's key, so a retried task or
a re-run overwrites its rows instead of adding new ones. Recording is best-effort: a failure
to write metrics is logged and never fails or blocks the pipeline.

No Airflow import here: the helpers take Airflow's task context as a plain mapping.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime

import psycopg
from psycopg import sql

from ingestion.watermark import connect

log = logging.getLogger("monitoring")

DDL = (
    """
    CREATE TABLE IF NOT EXISTS pipeline_task_runs (
        dag_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        status TEXT NOT NULL,  -- running (task wrote its metrics), success, up_for_retry, failed
        try_number INT,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        credits_used INT,
        credits_remaining INT,
        budget_exhausted BOOLEAN,
        error TEXT,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (dag_id, run_id, task_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_day_metrics (
        dag_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        layer TEXT NOT NULL,  -- bronze, silver, gate, gold
        day DATE NOT NULL,
        rows INT,
        verdict TEXT NOT NULL,  -- loaded, transformed, passed, deferred, failed, built
        detail TEXT,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (dag_id, run_id, task_id, layer, day)
    )
    """,
)

TASK_FIELDS = (
    "status",
    "try_number",
    "started_at",
    "finished_at",
    "credits_used",
    "credits_remaining",
    "budget_exhausted",
    "error",
)


@dataclass(frozen=True)
class TaskKey:
    dag_id: str
    run_id: str
    task_id: str


@dataclass(frozen=True)
class DayMetric:
    day: date
    rows: int | None
    verdict: str
    detail: str | None = None


def task_key(context: Mapping) -> TaskKey:
    ti = context["ti"]
    return TaskKey(ti.dag_id, ti.run_id, ti.task_id)


def ensure_tables(conn: psycopg.Connection) -> None:
    for statement in DDL:
        conn.execute(statement)


def upsert_task(conn: psycopg.Connection, key: TaskKey, **fields) -> None:
    """Insert the task run, or update only the given fields of an existing row.

    The task writes its own fields (credits) while it runs and the callback adds status
    and timing afterwards; neither overwrites what the other wrote. A write without a
    status creates the row as "running" but never changes the status of an existing row.
    """
    unknown = set(fields) - set(TASK_FIELDS)
    if unknown:
        raise ValueError(f"unknown task fields: {sorted(unknown)}")
    updates = list(fields)  # only what the caller passed
    inserted = fields if "status" in fields else {"status": "running", **fields}
    columns = ["dag_id", "run_id", "task_id", *inserted]
    assignments = [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in updates]
    query = sql.SQL(
        "INSERT INTO pipeline_task_runs ({columns}) VALUES ({values}) "
        "ON CONFLICT (dag_id, run_id, task_id) DO UPDATE SET {assignments}"
    ).format(
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        assignments=sql.SQL(", ").join([*assignments, sql.SQL("recorded_at = now()")]),
    )
    conn.execute(query, (key.dag_id, key.run_id, key.task_id, *inserted.values()))


def replace_days(conn: psycopg.Connection, key: TaskKey, layer: str, metrics: list[DayMetric]) -> None:
    """Make the task's rows for this layer exactly `metrics`, so a retry that processed
    fewer days does not leave rows from the earlier try behind."""
    with conn.transaction():
        conn.execute(
            "DELETE FROM pipeline_day_metrics WHERE dag_id = %s AND run_id = %s AND task_id = %s AND layer = %s",
            (key.dag_id, key.run_id, key.task_id, layer),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO pipeline_day_metrics (dag_id, run_id, task_id, layer, day, rows, verdict, detail) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                [(key.dag_id, key.run_id, key.task_id, layer, m.day, m.rows, m.verdict, m.detail) for m in metrics],
            )


def best_effort(action: str, write: Callable[[psycopg.Connection], None], connector=None) -> bool:
    """Run `write` on a fresh connection; log and swallow any error. True if it succeeded."""
    try:
        with (connector or connect)() as conn:
            ensure_tables(conn)
            write(conn)
        return True
    except Exception as exc:  # noqa: BLE001 -- metrics must never break the pipeline
        log.warning("Could not record %s: %s: %s", action, type(exc).__name__, exc)
        return False


def record_days(context: Mapping, layer: str, metrics: list[DayMetric], **task_fields) -> bool:
    """Record the days a task processed (and optionally its own fields, e.g. credits)."""
    key = task_key(context)

    def write(conn):
        upsert_task(conn, key, **task_fields)
        replace_days(conn, key, layer, metrics)

    return best_effort(f"{layer} metrics of {key.dag_id}.{key.task_id}", write)


def record_ingest(context: Mapping, result) -> bool:
    """Record an ingestion.run.IngestResult: works per bronze day and the API credits."""
    return record_days(
        context,
        "bronze",
        [DayMetric(day, works, "loaded") for day, works in result.works.items()],
        credits_used=result.credits_used,
        credits_remaining=result.credits_remaining,
        budget_exhausted=result.budget_exhausted,
    )


def record_status(context: Mapping, status: str, finished_at: datetime, error: str | None = None) -> bool:
    """Record a task run's outcome and timing; called from the task callbacks."""
    ti = context["ti"]
    key = task_key(context)
    fields = {
        "status": status,
        "try_number": getattr(ti, "try_number", None),
        "started_at": getattr(ti, "start_date", None),
        "finished_at": finished_at,
        "error": error,
    }
    return best_effort(f"{status} of {key.dag_id}.{key.task_id}", lambda conn: upsert_task(conn, key, **fields))
