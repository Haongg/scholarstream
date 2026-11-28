"""Tests for monitoring.metrics against a real Postgres, in a throwaway schema.

Uses the credentials from .env (the local compose stack) or the environment (CI's Postgres
service) and skips when Postgres is not reachable, unless REQUIRE_POSTGRES is set (CI).
The schema is dropped afterwards, so the real tables are untouched.
"""

import logging
import os
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from dotenv import dotenv_values

from ingestion import watermark
from monitoring import metrics
from monitoring.metrics import DayMetric, TaskKey

ROOT = Path(__file__).resolve().parent.parent
D1, D2, D3 = date(2026, 11, 20), date(2026, 11, 21), date(2026, 11, 22)
STARTED = datetime(2026, 11, 28, 0, 15, tzinfo=UTC)
FINISHED = datetime(2026, 11, 28, 0, 20, tzinfo=UTC)


def context(task_id="ingest", try_number=1):
    ti = SimpleNamespace(
        dag_id="ingest_openalex", run_id="scheduled__2026-11-28", task_id=task_id, try_number=try_number, start_date=STARTED
    )
    return {"ti": ti}


@pytest.fixture
def pg(monkeypatch):
    for name, value in dotenv_values(ROOT / ".env").items():
        if name.startswith("POSTGRES_") and value is not None:
            monkeypatch.setenv(name, value)
    # CI sets REQUIRE_POSTGRES (it runs a Postgres service), so a broken service fails the
    # run instead of silently skipping these tests.
    unavailable = pytest.fail if os.getenv("REQUIRE_POSTGRES") else pytest.skip
    if not {"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"} <= os.environ.keys():
        unavailable("no Postgres credentials (.env or environment)")
    try:
        admin = watermark.connect()
    except psycopg.OperationalError as exc:
        unavailable(f"Postgres not reachable: {exc}")
    schema = f"test_metrics_{uuid.uuid4().hex[:8]}"
    admin.execute(f'CREATE SCHEMA "{schema}"')

    def connector():
        conn = watermark.connect()
        conn.execute(f'SET search_path TO "{schema}"')
        return conn

    monkeypatch.setattr(metrics, "connect", connector)
    conn = connector()
    try:
        yield conn
    finally:
        conn.close()
        admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        admin.close()


def task_row(conn):
    rows = conn.execute(
        "SELECT status, try_number, credits_used, started_at, finished_at FROM pipeline_task_runs"
    ).fetchall()
    assert len(rows) == 1
    return rows[0]


def day_rows(conn):
    return conn.execute("SELECT layer, day, rows, verdict FROM pipeline_day_metrics ORDER BY day").fetchall()


def test_recording_a_run_twice_leaves_one_set_of_rows(pg):
    days = [DayMetric(D1, 10, "loaded"), DayMetric(D2, 20, "loaded")]

    assert metrics.record_days(context(), "bronze", days, credits_used=3)
    assert metrics.record_days(context(), "bronze", days, credits_used=3)

    assert day_rows(pg) == [("bronze", D1, 10, "loaded"), ("bronze", D2, 20, "loaded")]
    assert task_row(pg)[:3] == ("running", None, 3)


def test_a_retry_that_processed_fewer_days_replaces_the_earlier_rows(pg):
    metrics.record_days(context(), "bronze", [DayMetric(D1, 10, "loaded"), DayMetric(D2, 20, "loaded")])
    metrics.record_days(context(), "bronze", [DayMetric(D3, 30, "loaded")])

    assert day_rows(pg) == [("bronze", D3, 30, "loaded")]


def test_layers_of_one_task_are_kept_apart(pg):
    metrics.record_days(context("quality_gate"), "gate", [DayMetric(D1, 10, "passed")])
    metrics.record_days(context("quality_gate"), "silver", [DayMetric(D1, 10, "transformed")])
    metrics.record_days(context("quality_gate"), "gate", [DayMetric(D1, 10, "deferred")])

    assert sorted(day_rows(pg)) == [("gate", D1, 10, "deferred"), ("silver", D1, 10, "transformed")]


def test_callback_status_keeps_the_credits_the_task_wrote(pg):
    metrics.record_days(context(), "bronze", [DayMetric(D1, 10, "loaded")], credits_used=5)
    metrics.record_status(context(), "success", FINISHED)

    assert task_row(pg) == ("success", 1, 5, STARTED, FINISHED)


def test_retry_overwrites_credits_and_success_closes_the_row(pg):
    metrics.record_days(context(try_number=1), "bronze", [], credits_used=5)
    metrics.record_status(context(try_number=1), "up_for_retry", FINISHED, error="CountMismatch")
    metrics.record_days(context(try_number=2), "bronze", [DayMetric(D1, 10, "loaded")], credits_used=7)

    status, try_number, credits, *_ = task_row(pg)
    assert (status, try_number, credits) == ("up_for_retry", 1, 7)  # the task's write leaves status alone

    metrics.record_status(context(try_number=2), "success", FINISHED)
    assert task_row(pg)[:3] == ("success", 2, 7)


def test_unknown_task_field_is_rejected(pg):
    with pytest.raises(ValueError, match="unknown task fields"):
        metrics.upsert_task(pg, TaskKey("d", "r", "t"), credits=1)


def test_a_failing_write_is_logged_and_swallowed(caplog):
    def broken():
        raise OSError("connection refused")

    with caplog.at_level(logging.WARNING, logger="monitoring"):
        assert metrics.best_effort("bronze metrics", lambda conn: None, connector=broken) is False
    assert "Could not record bronze metrics: OSError: connection refused" in caplog.text
