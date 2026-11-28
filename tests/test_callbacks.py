"""Tests for the shared task callbacks: what they log and which status they record."""

import logging
from types import SimpleNamespace

import pytest

from monitoring import callbacks, metrics


@pytest.fixture
def recorded(monkeypatch):
    calls = []
    monkeypatch.setattr(
        metrics, "record_status", lambda context, status, finished_at, error=None: calls.append((status, error))
    )
    return calls


def context(exception=None):
    ti = SimpleNamespace(
        dag_id="bronze_to_silver", task_id="quality_gate", run_id="r1", try_number=1, log_url="http://localhost:8080/x"
    )
    return {"ti": ti, "exception": exception}


def test_failure_logs_an_alert_and_records_the_error(recorded, caplog):
    with caplog.at_level(logging.ERROR, logger="scholarstream.alerts"):
        callbacks.on_failure(context(RuntimeError("Quality gate failed")))

    assert recorded == [("failed", "RuntimeError: Quality gate failed")]
    assert "ALERT bronze_to_silver.quality_gate (run r1, try 1) failed: RuntimeError: Quality gate failed" in caplog.text
    assert "(log: http://localhost:8080/x)" in caplog.text


def test_retry_is_a_warning_not_an_alert(recorded, caplog):
    with caplog.at_level(logging.WARNING, logger="scholarstream.alerts"):
        callbacks.on_retry(context(ValueError()))

    assert recorded == [("up_for_retry", "ValueError")]
    assert "RETRY" in caplog.text
    assert "ALERT" not in caplog.text


def test_success_and_skip_only_record(recorded, caplog):
    with caplog.at_level(logging.DEBUG, logger="scholarstream.alerts"):
        callbacks.on_success(context())
        callbacks.on_skipped(context())

    assert recorded == [("success", None), ("skipped", None)]
    assert caplog.text == ""


def test_every_outcome_has_a_callback():
    assert set(callbacks.TASK_CALLBACKS) == {
        "on_success_callback",
        "on_skipped_callback",
        "on_retry_callback",
        "on_failure_callback",
    }
