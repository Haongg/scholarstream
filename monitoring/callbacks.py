"""Task callbacks shared by every DAG: record each task run's outcome, and alert on failure.

Alerts go to the log only for now: an ERROR line starting with "ALERT" on the
`scholarstream.alerts` logger (in the failed task's log), plus a `failed` row with the error
in pipeline_task_runs, so `SELECT * FROM pipeline_task_runs WHERE status = 'failed'` lists
every alert. A retry is a warning, not an alert. Deferred days are normal and never alert.

Imports of the metrics module happen inside the callbacks, so parsing a DAG stays light.
"""

import logging
from collections.abc import Mapping
from datetime import UTC, datetime

log = logging.getLogger("scholarstream.alerts")


def describe(context: Mapping) -> str:
    ti = context["ti"]
    return f"{ti.dag_id}.{ti.task_id} (run {ti.run_id}, try {ti.try_number})"


def error_text(context: Mapping) -> str | None:
    exc = context.get("exception")
    if exc is None:
        return None
    return f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__


def _record(context: Mapping, status: str, error: str | None = None) -> None:
    from monitoring.metrics import record_status

    record_status(context, status, datetime.now(UTC), error)


def on_success(context: Mapping) -> None:
    _record(context, "success")


def on_skipped(context: Mapping) -> None:
    _record(context, "skipped")


def on_retry(context: Mapping) -> None:
    error = error_text(context)
    log.warning("RETRY %s: %s", describe(context), error)
    _record(context, "up_for_retry", error)


def on_failure(context: Mapping) -> None:
    error = error_text(context)
    log_url = getattr(context["ti"], "log_url", None)
    log.error("ALERT %s failed: %s%s", describe(context), error, f" (log: {log_url})" if log_url else "")
    _record(context, "failed", error)


TASK_CALLBACKS = {
    "on_success_callback": on_success,
    "on_skipped_callback": on_skipped,
    "on_retry_callback": on_retry,
    "on_failure_callback": on_failure,
}
