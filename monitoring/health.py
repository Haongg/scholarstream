"""Pipeline health: catch a pipeline that stopped without any task failing.

    python -m monitoring.health                    # as of now
    python -m monitoring.health --as-of 2026-12-01 # pretend it is 06:00 UTC that day

A task failure already alerts through its callback, but a pipeline that simply stops running
does not: the stack was down 2026-11-23..11-27 and nothing noticed. These checks look at the
results instead (gold, the run log, the silver markers), so they catch that too:

    gold freshness     the latest day in gold is at most HEALTH_MAX_GOLD_LAG_DAYS old (default
                       3; normally 1-2, since yesterday is often deferred by the gate)
    ingest recency     the last successful ingest_openalex run finished at most
                       HEALTH_MAX_INGEST_AGE_HOURS ago (default 26: one daily run plus slack)
    stuck silver days  no silver day past the lookback window is still without _QUALITY_OK
                       (a failed gate keeps such a day out of gold until someone fixes it)
    API budget         the budget did not run out in the last 2 ingest/refresh runs in a row

Limitation: the checks run inside the same Airflow, so while the whole stack is down they do
not run either; they report the gap on the first run after it comes back. An outside
heartbeat (dead man's switch) is needed to alert during an outage; planned with the cloud move.

Exit code 0 if every check passes, 1 otherwise (the DAG task then fails and alerts).
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

log = logging.getLogger("monitoring.health")

GOLD_FRESHNESS = "gold freshness"
INGEST_RECENCY = "ingest recency"
STUCK_DAYS = "stuck silver days"
BUDGET = "API budget"

DEFAULT_MAX_GOLD_LAG_DAYS = 3
DEFAULT_MAX_INGEST_AGE_HOURS = 26
BUDGET_STREAK = 2
RUN_HOUR = 6  # the DAG runs at 06:00 UTC; --as-of pretends to be that moment


@dataclass
class HealthInputs:
    last_gold_day: date | None
    last_ingest_success: datetime | None
    unchecked_days: list[date] = field(default_factory=list)
    recent_budget_exhausted: list[bool] = field(default_factory=list)  # newest first


def check_gold_freshness(last_gold_day: date | None, today: date, max_lag_days: int) -> list[str]:
    if last_gold_day is None:
        return ["gold is empty or missing"]
    lag = (today - last_gold_day).days
    if lag > max_lag_days:
        return [f"latest gold day {last_gold_day} is {lag} days old (max {max_lag_days})"]
    return []


def check_ingest_recency(last_success: datetime | None, now: datetime, max_age: timedelta) -> list[str]:
    if last_success is None:
        return ["no successful ingest_openalex run on record"]
    age = now - last_success
    if age > max_age:
        hours, limit = age.total_seconds() / 3600, max_age.total_seconds() / 3600
        when = f"{last_success:%Y-%m-%d %H:%M} UTC"
        return [f"last successful ingest finished {hours:.0f} h ago ({when}; max {limit:.0f} h)"]
    return []


def check_stuck_days(unchecked: list[date], today: date, lookback: int) -> list[str]:
    from quality.run import is_settling

    stuck = [d for d in unchecked if not is_settling(d, today, lookback)]
    if stuck:
        days = ", ".join(d.isoformat() for d in stuck)
        return [f"{len(stuck)} silver day(s) past the {lookback}-day lookback still lack _QUALITY_OK: {days}"]
    return []


def check_budget(recent_exhausted: list[bool], streak: int = BUDGET_STREAK) -> list[str]:
    if len(recent_exhausted) >= streak and all(recent_exhausted[:streak]):
        return [f"the API budget ran out in each of the last {streak} ingest/refresh runs"]
    return []


def run_checks(
    inputs: HealthInputs, now: datetime, max_lag_days: int, max_ingest_age: timedelta, lookback: int
) -> dict[str, list[str]]:
    today = now.date()
    return {
        GOLD_FRESHNESS: check_gold_freshness(inputs.last_gold_day, today, max_lag_days),
        INGEST_RECENCY: check_ingest_recency(inputs.last_ingest_success, now, max_ingest_age),
        STUCK_DAYS: check_stuck_days(inputs.unchecked_days, today, lookback),
        BUDGET: check_budget(inputs.recent_budget_exhausted),
    }


def collect(s3, conn) -> HealthInputs:
    """Read the check inputs from gold, the silver markers and the run log in Postgres."""
    from monitoring import gold
    from monitoring.metrics import ensure_tables
    from quality.run import unchecked_days

    ensure_tables(conn)  # a fresh database has no run log yet; the checks then report it
    (last_ingest,) = conn.execute(
        "SELECT max(finished_at) FROM pipeline_task_runs "
        "WHERE dag_id = 'ingest_openalex' AND task_id = 'ingest' AND status = 'success'"
    ).fetchone()
    budget = conn.execute(
        "SELECT budget_exhausted FROM pipeline_task_runs "
        "WHERE dag_id IN ('ingest_openalex', 'refresh_openalex') AND status = 'success' "
        "AND budget_exhausted IS NOT NULL ORDER BY finished_at DESC LIMIT %s",
        (BUDGET_STREAK,),
    ).fetchall()
    return HealthInputs(
        last_gold_day=gold.last_day(s3),
        last_ingest_success=last_ingest,
        unchecked_days=unchecked_days(s3),
        recent_budget_exhausted=[row[0] for row in budget],
    )


def config() -> tuple[int, timedelta, int]:
    from ingestion.run import lookback_days

    max_lag = int(os.getenv("HEALTH_MAX_GOLD_LAG_DAYS") or DEFAULT_MAX_GOLD_LAG_DAYS)
    max_age = timedelta(hours=int(os.getenv("HEALTH_MAX_INGEST_AGE_HOURS") or DEFAULT_MAX_INGEST_AGE_HOURS))
    return max_lag, max_age, lookback_days()


def as_of_moment(as_of: date | None) -> datetime:
    return datetime.combine(as_of, time(RUN_HOUR), UTC) if as_of else datetime.now(UTC)


def health(now: datetime | None = None) -> dict[str, list[str]]:
    """Run every check against the real storage; log and return each check's problems."""
    from ingestion.bronze_writer import s3_client
    from ingestion.watermark import connect

    now = now or datetime.now(UTC)
    with connect() as conn:
        inputs = collect(s3_client(), conn)
    results = run_checks(inputs, now, *config())
    log.info("Health as of %s UTC (latest gold day: %s)", f"{now:%Y-%m-%d %H:%M}", inputs.last_gold_day)
    for name, problems in results.items():
        if not problems:
            log.info("ok   %s", name)
            continue
        log.error("FAIL %s", name)
        for problem in problems:
            log.error("      - %s", problem)
    return results


def summary(results: dict[str, list[str]]) -> str:
    return "; ".join(f"{name}: {p}" for name, problems in results.items() for p in problems)


def main(argv=None) -> int:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", type=date.fromisoformat, help="judge as if it were 06:00 UTC on this day")
    args = parser.parse_args(argv)
    results = health(as_of_moment(args.as_of))
    return 1 if any(results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
