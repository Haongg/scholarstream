"""Quality gate between silver and gold: check each day's silver tables, fail if anything is off.

    python -m quality.run                                  # every day found in silver
    python -m quality.run --from 2026-10-01 --to 2026-10-03
    python -m quality.run --unchecked                      # only days without a pass marker

Each day that passes gets a _QUALITY_OK marker in silver (transform removes it when it rewrites
the day). A day that fails stays unmarked, so the DAG keeps re-checking it on every run
until it passes, instead of dropping it once it is no longer freshly transformed.

A day still inside the ingestion lookback window is settling: OpenAlex may not have indexed it
fully, and ingestion re-fetches it on every run. If the row count is its only failure, it is
deferred: not a failure, but not marked either, so gold leaves it out for now and the gate
checks it again next run. Once it leaves the window its final version gets the full check.
Warnings (a day far above its usual size) are logged and never block.

Exit code 0 means nothing failed (deferred days allowed); 1 means at least one day failed
(the DAG should stop).
"""

import argparse
import io
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import polars as pl
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from ingestion.bronze_writer import last_modified, s3_client
from ingestion.run import lookback_days
from quality.checks import ROW_COUNT_CHECK, run_checks, run_warnings
from transform.run import PARTITION, SILVER_BUCKET, quality_marker_key

log = logging.getLogger("quality")


SILVER_TABLES = ("works", "work_topics", "topics")


def silver_days(s3) -> list[date]:
    """Days present in any silver table, so a day missing from one table is still checked."""
    days = set()
    paginator = s3.get_paginator("list_objects_v2")
    for table in SILVER_TABLES:
        for listing in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=f"{table}/", Delimiter="/"):
            for common in listing.get("CommonPrefixes", []):
                days.add(date.fromisoformat(common["Prefix"].rstrip("/").split(PARTITION)[-1]))
    return sorted(days)


def is_checked(s3, day: date) -> bool:
    return last_modified(s3, SILVER_BUCKET, quality_marker_key(day)) is not None


def unchecked_days(s3) -> list[date]:
    """Silver days that have not passed the gate since they were last written."""
    return [d for d in silver_days(s3) if not is_checked(s3, d)]


def is_settling(day: date, today: date, lookback: int) -> bool:
    """Inside the lookback window, which ingestion re-fetches every run: yesterday back to
    `lookback` days ago. The last time a day is loaded it is `lookback` days old."""
    return (today - day).days <= lookback


def mark_checked(s3, day: date) -> None:
    s3.put_object(Bucket=SILVER_BUCKET, Key=quality_marker_key(day), Body=b"")


def read_table(s3, table: str, day: date) -> pl.DataFrame:
    key = f"{table}/{PARTITION}{day.isoformat()}/{table}.parquet"
    return pl.read_parquet(io.BytesIO(s3.get_object(Bucket=SILVER_BUCKET, Key=key)["Body"].read()))


def try_read(s3, table: str, day: date) -> tuple[pl.DataFrame | None, str | None]:
    """Read a silver table, returning (table, None) or (None, reason) instead of raising."""
    try:
        return read_table(s3, table, day), None
    except ClientError as error:
        return None, f"{table}: {error.response['Error']['Code']}"
    except (pl.exceptions.PolarsError, OSError) as error:
        return None, f"{table}: unreadable parquet ({type(error).__name__})"


PASSED, DEFERRED, FAILED = "passed", "deferred", "failed"


@dataclass
class DayVerdict:
    verdict: str  # passed (and marked), deferred (settling, unmarked) or failed
    works: int | None  # None if the works table could not be read
    problems: list[str] = field(default_factory=list)  # failing (or deferring) checks
    warnings: list[str] = field(default_factory=list)  # non-blocking


@dataclass
class GateResult:
    days: dict[date, DayVerdict] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """Nothing failed; deferred days are allowed."""
        return all(v.verdict != FAILED for v in self.days.values())

    def with_verdict(self, verdict: str) -> list[date]:
        return [d for d, v in self.days.items() if v.verdict == verdict]


def gate(s3, days: list[date]) -> GateResult:
    """Check the given silver days and mark each one that passes. `.ok` is True if none
    failed (deferred days do not count as failed). Used by the CLI and the DAG."""
    today = datetime.now(UTC).date()
    lookback = lookback_days()

    # Row counts of every day, for the row-count baseline (earlier weeks of the same weekday).
    # A day whose works table cannot be read is left out of the baseline.
    counts = {}
    for d in silver_days(s3):
        works, _ = try_read(s3, "works", d)
        if works is not None:
            counts[d] = works.height

    result = GateResult()
    for day in days:
        log.info("== %s", day)
        works, works_error = try_read(s3, "works", day)
        topics, topics_error = try_read(s3, "work_topics", day)
        topic_dim, topic_dim_error = try_read(s3, "topics", day)
        if works is None or topics is None or topic_dim is None:
            reasons = [r for r in (works_error, topics_error, topic_dim_error) if r]
            log.error("FAIL silver tables readable")
            for reason in reasons:
                log.error("     - %s", reason)
            result.days[day] = DayVerdict(
                FAILED, works.height if works is not None else None, [f"silver tables readable: {r}" for r in reasons]
            )
            continue
        log.info("   %d works, %d work_topics, %d topics", works.height, topics.height, topic_dim.height)
        results = run_checks(works, topics, day, today, counts, topic_dim)
        failing = {name for name, problems in results.items() if problems}
        defer = failing == {ROW_COUNT_CHECK} and is_settling(day, today, lookback)
        verdict = DayVerdict(DEFERRED if defer else FAILED if failing else PASSED, works.height)
        for name, problems in results.items():
            if not problems:
                log.info("ok   %s", name)
                continue
            level, label = (logging.WARNING, "DEFER") if defer else (logging.ERROR, "FAIL ")
            log.log(level, "%s %s", label, name)
            for problem in problems:
                log.log(level, "      - %s", problem)
                verdict.problems.append(f"{name}: {problem}")
        for name, warnings in run_warnings(works, day, counts).items():
            for warning in warnings:
                log.warning("WARN  %s: %s (not blocking; gold filters it)", name, warning)
                verdict.warnings.append(f"{name}: {warning}")
        if defer:
            log.warning("      %s is within the %d-day lookback window: left unmarked, re-checked next run", day, lookback)
        elif not failing:
            mark_checked(s3, day)
        result.days[day] = verdict
    deferred = result.with_verdict(DEFERRED)
    if deferred:
        log.warning("Deferred (still settling, not in gold yet): %s", ", ".join(d.isoformat() for d in deferred))
    if not result.ok:
        log.error("Quality gate FAILED: do not promote to gold")
    else:
        log.info("Quality gate passed")
    return result


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    parser.add_argument("--unchecked", action="store_true", help="only days without a pass marker")
    args = parser.parse_args(argv)

    s3 = s3_client()
    candidates = unchecked_days(s3) if args.unchecked else silver_days(s3)
    days = [d for d in candidates if (not args.start or d >= args.start) and (not args.end or d <= args.end)]
    if not days:
        log.info("No silver partitions in range")
        return 0
    return 0 if gate(s3, days).ok else 1


if __name__ == "__main__":
    sys.exit(main())
