"""Quality gate between silver and gold: check each day's silver tables, fail if anything is off.

    python -m quality.run                                  # every day found in silver
    python -m quality.run --from 2026-10-01 --to 2026-10-03

Exit code 0 means every check passed; 1 means at least one failed (the DAG should stop).
"""

import argparse
import io
import logging
import sys
from datetime import UTC, date, datetime

import polars as pl
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from ingestion.bronze_writer import s3_client
from quality.checks import run_checks
from transform.run import PARTITION, SILVER_BUCKET

log = logging.getLogger("quality")


SILVER_TABLES = ("works", "work_topics")


def silver_days(s3) -> list[date]:
    """Days present in any silver table, so a day missing from one table is still checked."""
    days = set()
    paginator = s3.get_paginator("list_objects_v2")
    for table in SILVER_TABLES:
        for listing in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=f"{table}/", Delimiter="/"):
            for common in listing.get("CommonPrefixes", []):
                days.add(date.fromisoformat(common["Prefix"].rstrip("/").split(PARTITION)[-1]))
    return sorted(days)


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


def gate(s3, days: list[date]) -> bool:
    """Check the given silver days; True if every check passed. Used by the CLI and the DAG."""
    today = datetime.now(UTC).date()
    all_days = silver_days(s3)

    # Row counts of every earlier day, so a day is judged against the days before it.
    # A day whose works table cannot be read is left out of the baseline.
    counts = {}
    for d in all_days:
        works, _ = try_read(s3, "works", d)
        if works is not None:
            counts[d] = works.height

    failed = False
    for day in days:
        log.info("== %s", day)
        works, works_error = try_read(s3, "works", day)
        topics, topics_error = try_read(s3, "work_topics", day)
        if works is None or topics is None:
            failed = True
            log.error("FAIL silver tables readable")
            for reason in (works_error, topics_error):
                if reason:
                    log.error("     - %s", reason)
            continue
        log.info("   %d works, %d work_topics", works.height, topics.height)
        previous = [counts[d] for d in all_days if d < day and d in counts]
        results = run_checks(works, topics, day, today, previous)
        for name, problems in results.items():
            if problems:
                failed = True
                log.error("FAIL %s", name)
                for problem in problems:
                    log.error("     - %s", problem)
            else:
                log.info("ok   %s", name)
    if failed:
        log.error("Quality gate FAILED: do not promote to gold")
        return False
    log.info("Quality gate passed")
    return True


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    args = parser.parse_args(argv)

    s3 = s3_client()
    days = [d for d in silver_days(s3) if (not args.start or d >= args.start) and (not args.end or d <= args.end)]
    if not days:
        log.info("No silver partitions in range")
        return 0
    return 0 if gate(s3, days) else 1


if __name__ == "__main__":
    sys.exit(main())
