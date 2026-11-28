"""Transform bronze pages into silver Parquet tables, one publication day at a time.

    python -m transform.run                                  # every day found in bronze
    python -m transform.run --from 2026-10-01 --to 2026-10-03
    python -m transform.run --stale                          # only days reloaded since silver was built

Each day's silver files are overwritten in place, so re-running a day never duplicates rows.
Rewriting a day removes its quality marker, so the gate must pass it again.
Only bronze days carrying the _SUCCESS marker are read; a partial partition (ingestion
stopped mid-day) is skipped with a warning so silver is never built from incomplete data.
"""

import argparse
import gzip
import io
import json
import logging
import sys
from datetime import date, datetime

import polars as pl
from dotenv import load_dotenv

from ingestion.bronze_writer import (
    BRONZE_BUCKET,
    BronzeWriter,
    last_modified,
    s3_client,
)
from transform.silver import to_tables

SILVER_BUCKET = "silver"
PARTITION = "publication_date="
# Written by the quality gate when a day passes; gold reads only days that carry it.
QUALITY_MARKER = "_QUALITY_OK"


def quality_marker_key(day: date) -> str:
    return f"works/{PARTITION}{day.isoformat()}/{QUALITY_MARKER}"

log = logging.getLogger("transform")


def bronze_days(s3) -> list[date]:
    """Publication days that have a partition in bronze."""
    days = []
    paginator = s3.get_paginator("list_objects_v2")
    for listing in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix="works/", Delimiter="/"):
        for common in listing.get("CommonPrefixes", []):
            days.append(date.fromisoformat(common["Prefix"].rstrip("/").split(PARTITION)[-1]))
    return sorted(days)


def read_day(s3, day: date) -> list[dict]:
    """All raw works of a day, across its bronze pages."""
    prefix = f"works/{PARTITION}{day.isoformat()}/"
    works = []
    for listing in s3.get_paginator("list_objects_v2").paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in listing.get("Contents", []):
            if not obj["Key"].endswith(".json.gz"):
                continue  # the _SUCCESS marker
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read()
            works.extend(json.loads(gzip.decompress(body))["results"])
    return works


def write_table(s3, table: str, day: date, df: pl.DataFrame) -> None:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    key = f"{table}/{PARTITION}{day.isoformat()}/{table}.parquet"
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buffer.getvalue())


def transform_day(s3, day: date) -> tuple[int, int, int]:
    works, work_topics, topics = to_tables(read_day(s3, day))
    # The day's silver is about to change, so its earlier quality pass no longer holds. Removed
    # first: if the writes below fail halfway, the day is left unchecked, never falsely passed.
    s3.delete_object(Bucket=SILVER_BUCKET, Key=quality_marker_key(day))
    write_table(s3, "works", day, works)
    write_table(s3, "topics", day, topics)
    write_table(s3, "work_topics", day, work_topics)  # last: stale_days reads its timestamp
    return len(works), len(work_topics), len(topics)


def complete_days(s3, start: date | None = None, end: date | None = None) -> dict[date, datetime]:
    """Bronze days in range that carry the _SUCCESS marker, with the marker's write time."""
    bronze = BronzeWriter(s3)
    complete = {}
    for day in bronze_days(s3):
        if (start and day < start) or (end and day > end):
            continue
        marked = bronze.completed_at(day)
        if marked is None:
            log.warning("Skipping %s: bronze partition has no _SUCCESS marker (partial load)", day)
        else:
            complete[day] = marked
    return complete


def stale_days(s3) -> list[date]:
    """Complete bronze days whose silver is missing or older than the latest bronze load.

    Derived from storage rather than from whichever run triggered us, so a day loaded by a
    run that later failed (and so never announced it) is still picked up next time.
    work_topics is written last, so its timestamp is when the day's silver was finished.
    """
    stale = []
    for day, marked in complete_days(s3).items():
        built = last_modified(s3, SILVER_BUCKET, f"work_topics/{PARTITION}{day.isoformat()}/work_topics.parquet")
        if built is None or built <= marked:  # <= : S3 timestamps are coarse; a redo is harmless
            stale.append(day)
    return stale


def transform_days(s3, days: list[date]) -> dict[date, tuple[int, int, int]]:
    """Transform each day; returns its (works, work_topics, topics) row counts."""
    counts = {}
    for day in days:
        counts[day] = works, work_topics, topics = transform_day(s3, day)
        log.info("Silver %s: %d works, %d work_topics, %d topics", day, works, work_topics, topics)
    return counts


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    parser.add_argument("--stale", action="store_true", help="only days whose bronze is newer than silver")
    args = parser.parse_args(argv)

    s3 = s3_client()
    days = stale_days(s3) if args.stale else sorted(complete_days(s3, args.start, args.end))
    if not days:
        log.info("No bronze partitions to transform")
        return 0
    transform_days(s3, days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
