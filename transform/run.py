"""Transform bronze pages into silver Parquet tables, one publication day at a time.

    python -m transform.run                                  # every day found in bronze
    python -m transform.run --from 2026-10-01 --to 2026-10-03

Each day's silver files are overwritten in place, so re-running a day never duplicates rows.
"""

import argparse
import gzip
import io
import json
import logging
import sys
from datetime import date

import polars as pl
from dotenv import load_dotenv

from ingestion.bronze_writer import BRONZE_BUCKET, s3_client
from transform.silver import to_tables

SILVER_BUCKET = "silver"
PARTITION = "publication_date="

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
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read()
            works.extend(json.loads(gzip.decompress(body))["results"])
    return works


def write_table(s3, table: str, day: date, df: pl.DataFrame) -> None:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    key = f"{table}/{PARTITION}{day.isoformat()}/{table}.parquet"
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buffer.getvalue())


def transform_day(s3, day: date) -> tuple[int, int]:
    works, work_topics = to_tables(read_day(s3, day))
    write_table(s3, "works", day, works)
    write_table(s3, "work_topics", day, work_topics)
    return len(works), len(work_topics)


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    args = parser.parse_args(argv)

    s3 = s3_client()
    days = [d for d in bronze_days(s3) if (not args.start or d >= args.start) and (not args.end or d <= args.end)]
    if not days:
        log.info("No bronze partitions in range")
        return 0
    for day in days:
        works, topics = transform_day(s3, day)
        log.info("Silver %s: %d works, %d work_topics", day, works, topics)
    return 0


if __name__ == "__main__":
    sys.exit(main())
