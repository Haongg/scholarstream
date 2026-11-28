"""Read what gold holds, for run metrics and health checks (dbt writes one Parquet file per table)."""

import io
from datetime import date

import polars as pl
from botocore.exceptions import ClientError

GOLD_BUCKET = "gold"


def read_column(s3, table: str, column: str) -> pl.Series | None:
    """One column of a gold table, or None if the table does not exist yet."""
    try:
        body = s3.get_object(Bucket=GOLD_BUCKET, Key=f"{table}.parquet")["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return pl.read_parquet(io.BytesIO(body), columns=[column]).to_series()


def works_per_day(s3) -> dict[date, int]:
    """Works in gold per publication day."""
    days = read_column(s3, "dim_works", "publication_date")
    if days is None:
        return {}
    counts = days.value_counts(sort=False)
    return dict(zip(counts["publication_date"].to_list(), counts["count"].to_list(), strict=True))


def last_day(s3) -> date | None:
    """Latest publication day in gold (dim_date spans the first to the last day of dim_works)."""
    days = read_column(s3, "dim_date", "date_day")
    return None if days is None or days.is_empty() else days.max()
