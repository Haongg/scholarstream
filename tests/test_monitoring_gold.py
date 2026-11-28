"""Tests for monitoring.gold against an in-memory fake of the gold bucket."""

import io
from datetime import date

import polars as pl
import pytest
from botocore.exceptions import ClientError

from monitoring import gold

D1, D2 = date(2026, 11, 25), date(2026, 11, 26)


def parquet(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


class FakeS3:
    def __init__(self, objects, error_code="NoSuchKey"):
        self.objects = objects
        self.error_code = error_code

    def get_object(self, Bucket, Key):
        assert Bucket == "gold"
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": self.error_code, "Message": Key}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}


def test_works_per_day_counts_dim_works_by_publication_date():
    dim_works = pl.DataFrame({"work_id": ["W1", "W2", "W3"], "publication_date": [D1, D2, D2]})
    s3 = FakeS3({"dim_works.parquet": parquet(dim_works)})

    assert gold.works_per_day(s3) == {D1: 1, D2: 2}


def test_last_day_is_the_end_of_dim_date():
    s3 = FakeS3({"dim_date.parquet": parquet(pl.DataFrame({"date_day": [D1, D2], "year": [2026, 2026]}))})

    assert gold.last_day(s3) == D2


def test_missing_gold_reads_as_empty():
    s3 = FakeS3({})

    assert gold.works_per_day(s3) == {}
    assert gold.last_day(s3) is None


def test_other_s3_errors_are_raised():
    with pytest.raises(ClientError):
        gold.last_day(FakeS3({}, error_code="AccessDenied"))
