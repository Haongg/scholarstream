"""Tests for quality.run against an in-memory fake of the S3 client."""

import io
import logging
from datetime import date

import polars as pl
import pytest
from botocore.exceptions import ClientError

from quality import run as quality_run


def parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def works_bytes(day: date, n: int = 2) -> bytes:
    return parquet_bytes(
        pl.DataFrame(
            {"work_id": [f"W{i}" for i in range(n)], "publication_date": [day] * n},
            schema={"work_id": pl.String, "publication_date": pl.Date},
        )
    )


def topics_bytes(n: int = 2) -> bytes:
    return parquet_bytes(
        pl.DataFrame(
            {"work_id": [f"W{i}" for i in range(n)], "topic_id": ["T1"] * n, "rank": [1] * n},
            schema={"work_id": pl.String, "topic_id": pl.String, "rank": pl.Int64},
        )
    )


class FakeS3:
    """Holds silver objects as {key: bytes}; mimics the calls quality.run makes."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix, Delimiter):
        prefixes = sorted({k[: k.rindex("/") + 1] for k in self.objects if k.startswith(Prefix)})
        yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": Key}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}


def key(table: str, day: date) -> str:
    return f"{table}/publication_date={day.isoformat()}/{table}.parquet"


def good_day(day: date) -> dict[str, bytes]:
    return {key("works", day): works_bytes(day), key("work_topics", day): topics_bytes()}


D1, D2, D3 = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)


@pytest.fixture
def use_s3(monkeypatch):
    def install(objects):
        monkeypatch.setattr(quality_run, "s3_client", lambda: FakeS3(objects))
        monkeypatch.setattr(quality_run, "load_dotenv", lambda: None)

    return install


def test_good_silver_passes(use_s3):
    use_s3({**good_day(D1), **good_day(D2)})

    assert quality_run.main([]) == 0


def test_missing_work_topics_fails_without_crashing(use_s3, caplog):
    objects = {**good_day(D1), **good_day(D2)}
    del objects[key("work_topics", D2)]
    use_s3(objects)

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert "work_topics: NoSuchKey" in caplog.text


def test_missing_works_fails_and_other_days_are_still_checked(use_s3, caplog):
    # The day prefix is listed (a stray file sits there) but works.parquet itself is gone.
    objects = {**good_day(D1), **good_day(D2), **good_day(D3)}
    objects["works/publication_date=2026-10-02/_tmp"] = b""
    del objects[key("works", D2)]
    use_s3(objects)

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert "works: NoSuchKey" in caplog.text
    assert "== 2026-10-03" in caplog.text  # a broken day does not stop the rest


def test_day_with_only_work_topics_is_still_found_and_fails(use_s3, caplog):
    # The whole works/ folder for D2 is gone; only work_topics/ remains.
    objects = {**good_day(D1), **good_day(D2)}
    del objects[key("works", D2)]
    use_s3(objects)

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert "== 2026-10-02" in caplog.text
    assert "works: NoSuchKey" in caplog.text


def test_corrupt_parquet_fails_without_crashing(use_s3, caplog):
    objects = good_day(D1)
    objects[key("works", D1)] = b"not a parquet file"
    use_s3(objects)

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert "works: unreadable parquet" in caplog.text
