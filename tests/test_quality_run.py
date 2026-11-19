"""Tests for quality.run against an in-memory fake of the S3 client."""

import io
import logging
from datetime import UTC, date, datetime, timedelta

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


def topic_dim_bytes() -> bytes:
    names = ("topic_name", "subfield_id", "subfield_name", "field_id", "field_name", "domain_id", "domain_name")
    return parquet_bytes(pl.DataFrame({"topic_id": ["T1"], **{c: ["x"] for c in names}}))


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

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": Key}}, "HeadObject")
        return {"LastModified": datetime(2026, 11, 1, tzinfo=UTC)}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body


def key(table: str, day: date) -> str:
    return f"{table}/publication_date={day.isoformat()}/{table}.parquet"


def good_day(day: date, n: int = 2) -> dict[str, bytes]:
    return {
        key("works", day): works_bytes(day, n),
        key("work_topics", day): topics_bytes(n),
        key("topics", day): topic_dim_bytes(),
    }


def weekly_history(day: date, n: int, weeks: int = 4) -> dict[str, bytes]:
    """Good silver of `n` works on the same weekday in each of the previous `weeks` weeks."""
    history = {}
    for w in range(1, weeks + 1):
        history.update(good_day(day - timedelta(weeks=w), n))
    return history


def marker(day: date) -> str:
    return f"works/publication_date={day.isoformat()}/_QUALITY_OK"


D1, D2, D3 = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)


TODAY = datetime.now(UTC).date()  # the gate judges a day's age against the real clock


@pytest.fixture
def use_s3(monkeypatch):
    monkeypatch.setenv("INGEST_LOOKBACK_DAYS", "7")

    def install(objects):
        monkeypatch.setattr(quality_run, "s3_client", lambda: FakeS3(objects))
        monkeypatch.setattr(quality_run, "load_dotenv", lambda: None)
        return objects  # the fake writes markers into this dict

    return install


def test_only_passing_days_get_a_marker(use_s3):
    objects = {**good_day(D1), **good_day(D2)}
    del objects[key("work_topics", D2)]
    use_s3(objects)

    assert quality_run.main([]) == 1

    assert marker(D1) in objects
    assert marker(D2) not in objects


def test_failed_day_is_checked_again_on_the_next_run(use_s3, caplog):
    # The bug this guards against: a day that failed was never re-checked once it was no
    # longer freshly transformed, and the next run promoted silver anyway.
    objects = {**good_day(D1), **good_day(D2)}
    del objects[key("work_topics", D2)]
    use_s3(objects)
    quality_run.main(["--unchecked"])
    caplog.clear()

    with caplog.at_level(logging.INFO):
        assert quality_run.main(["--unchecked"]) == 1

    assert "== 2026-10-02" in caplog.text
    assert "== 2026-10-01" not in caplog.text  # passed last time, not checked again


def test_unchecked_with_everything_passed_checks_nothing(use_s3, caplog):
    objects = use_s3({**good_day(D1), marker(D1): b""})

    with caplog.at_level(logging.INFO):
        assert quality_run.main(["--unchecked"]) == 0

    assert "No silver partitions in range" in caplog.text
    assert objects[marker(D1)] == b""


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


def test_missing_topics_table_fails_without_crashing(use_s3, caplog):
    # e.g. a day built before the topics table existed and never rebuilt
    objects = {**good_day(D1), **good_day(D2)}
    del objects[key("topics", D2)]
    use_s3(objects)

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert "- topics: NoSuchKey" in caplog.text
    assert marker(D1) in objects and marker(D2) not in objects


def test_lookback_window_ends_on_the_last_day_ingestion_refetches():
    assert quality_run.is_settling(TODAY - timedelta(days=7), TODAY, lookback=7)  # loaded one last time
    assert not quality_run.is_settling(TODAY - timedelta(days=8), TODAY, lookback=7)
    assert not quality_run.is_settling(TODAY - timedelta(days=1), TODAY, lookback=0)


def test_young_short_day_is_deferred_not_failed(use_s3, caplog):
    # e.g. 2026-11-18 loaded on 11-19: OpenAlex had indexed 170 of its ~2,800 works
    young = TODAY - timedelta(days=1)
    objects = use_s3({**weekly_history(young, 20), **good_day(young, 2)})

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 0

    assert marker(young) not in objects  # not in gold yet; re-checked next run
    assert marker(young - timedelta(weeks=1)) in objects
    assert "DEFER row count vs same weekday" in caplog.text
    assert "FAILED" not in caplog.text


def test_short_day_outside_the_lookback_window_fails(use_s3, caplog):
    # no longer re-fetched, so a missing share of works will not arrive any more
    old = TODAY - timedelta(days=8)
    objects = use_s3({**weekly_history(old, 20), **good_day(old, 2)})

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 1

    assert marker(old) not in objects
    assert "FAIL  row count vs same weekday" in caplog.text


def test_young_day_with_another_failure_is_not_deferred(use_s3):
    young = TODAY - timedelta(days=1)
    day = good_day(young, 2)
    day[key("works", young)] = works_bytes(young - timedelta(days=1), 2)  # partition mismatch
    objects = use_s3({**weekly_history(young, 20), **day})

    assert quality_run.main([]) == 1
    assert marker(young) not in objects


def test_day_far_above_usual_only_warns_and_passes(use_s3, caplog):
    # e.g. 2026-11-15: a bulk upload made the day 2.5x its usual size; gold filters it
    old = TODAY - timedelta(days=10)
    objects = use_s3({**weekly_history(old, 4), **good_day(old, 20)})

    with caplog.at_level(logging.INFO):
        assert quality_run.main([]) == 0

    assert marker(old) in objects
    assert "WARN  row count far above same weekday" in caplog.text


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
