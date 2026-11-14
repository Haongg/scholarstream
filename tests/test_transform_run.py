"""Tests for which bronze days transform.run reads: only marked days, and with --stale only
days whose bronze was reloaded after silver was built."""

import gzip
import io
import json
from datetime import UTC, date, datetime

import polars as pl
from botocore.exceptions import ClientError

from transform import run as transform_run

D1, D2 = date(2026, 10, 1), date(2026, 10, 2)
T0, T1, T2 = (datetime(2026, 11, 1, h, tzinfo=UTC) for h in (1, 2, 3))


def page_bytes(*ids) -> bytes:
    return gzip.compress(json.dumps({"meta": {"count": len(ids)}, "results": [{"id": i} for i in ids]}).encode())


def bronze(day: date, *ids, marked: datetime | None = T0) -> dict:
    """A bronze day with one page, plus its _SUCCESS marker written at `marked` (None = partial)."""
    prefix = f"works/publication_date={day.isoformat()}/"
    objects = {("bronze", prefix + "page-00001.json.gz"): (page_bytes(*ids), T0)}
    if marked:
        objects[("bronze", prefix + "_SUCCESS")] = (b"", marked)
    return objects


def silver(day: date, built: datetime) -> dict:
    return {("silver", f"work_topics/publication_date={day.isoformat()}/work_topics.parquet"): (b"", built)}


class FakeS3:
    """Objects as {(bucket, key): (bytes, last_modified)}; records keys written."""

    def __init__(self, objects: dict):
        self.objects = objects
        self.written = []

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix, Delimiter=None):
        keys = sorted(k for b, k in self.objects if b == Bucket and k.startswith(Prefix))
        if Delimiter:
            prefixes = sorted({Prefix + k[len(Prefix) :].split(Delimiter)[0] + Delimiter for k in keys})
            yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}
        else:
            yield {"Contents": [{"Key": k} for k in keys]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)][0])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": Key}}, "HeadObject")
        return {"LastModified": self.objects[(Bucket, Key)][1]}

    def put_object(self, Bucket, Key, Body):
        self.written.append(Key)


def run(monkeypatch, objects, argv=()):
    s3 = FakeS3(objects)
    monkeypatch.setattr(transform_run, "s3_client", lambda: s3)
    monkeypatch.setattr(transform_run, "load_dotenv", lambda: None)
    seen = []

    def fake_to_tables(works):
        seen.append([w["id"] for w in works])
        return pl.DataFrame(), pl.DataFrame()

    monkeypatch.setattr(transform_run, "to_tables", fake_to_tables)
    assert transform_run.main(list(argv)) == 0
    return s3, seen


def test_partial_day_is_skipped(monkeypatch):
    s3, seen = run(monkeypatch, {**bronze(D1, "W1"), **bronze(D2, "W2", marked=None)})

    assert seen == [["W1"]]  # the marker file itself is not parsed as a page
    assert all("2026-10-02" not in k for k in s3.written)


def test_nothing_complete_writes_nothing(monkeypatch):
    s3, seen = run(monkeypatch, bronze(D1, "W1", marked=None))

    assert seen == []
    assert s3.written == []


def test_stale_picks_days_without_silver_or_reloaded_since(monkeypatch):
    objects = {
        **bronze(D1, "W1", marked=T2), **silver(D1, built=T1),  # reloaded after silver was built
        **bronze(D2, "W2", marked=T0), **silver(D2, built=T1),  # silver is up to date
        **bronze(date(2026, 10, 3), "W3"),                       # never transformed
    }
    _, seen = run(monkeypatch, objects, ["--stale"])

    assert seen == [["W1"], ["W3"]]


def test_stale_ignores_partial_days(monkeypatch):
    _, seen = run(monkeypatch, bronze(D1, "W1", marked=None), ["--stale"])

    assert seen == []
