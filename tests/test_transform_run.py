"""Tests that transform.run only reads bronze days carrying the _SUCCESS marker."""

import gzip
import io
import json
from datetime import date

import polars as pl
from botocore.exceptions import ClientError

from transform import run as transform_run

D1, D2 = date(2026, 10, 1), date(2026, 10, 2)


def prefix(day: date) -> str:
    return f"works/publication_date={day.isoformat()}/"


def page_bytes(*ids) -> bytes:
    return gzip.compress(json.dumps({"meta": {"count": len(ids)}, "results": [{"id": i} for i in ids]}).encode())


class FakeS3:
    """Holds bronze objects as {key: bytes} and records silver writes."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.written = []

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix, Delimiter=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        if Delimiter:
            prefixes = sorted({Prefix + k[len(Prefix) :].split(Delimiter)[0] + Delimiter for k in keys})
            yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}
        else:
            yield {"Contents": [{"Key": k} for k in keys]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": Key}}, "HeadObject")
        return {}

    def put_object(self, Bucket, Key, Body):
        self.written.append(Key)


def run(monkeypatch, objects):
    s3 = FakeS3(objects)
    monkeypatch.setattr(transform_run, "s3_client", lambda: s3)
    monkeypatch.setattr(transform_run, "load_dotenv", lambda: None)
    seen = []
    def fake_to_tables(works):
        seen.append(works)
        return pl.DataFrame(), pl.DataFrame()

    monkeypatch.setattr(transform_run, "to_tables", fake_to_tables)
    assert transform_run.main([]) == 0
    return s3, seen


def test_partial_day_is_skipped(monkeypatch):
    objects = {
        prefix(D1) + "page-00001.json.gz": page_bytes("W1"),
        prefix(D1) + "_SUCCESS": b"",
        prefix(D2) + "page-00001.json.gz": page_bytes("W2"),  # no marker: ingestion stopped mid-day
    }
    s3, seen = run(monkeypatch, objects)

    assert seen == [[{"id": "W1"}]]  # the marker file itself is not parsed as a page
    assert all("2026-10-02" not in k for k in s3.written)


def test_nothing_complete_writes_nothing(monkeypatch):
    s3, seen = run(monkeypatch, {prefix(D1) + "page-00001.json.gz": page_bytes("W1")})

    assert seen == []
    assert s3.written == []
