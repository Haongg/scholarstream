"""Watermark: the last publication date fully loaded, stored in Postgres."""

import os
from datetime import date

import psycopg

SOURCE = "openalex_works_cs"

DDL = """
CREATE TABLE IF NOT EXISTS ingestion_watermark (
    source TEXT PRIMARY KEY,
    last_loaded_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        autocommit=True,
    )


class Watermark:
    def __init__(self, conn: psycopg.Connection, source: str = SOURCE):
        self.conn = conn
        self.source = source
        self.conn.execute(DDL)

    def get(self) -> date | None:
        row = self.conn.execute(
            "SELECT last_loaded_date FROM ingestion_watermark WHERE source = %s",
            (self.source,),
        ).fetchone()
        return row[0] if row else None

    def advance(self, day: date) -> None:
        """Move the watermark forward only, so a backfill of old dates never rewinds it."""
        self.conn.execute(
            """
            INSERT INTO ingestion_watermark (source, last_loaded_date)
            VALUES (%s, %s)
            ON CONFLICT (source) DO UPDATE
            SET last_loaded_date = GREATEST(ingestion_watermark.last_loaded_date, EXCLUDED.last_loaded_date),
                updated_at = now()
            """,
            (self.source, day),
        )
