"""Pure bronze -> silver transformation: raw OpenAlex works into `works` and `work_topics` tables."""

from collections.abc import Iterable
from datetime import date, datetime

import polars as pl

WORKS_SCHEMA = {
    "work_id": pl.String,
    "title": pl.String,
    "abstract": pl.String,
    "publication_date": pl.Date,
    "publication_year": pl.Int64,
    "type": pl.String,
    "language": pl.String,
    "cited_by_count": pl.Int64,
    "is_retracted": pl.Boolean,
    "updated_date": pl.Datetime("us"),
}

WORK_TOPICS_SCHEMA = {
    "work_id": pl.String,
    "topic_id": pl.String,
    "rank": pl.Int64,
    "score": pl.Float64,
}


def short_id(url: str | None) -> str | None:
    """'https://openalex.org/W123' -> 'W123'."""
    return url.rsplit("/", 1)[-1] if url else None


def rebuild_abstract(inverted_index: dict[str, list[int]] | None) -> str | None:
    """OpenAlex ships abstracts as {word: [positions]}; put each word back in place.

    Returns None (not "") when the work has no abstract, so "missing" stays distinguishable.
    """
    if not inverted_index:
        return None
    words = {pos: word for word, positions in inverted_index.items() for pos in positions}
    return " ".join(words[pos] for pos in sorted(words))


def latest_versions(works: Iterable[dict]) -> list[dict]:
    """Keep one record per work id: the one with the newest `updated_date`."""
    latest: dict[str, dict] = {}
    for work in works:
        current = latest.get(work["id"])
        if current is None or work["updated_date"] > current["updated_date"]:
            latest[work["id"]] = work
    return list(latest.values())


def to_tables(works: Iterable[dict]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Deduplicate raw works and split them into the `works` and `work_topics` tables."""
    work_rows, topic_rows = [], []
    for work in latest_versions(works):
        work_id = short_id(work["id"])
        work_rows.append(
            {
                "work_id": work_id,
                "title": work.get("title"),
                "abstract": rebuild_abstract(work.get("abstract_inverted_index")),
                "publication_date": date.fromisoformat(work["publication_date"]),
                "publication_year": work.get("publication_year"),
                "type": work.get("type"),
                "language": work.get("language"),
                "cited_by_count": work.get("cited_by_count"),
                "is_retracted": work.get("is_retracted"),
                "updated_date": datetime.fromisoformat(work["updated_date"]),
            }
        )
        for rank, topic in enumerate(work.get("topics") or [], start=1):
            topic_rows.append(
                {
                    "work_id": work_id,
                    "topic_id": short_id(topic["id"]),
                    "rank": rank,
                    "score": topic.get("score"),
                }
            )
    return (
        pl.DataFrame(work_rows, schema=WORKS_SCHEMA),
        pl.DataFrame(topic_rows, schema=WORK_TOPICS_SCHEMA),
    )
