"""Pure bronze -> silver transformation: raw OpenAlex works into `works`, `work_topics` and
`topics` tables."""

from collections.abc import Iterable
from datetime import date, datetime

import polars as pl

# Every silver table of a day, in the order transform writes them. work_topics comes last:
# its write time is when the day's silver was finished (see transform.run.stale_days).
SILVER_TABLES = (
    "works",
    "topics",
    "authors",
    "institutions",
    "work_authorships",
    "authorship_institutions",
    "work_topics",
)

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

# The topic hierarchy (topic -> subfield -> field -> domain) as seen on the day's works.
TOPICS_SCHEMA = {
    "topic_id": pl.String,
    "topic_name": pl.String,
    "subfield_id": pl.String,
    "subfield_name": pl.String,
    "field_id": pl.String,
    "field_name": pl.String,
    "domain_id": pl.String,
    "domain_name": pl.String,
}


# One row per author slot of a work, in byline order. author_order (1-based) is the key, not
# author_id: about 0.7% of slots have no author id, and the same author can fill two slots of
# one work (measured 2026-11-28). raw_affiliation_strings is kept for later matching: a third
# of slots have no institution, some of them with an affiliation string OpenAlex did not match.
WORK_AUTHORSHIPS_SCHEMA = {
    "work_id": pl.String,
    "author_order": pl.Int64,
    "author_id": pl.String,
    "author_position": pl.String,  # first / middle / last
    "is_corresponding": pl.Boolean,
    "raw_author_name": pl.String,
    "raw_affiliation_strings": pl.List(pl.String),
}

# An author slot can list several institutions.
AUTHORSHIP_INSTITUTIONS_SCHEMA = {
    "work_id": pl.String,
    "author_order": pl.Int64,
    "institution_id": pl.String,
}

AUTHORS_SCHEMA = {
    "author_id": pl.String,
    "author_name": pl.String,
    "orcid": pl.String,
}

INSTITUTIONS_SCHEMA = {
    "institution_id": pl.String,
    "institution_name": pl.String,
    "ror": pl.String,
    "country_code": pl.String,
    "institution_type": pl.String,
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


def topic_row(topic: dict) -> dict:
    """Flatten a raw topic and its hierarchy: 'https://openalex.org/subfields/1707' -> '1707'."""
    row = {"topic_id": short_id(topic.get("id")), "topic_name": topic.get("display_name")}
    for level in ("subfield", "field", "domain"):
        parent = topic.get(level) or {}
        row[f"{level}_id"] = short_id(parent.get("id"))
        row[f"{level}_name"] = parent.get("display_name")
    return row


def author_row(author: dict) -> dict:
    return {
        "author_id": short_id(author.get("id")),
        "author_name": author.get("display_name"),
        "orcid": short_id(author.get("orcid")),
    }


def institution_row(institution: dict) -> dict:
    return {
        "institution_id": short_id(institution.get("id")),
        "institution_name": institution.get("display_name"),
        "ror": short_id(institution.get("ror")),
        "country_code": institution.get("country_code"),
        "institution_type": institution.get("type"),
    }


def authorship_rows(work_id: str, authorships: list[dict] | None) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Flatten a work's authorships into (slots, slot institutions, authors, institutions) rows.

    A slot without an author id stays (it still counts as an author of the work) but adds no
    author row."""
    slots, slot_institutions, authors, institutions = [], [], [], []
    for order, authorship in enumerate(authorships or [], start=1):
        author = authorship.get("author") or {}
        slots.append(
            {
                "work_id": work_id,
                "author_order": order,
                "author_id": short_id(author.get("id")),
                "author_position": authorship.get("author_position"),
                "is_corresponding": authorship.get("is_corresponding"),
                "raw_author_name": authorship.get("raw_author_name"),
                "raw_affiliation_strings": authorship.get("raw_affiliation_strings") or [],
            }
        )
        if author.get("id"):
            authors.append(author_row(author))
        for institution in authorship.get("institutions") or []:
            slot_institutions.append(
                {"work_id": work_id, "author_order": order, "institution_id": short_id(institution.get("id"))}
            )
            institutions.append(institution_row(institution))
    return slots, slot_institutions, authors, institutions


def to_tables(works: Iterable[dict]) -> dict[str, pl.DataFrame]:
    """Deduplicate raw works and split them into the silver tables, keyed and ordered as
    SILVER_TABLES.

    The dimension tables (`topics`, `authors`, `institutions`) hold one row per distinct
    entity of the day. Only exact repeats are dropped: a topic seen with two different
    hierarchies (or an author with two names) keeps both rows, so the quality gate flags it
    instead of one being picked silently.
    """
    work_rows, topic_rows, topic_dim_rows = [], [], []
    slot_rows, slot_institution_rows, author_rows, institution_rows = [], [], [], []
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
            topic_dim_rows.append(topic_row(topic))
        slots, slot_institutions, authors, institutions = authorship_rows(work_id, work.get("authorships"))
        slot_rows += slots
        slot_institution_rows += slot_institutions
        author_rows += authors
        institution_rows += institutions
    tables = {
        "works": pl.DataFrame(work_rows, schema=WORKS_SCHEMA),
        "topics": pl.DataFrame(topic_dim_rows, schema=TOPICS_SCHEMA).unique().sort("topic_id"),
        "work_topics": pl.DataFrame(topic_rows, schema=WORK_TOPICS_SCHEMA),
        "work_authorships": pl.DataFrame(slot_rows, schema=WORK_AUTHORSHIPS_SCHEMA),
        "authorship_institutions": pl.DataFrame(slot_institution_rows, schema=AUTHORSHIP_INSTITUTIONS_SCHEMA)
        .unique()
        .sort("work_id", "author_order", "institution_id"),
        "authors": pl.DataFrame(author_rows, schema=AUTHORS_SCHEMA).unique().sort("author_id"),
        "institutions": pl.DataFrame(institution_rows, schema=INSTITUTIONS_SCHEMA).unique().sort("institution_id"),
    }
    return {name: tables[name] for name in SILVER_TABLES}
