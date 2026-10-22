from datetime import date, datetime

from transform.silver import latest_versions, rebuild_abstract, short_id, to_tables


def raw_work(work_id="https://openalex.org/W1", updated="2026-10-01T10:00:00.000000", **overrides):
    work = {
        "id": work_id,
        "title": "A title",
        "abstract_inverted_index": {"Deep": [0], "learning": [1], "works": [2]},
        "publication_date": "2026-10-01",
        "publication_year": 2026,
        "type": "article",
        "language": "en",
        "cited_by_count": 3,
        "is_retracted": False,
        "updated_date": updated,
        "topics": [
            {"id": "https://openalex.org/T1", "score": 0.7},
            {"id": "https://openalex.org/T2", "score": 0.2},
            {"id": "https://openalex.org/T3", "score": 0.0},
        ],
    }
    work.update(overrides)
    return work


def test_short_id_strips_url():
    assert short_id("https://openalex.org/W123") == "W123"
    assert short_id(None) is None


def test_rebuild_abstract_orders_words_by_position():
    assert rebuild_abstract({"learning": [1, 3], "Deep": [0], "is": [2]}) == "Deep learning is learning"


def test_rebuild_abstract_is_none_when_missing():
    assert rebuild_abstract(None) is None
    assert rebuild_abstract({}) is None


def test_latest_versions_keeps_newest_per_work():
    old = raw_work(updated="2026-10-01T10:00:00", title="old")
    new = raw_work(updated="2026-10-05T10:00:00", title="new")

    assert [w["title"] for w in latest_versions([new, old])] == ["new"]


def test_to_tables_builds_works_row():
    works, _ = to_tables([raw_work()])

    row = works.row(0, named=True)
    assert row["work_id"] == "W1"
    assert row["abstract"] == "Deep learning works"
    assert row["publication_date"] == date(2026, 10, 1)
    assert row["updated_date"] == datetime(2026, 10, 1, 10, 0)
    assert "primary_topic_id" not in works.columns


def test_to_tables_splits_topics_with_rank():
    _, topics = to_tables([raw_work()])

    assert topics.select("work_id", "topic_id", "rank", "score").rows() == [
        ("W1", "T1", 1, 0.7),
        ("W1", "T2", 2, 0.2),
        ("W1", "T3", 3, 0.0),
    ]


def test_to_tables_keeps_work_without_abstract():
    works, topics = to_tables([raw_work(abstract_inverted_index=None)])

    assert works["abstract"].to_list() == [None]
    assert len(topics) == 3


def test_to_tables_uses_topics_of_the_latest_version_only():
    old = raw_work(updated="2026-10-01T10:00:00", topics=[{"id": "https://openalex.org/T9", "score": 1.0}])
    new = raw_work(updated="2026-10-05T10:00:00")

    works, topics = to_tables([old, new])

    assert len(works) == 1
    assert topics["topic_id"].to_list() == ["T1", "T2", "T3"]


def test_to_tables_empty_input_keeps_schema():
    works, topics = to_tables([])

    assert works.is_empty() and topics.is_empty()
    assert "work_id" in works.columns and "rank" in topics.columns
