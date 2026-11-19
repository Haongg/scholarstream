from datetime import date, datetime

from transform.silver import latest_versions, rebuild_abstract, short_id, to_tables


def raw_topic(topic_id: str, score: float, subfield=("1707", "Computer Vision and Pattern Recognition")) -> dict:
    """A raw OpenAlex topic with its subfield -> field -> domain hierarchy."""
    return {
        "id": f"https://openalex.org/{topic_id}",
        "display_name": f"Topic {topic_id}",
        "score": score,
        "subfield": {"id": f"https://openalex.org/subfields/{subfield[0]}", "display_name": subfield[1]},
        "field": {"id": "https://openalex.org/fields/17", "display_name": "Computer Science"},
        "domain": {"id": "https://openalex.org/domains/3", "display_name": "Physical Sciences"},
    }


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
        "topics": [raw_topic("T1", 0.7), raw_topic("T2", 0.2), raw_topic("T3", 0.0)],
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
    works, _, _ = to_tables([raw_work()])

    row = works.row(0, named=True)
    assert row["work_id"] == "W1"
    assert row["abstract"] == "Deep learning works"
    assert row["publication_date"] == date(2026, 10, 1)
    assert row["updated_date"] == datetime(2026, 10, 1, 10, 0)
    assert "primary_topic_id" not in works.columns


def test_to_tables_splits_topics_with_rank():
    _, topics, _ = to_tables([raw_work()])

    assert topics.select("work_id", "topic_id", "rank", "score").rows() == [
        ("W1", "T1", 1, 0.7),
        ("W1", "T2", 2, 0.2),
        ("W1", "T3", 3, 0.0),
    ]


def test_to_tables_keeps_work_without_abstract():
    works, topics, _ = to_tables([raw_work(abstract_inverted_index=None)])

    assert works["abstract"].to_list() == [None]
    assert len(topics) == 3


def test_to_tables_uses_topics_of_the_latest_version_only():
    old = raw_work(updated="2026-10-01T10:00:00", topics=[raw_topic("T9", 1.0)])
    new = raw_work(updated="2026-10-05T10:00:00")

    works, topics, topic_dim = to_tables([old, new])

    assert len(works) == 1
    assert topics["topic_id"].to_list() == ["T1", "T2", "T3"]
    assert topic_dim["topic_id"].to_list() == ["T1", "T2", "T3"]  # T9 only on the stale version


def test_to_tables_empty_input_keeps_schema():
    works, topics, topic_dim = to_tables([])

    assert works.is_empty() and topics.is_empty() and topic_dim.is_empty()
    assert "work_id" in works.columns and "rank" in topics.columns and "domain_name" in topic_dim.columns


def test_topics_table_flattens_hierarchy_to_short_ids():
    _, _, topic_dim = to_tables([raw_work()])

    assert topic_dim.row(0, named=True) == {
        "topic_id": "T1",
        "topic_name": "Topic T1",
        "subfield_id": "1707",
        "subfield_name": "Computer Vision and Pattern Recognition",
        "field_id": "17",
        "field_name": "Computer Science",
        "domain_id": "3",
        "domain_name": "Physical Sciences",
    }


def test_topics_table_has_one_row_per_topic_shared_by_works():
    _, _, topic_dim = to_tables([raw_work("https://openalex.org/W1"), raw_work("https://openalex.org/W2")])

    assert topic_dim["topic_id"].to_list() == ["T1", "T2", "T3"]


def test_topics_table_keeps_conflicting_hierarchies_for_the_gate():
    w1 = raw_work("https://openalex.org/W1", topics=[raw_topic("T1", 0.9)])
    w2 = raw_work("https://openalex.org/W2", topics=[raw_topic("T1", 0.9, subfield=("1702", "Artificial Intelligence"))])

    _, _, topic_dim = to_tables([w1, w2])

    assert sorted(topic_dim["subfield_id"].to_list()) == ["1702", "1707"]


def test_topics_table_tolerates_missing_hierarchy():
    _, _, topic_dim = to_tables([raw_work(topics=[{"id": "https://openalex.org/T1", "score": 0.5}])])

    row = topic_dim.row(0, named=True)
    assert row["topic_id"] == "T1" and row["subfield_id"] is None and row["domain_name"] is None
