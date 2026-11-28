from datetime import date, datetime

from transform.silver import (
    SILVER_TABLES,
    latest_versions,
    rebuild_abstract,
    short_id,
    to_tables,
)


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
    works = to_tables([raw_work()])["works"]

    row = works.row(0, named=True)
    assert row["work_id"] == "W1"
    assert row["abstract"] == "Deep learning works"
    assert row["publication_date"] == date(2026, 10, 1)
    assert row["updated_date"] == datetime(2026, 10, 1, 10, 0)
    assert "primary_topic_id" not in works.columns


def test_to_tables_splits_topics_with_rank():
    topics = to_tables([raw_work()])["work_topics"]

    assert topics.select("work_id", "topic_id", "rank", "score").rows() == [
        ("W1", "T1", 1, 0.7),
        ("W1", "T2", 2, 0.2),
        ("W1", "T3", 3, 0.0),
    ]


def test_to_tables_keeps_work_without_abstract():
    tables = to_tables([raw_work(abstract_inverted_index=None)])
    works, topics = tables["works"], tables["work_topics"]

    assert works["abstract"].to_list() == [None]
    assert len(topics) == 3


def test_to_tables_uses_topics_of_the_latest_version_only():
    old = raw_work(updated="2026-10-01T10:00:00", topics=[raw_topic("T9", 1.0)])
    new = raw_work(updated="2026-10-05T10:00:00")

    tables = to_tables([old, new])
    works, topics, topic_dim = tables["works"], tables["work_topics"], tables["topics"]

    assert len(works) == 1
    assert topics["topic_id"].to_list() == ["T1", "T2", "T3"]
    assert topic_dim["topic_id"].to_list() == ["T1", "T2", "T3"]  # T9 only on the stale version


def test_to_tables_empty_input_keeps_schema():
    tables = to_tables([])
    works, topics, topic_dim = tables["works"], tables["work_topics"], tables["topics"]

    assert works.is_empty() and topics.is_empty() and topic_dim.is_empty()
    assert "work_id" in works.columns and "rank" in topics.columns and "domain_name" in topic_dim.columns


def test_topics_table_flattens_hierarchy_to_short_ids():
    topic_dim = to_tables([raw_work()])["topics"]

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
    topic_dim = to_tables([raw_work("https://openalex.org/W1"), raw_work("https://openalex.org/W2")])["topics"]

    assert topic_dim["topic_id"].to_list() == ["T1", "T2", "T3"]


def test_topics_table_keeps_conflicting_hierarchies_for_the_gate():
    w1 = raw_work("https://openalex.org/W1", topics=[raw_topic("T1", 0.9)])
    w2 = raw_work("https://openalex.org/W2", topics=[raw_topic("T1", 0.9, subfield=("1702", "Artificial Intelligence"))])

    topic_dim = to_tables([w1, w2])["topics"]

    assert sorted(topic_dim["subfield_id"].to_list()) == ["1702", "1707"]


def test_topics_table_tolerates_missing_hierarchy():
    topic_dim = to_tables([raw_work(topics=[{"id": "https://openalex.org/T1", "score": 0.5}])])["topics"]

    row = topic_dim.row(0, named=True)
    assert row["topic_id"] == "T1" and row["subfield_id"] is None and row["domain_name"] is None


def raw_institution(inst_id="I1", name="Google (United States)", country="US") -> dict:
    return {
        "id": f"https://openalex.org/{inst_id}",
        "display_name": name,
        "ror": "https://ror.org/00njsd438",
        "country_code": country,
        "type": "company",
        "lineage": [f"https://openalex.org/{inst_id}"],
    }


def raw_authorship(author_id="A1", position="first", institutions=(), name=None, **overrides) -> dict:
    authorship = {
        "author_position": position,
        "author": {
            "id": f"https://openalex.org/{author_id}" if author_id else None,
            "display_name": name or f"Author {author_id}",
            "orcid": "https://orcid.org/0000-0001-2345-6789" if author_id else None,
        },
        "institutions": list(institutions),
        "is_corresponding": position == "first",
        "raw_author_name": f"raw {author_id}",
        "raw_affiliation_strings": ["Google, Mountain View"] if institutions else [],
    }
    authorship.update(overrides)
    return authorship


def test_work_authorships_keep_byline_order_and_flags():
    work = raw_work(authorships=[raw_authorship("A1", "first", [raw_institution()]), raw_authorship("A2", "last")])

    slots = to_tables([work])["work_authorships"]

    assert slots.rows(named=True) == [
        {
            "work_id": "W1", "author_order": 1, "author_id": "A1", "author_position": "first",
            "is_corresponding": True, "raw_author_name": "raw A1",
            "raw_affiliation_strings": ["Google, Mountain View"],
        },
        {
            "work_id": "W1", "author_order": 2, "author_id": "A2", "author_position": "last",
            "is_corresponding": False, "raw_author_name": "raw A2", "raw_affiliation_strings": [],
        },
    ]


def test_slot_without_author_id_is_kept_but_not_described():
    work = raw_work(authorships=[raw_authorship("A1"), raw_authorship(None, "last")])

    tables = to_tables([work])

    assert tables["work_authorships"]["author_id"].to_list() == ["A1", None]
    assert tables["authors"]["author_id"].to_list() == ["A1"]


def test_same_author_in_two_slots_keeps_both_slots_one_author_row():
    work = raw_work(authorships=[raw_authorship("A1", "middle"), raw_authorship("A1", "middle")])

    tables = to_tables([work])

    assert tables["work_authorships"]["author_order"].to_list() == [1, 2]
    assert tables["authors"].height == 1


def test_slot_with_several_institutions_links_each_once():
    google, mit = raw_institution("I1"), raw_institution("I2", "MIT")
    work = raw_work(authorships=[raw_authorship("A1", institutions=[google, mit, google])])

    tables = to_tables([work])

    assert tables["authorship_institutions"].rows() == [("W1", 1, "I1"), ("W1", 1, "I2")]
    assert tables["institutions"].row(0, named=True) == {
        "institution_id": "I1",
        "institution_name": "Google (United States)",
        "ror": "00njsd438",
        "country_code": "US",
        "institution_type": "company",
    }
    assert tables["institutions"].height == 2


def test_authors_shared_by_works_are_one_row_with_short_orcid():
    w1 = raw_work("https://openalex.org/W1", authorships=[raw_authorship("A1")])
    w2 = raw_work("https://openalex.org/W2", authorships=[raw_authorship("A1")])

    authors = to_tables([w1, w2])["authors"]

    assert authors.rows() == [("A1", "Author A1", "0000-0001-2345-6789")]


def test_author_with_two_names_keeps_both_rows_for_the_gate():
    w1 = raw_work("https://openalex.org/W1", authorships=[raw_authorship("A1", name="Ann Lee")])
    w2 = raw_work("https://openalex.org/W2", authorships=[raw_authorship("A1", name="A. Lee")])

    assert sorted(to_tables([w1, w2])["authors"]["author_name"].to_list()) == ["A. Lee", "Ann Lee"]


def test_work_without_authorships_has_no_slots():
    tables = to_tables([raw_work(authorships=None), raw_work("https://openalex.org/W2", authorships=[])])

    assert tables["works"].height == 2
    assert tables["work_authorships"].is_empty()
    assert "raw_affiliation_strings" in tables["work_authorships"].columns


def test_authorships_come_from_the_latest_version_only():
    old = raw_work(updated="2026-10-01T10:00:00", authorships=[raw_authorship("A9", institutions=[raw_institution("I9")])])
    new = raw_work(updated="2026-10-05T10:00:00", authorships=[raw_authorship("A1")])

    tables = to_tables([old, new])

    assert tables["work_authorships"]["author_id"].to_list() == ["A1"]
    assert tables["authors"]["author_id"].to_list() == ["A1"]
    assert tables["institutions"].is_empty() and tables["authorship_institutions"].is_empty()


def test_silver_tables_end_with_work_topics():
    # transform writes in this order; stale_days reads work_topics' timestamp as "finished".
    assert list(to_tables([raw_work()])) == list(SILVER_TABLES)
    assert SILVER_TABLES[-1] == "work_topics"
