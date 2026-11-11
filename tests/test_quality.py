from datetime import date

import polars as pl

from quality.checks import (
    check_publication_date,
    check_row_count,
    check_topics_reference_works,
    check_topics_shape,
    check_work_id,
    run_checks,
)

DAY = date(2026, 10, 1)
TODAY = date(2026, 10, 20)


def works_df(ids=("W1", "W2"), published=DAY):
    return pl.DataFrame(
        {"work_id": list(ids), "publication_date": [published] * len(ids)},
        schema={"work_id": pl.String, "publication_date": pl.Date},
    )


def topics_df(rows):
    return pl.DataFrame(rows, schema={"work_id": pl.String, "topic_id": pl.String, "rank": pl.Int64}, orient="row")


GOOD_TOPICS = [("W1", "T1", 1), ("W1", "T2", 2), ("W2", "T1", 1)]


def test_work_id_passes_when_unique():
    assert check_work_id(works_df()) == []


def test_work_id_flags_duplicates_and_nulls():
    works = pl.DataFrame({"work_id": ["W1", "W1", None]}, schema={"work_id": pl.String})

    problems = check_work_id(works)

    assert any("null" in p for p in problems)
    assert any("share a work_id" in p for p in problems)


def test_topics_must_reference_existing_works():
    topics = topics_df(GOOD_TOPICS + [("W404", "T1", 1)])

    assert check_topics_reference_works(works_df(), topics_df(GOOD_TOPICS)) == []
    assert "1 work_topics rows point" in check_topics_reference_works(works_df(), topics)[0]


def test_topics_shape_passes_on_good_data():
    assert check_topics_shape(works_df(), topics_df(GOOD_TOPICS)) == []


def test_topics_shape_flags_duplicate_pair_bad_rank_and_missing_topics():
    topics = topics_df([("W1", "T1", 1), ("W1", "T1", 2), ("W1", "T2", 9)])

    problems = check_topics_shape(works_df(), topics)

    assert any("repeat" in p for p in problems)
    assert any("rank outside" in p for p in problems)
    assert any("have no topic" in p for p in problems)  # W2 has none


def test_publication_date_passes_for_partition_day():
    assert check_publication_date(works_df(), DAY, TODAY) == []


def test_publication_date_flags_wrong_day_and_future():
    assert any("partition mismatch" in p for p in check_publication_date(works_df(published=date(2026, 10, 9)), DAY, TODAY))
    future = works_df(published=date(2026, 12, 1))
    assert any("after" in p for p in check_publication_date(future, date(2026, 12, 1), TODAY))


def test_row_count_skipped_without_baseline():
    assert check_row_count(10, []) == []


def test_row_count_accepts_normal_variation_and_flags_outliers():
    previous = [5000, 5200, 4800, 5100]

    assert check_row_count(5300, previous) == []
    assert check_row_count(2000, previous)  # under half of the median
    assert check_row_count(11000, previous)  # over double the median


def test_row_count_uses_only_recent_window():
    old_big_days = [100000] * 5
    recent = [50, 52, 48, 51, 49, 50, 53]

    assert check_row_count(55, old_big_days + recent) == []


def test_null_publication_date_is_flagged():
    works = pl.DataFrame(
        {"work_id": ["W1"], "publication_date": [None]},
        schema={"work_id": pl.String, "publication_date": pl.Date},
    )

    assert any("null publication_date" in p for p in check_publication_date(works, DAY, TODAY))


def test_null_rank_and_topic_id_are_flagged():
    topics = pl.DataFrame(
        [("W1", "T1", None), ("W2", None, 1)],
        schema={"work_id": pl.String, "topic_id": pl.String, "rank": pl.Int64},
        orient="row",
    )

    problems = check_topics_shape(works_df(), topics)

    assert any("rank outside" in p for p in problems)
    assert any("null topic_id" in p for p in problems)


def test_null_work_id_in_topics_is_flagged():
    topics = pl.DataFrame(
        [(None, "T1", 1)],
        schema={"work_id": pl.String, "topic_id": pl.String, "rank": pl.Int64},
        orient="row",
    )

    assert any("null work_id" in p for p in check_topics_reference_works(works_df(), topics))


def test_empty_day_fails_even_without_baseline():
    assert check_row_count(0, [])


def test_run_checks_reports_every_check_by_name():
    results = run_checks(works_df(), topics_df(GOOD_TOPICS), DAY, TODAY, [2, 2, 3])

    assert set(results) == {
        "work_id not null and unique",
        "topics reference works",
        "topics shape",
        "publication date",
        "row count vs recent days",
    }
    assert all(problems == [] for problems in results.values())
