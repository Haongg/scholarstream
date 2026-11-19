from datetime import date, timedelta

import polars as pl

from quality.checks import (
    check_publication_date,
    check_row_count,
    check_topic_dimension,
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


def topic_dim_df(*rows):
    """topics table rows as (topic_id, subfield_id); the other names are filled in."""
    return pl.DataFrame(
        [
            {
                "topic_id": t, "topic_name": f"Topic {t}", "subfield_id": s, "subfield_name": "Sub",
                "field_id": "17", "field_name": "Computer Science", "domain_id": "3", "domain_name": "Physical Sciences",
            }
            for t, s in rows
        ]
    )


GOOD_TOPIC_DIM = topic_dim_df(("T1", "1707"), ("T2", "1702"))


def test_topic_dimension_passes_on_good_data():
    assert check_topic_dimension(topics_df(GOOD_TOPICS), GOOD_TOPIC_DIM) == []


def test_topic_dimension_flags_topic_with_two_hierarchies():
    problems = check_topic_dimension(topics_df(GOOD_TOPICS), topic_dim_df(("T1", "1707"), ("T1", "1702"), ("T2", "1702")))

    assert any("more than one hierarchy: T1" in p for p in problems)


def test_topic_dimension_flags_incomplete_hierarchy():
    dim = GOOD_TOPIC_DIM.with_columns(pl.when(pl.col("topic_id") == "T2").then(None).otherwise(pl.col("field_id")).alias("field_id"))

    assert any("miss a name" in p for p in check_topic_dimension(topics_df(GOOD_TOPICS), dim))


def test_topic_dimension_flags_topic_used_but_not_described():
    problems = check_topic_dimension(topics_df(GOOD_TOPICS), topic_dim_df(("T1", "1707")))

    assert any("1 topic_ids in work_topics are missing" in p for p in problems)


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


SUNDAY = date(2026, 11, 8)


def weeks_before(day: date, *counts: int) -> dict[date, int]:
    """{day - 1 week: counts[0], day - 2 weeks: counts[1], ...}"""
    return {day - timedelta(weeks=i + 1): c for i, c in enumerate(counts)}


def test_row_count_skipped_without_baseline():
    assert check_row_count(SUNDAY, 10, {}) == []


def test_row_count_accepts_normal_variation_and_flags_outliers():
    counts = weeks_before(SUNDAY, 5000, 5200, 4800, 5100)

    assert check_row_count(SUNDAY, 5300, counts) == []
    assert check_row_count(SUNDAY, 2000, counts)  # under half of the median
    assert check_row_count(SUNDAY, 11000, counts)  # over double the median


def test_weekend_is_judged_against_earlier_weekends_not_weekdays():
    # Real silver around 2026-11-08: the weekdays before it are ~2,900, a trailing 7-day
    # median put the floor at 1,440 and failed this Sunday's 1,424 works.
    weekdays = {SUNDAY - timedelta(days=i): 2900 for i in range(1, 7)}
    sundays = weeks_before(SUNDAY, 1660, 1464, 1728)

    assert check_row_count(SUNDAY, 1424, {**weekdays, **sundays}) == []


def test_row_count_uses_only_the_last_weeks():
    counts = weeks_before(SUNDAY, 50, 52, 48, 51, 100000, 100000, 100000)

    assert check_row_count(SUNDAY, 55, counts) == []


def test_first_of_month_is_left_out_of_the_baseline():
    day = date(2026, 11, 29)  # four weeks after Sunday 2026-11-01
    counts = {date(2026, 11, 22): 1600, date(2026, 11, 1): 9000}

    assert check_row_count(day, 1500, counts) == []
    # Judged against 1,600 alone; with the 1st included the median would be 5,300 and pass it.
    assert check_row_count(day, 4000, counts)


def test_first_of_month_may_run_high_but_not_low():
    day = date(2026, 12, 1)
    counts = weeks_before(day, 3000, 2900, 3100)

    assert check_row_count(day, 6400, counts) == []  # month-only dates land on the 1st
    assert check_row_count(day, 1000, counts)


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
    assert check_row_count(SUNDAY, 0, {})


def test_run_checks_reports_every_check_by_name():
    results = run_checks(works_df(), topics_df(GOOD_TOPICS), DAY, TODAY, weeks_before(DAY, 2, 2, 3), GOOD_TOPIC_DIM)

    assert set(results) == {
        "work_id not null and unique",
        "topics reference works",
        "topics shape",
        "topic dimension",
        "publication date",
        "row count vs same weekday",
    }
    assert all(problems == [] for problems in results.values())
