"""Data quality checks on the silver tables. Each check returns a list of problems (empty = passed)."""

import statistics
from datetime import date, timedelta

import polars as pl

MAX_RANK = 3
ROW_COUNT_MIN_RATIO = 0.5
ROW_COUNT_MAX_RATIO = 2.0
ROW_COUNT_WEEKS = 4  # the same weekday of this many previous weeks is the baseline


def check_work_id(works: pl.DataFrame) -> list[str]:
    problems = []
    nulls = works["work_id"].null_count()
    if nulls:
        problems.append(f"{nulls} works have a null work_id")
    duplicates = works.filter(pl.col("work_id").is_not_null())["work_id"].is_duplicated().sum()
    if duplicates:
        problems.append(f"{duplicates} works rows share a work_id with another row")
    return problems


def check_topics_reference_works(works: pl.DataFrame, topics: pl.DataFrame) -> list[str]:
    problems = []
    nulls = topics["work_id"].null_count()
    if nulls:
        problems.append(f"{nulls} work_topics rows have a null work_id")
    orphans = topics.filter(pl.col("work_id").is_not_null()).join(
        works.select("work_id"), on="work_id", how="anti"
    )
    if not orphans.is_empty():
        problems.append(f"{len(orphans)} work_topics rows point to a work missing from works")
    return problems


def check_topics_shape(works: pl.DataFrame, topics: pl.DataFrame) -> list[str]:
    problems = []
    null_topic_ids = topics["topic_id"].null_count()
    if null_topic_ids:
        problems.append(f"{null_topic_ids} work_topics rows have a null topic_id")
    duplicates = topics.select("work_id", "topic_id").is_duplicated().sum()
    if duplicates:
        problems.append(f"{duplicates} work_topics rows repeat a (work_id, topic_id) pair")
    # A null rank makes is_between return null, so count it explicitly.
    bad_rank = topics.filter(~pl.col("rank").is_between(1, MAX_RANK).fill_null(False)).height
    if bad_rank:
        problems.append(f"{bad_rank} work_topics rows have a rank outside 1..{MAX_RANK}")
    without_topics = works.join(topics.select("work_id").unique(), on="work_id", how="anti").height
    if without_topics:
        problems.append(f"{without_topics} works have no topic")
    return problems


def check_publication_date(works: pl.DataFrame, day: date, today: date) -> list[str]:
    problems = []
    null_dates = works["publication_date"].null_count()
    if null_dates:
        problems.append(f"{null_dates} works have a null publication_date")
    wrong_day = works.filter(pl.col("publication_date") != day).height
    if wrong_day:
        problems.append(f"{wrong_day} works are not published on {day} (partition mismatch)")
    in_future = works.filter(pl.col("publication_date") > today).height
    if in_future:
        problems.append(f"{in_future} works have a publication_date after {today}")
    return problems


def row_count_baseline(day: date, counts: dict[date, int]) -> list[int]:
    """Works counts of the same weekday in the previous ROW_COUNT_WEEKS weeks.

    Works per day follow a weekly cycle (weekends about half of weekdays), so a trailing
    window of plain days puts weekends near the lower bound. The 1st of a month is left out
    of the baseline because it runs high (likely works with only a month as their date).
    """
    weeks_back = (day - timedelta(weeks=w) for w in range(1, ROW_COUNT_WEEKS + 1))
    return [counts[d] for d in weeks_back if d in counts and d.day != 1]


def check_row_count(day: date, count: int, counts: dict[date, int]) -> list[str]:
    """Flag an empty day, or one far from the median of the same weekday in previous weeks.

    `counts` holds works per day for any days; only the baseline days are used. The band is
    skipped without a baseline, and its upper bound on the 1st of a month (see above).
    """
    if count == 0:
        return ["silver has 0 works for this day"]
    baseline = row_count_baseline(day, counts)
    if not baseline:
        return []
    median = statistics.median(baseline)
    low, high = median * ROW_COUNT_MIN_RATIO, median * ROW_COUNT_MAX_RATIO
    too_high = count > high and day.day != 1
    if count < low or too_high:
        message = (
            f"{count} works is outside {low:.0f}..{high:.0f} "
            f"({ROW_COUNT_MIN_RATIO:.0%}-{ROW_COUNT_MAX_RATIO:.0%} of the median {median:.0f} "
            f"of the same weekday over the previous {len(baseline)} weeks)"
        )
        return [message]
    return []


def run_checks(
    works: pl.DataFrame,
    topics: pl.DataFrame,
    day: date,
    today: date,
    counts: dict[date, int],
) -> dict[str, list[str]]:
    """Run every check for one day and return {check name: problems}. `counts`: works per day."""
    return {
        "work_id not null and unique": check_work_id(works),
        "topics reference works": check_topics_reference_works(works, topics),
        "topics shape": check_topics_shape(works, topics),
        "publication date": check_publication_date(works, day, today),
        "row count vs same weekday": check_row_count(day, len(works), counts),
    }
