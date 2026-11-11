"""Data quality checks on the silver tables. Each check returns a list of problems (empty = passed)."""

import statistics
from datetime import date

import polars as pl

MAX_RANK = 3
ROW_COUNT_MIN_RATIO = 0.5
ROW_COUNT_MAX_RATIO = 2.0
ROW_COUNT_WINDOW = 7  # previous days used as the baseline


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


def check_row_count(count: int, previous_counts: list[int]) -> list[str]:
    """Flag an empty day, or one far from the median of recent days. Band skipped without a baseline."""
    if count == 0:
        return ["silver has 0 works for this day"]
    baseline = previous_counts[-ROW_COUNT_WINDOW:]
    if not baseline:
        return []
    median = statistics.median(baseline)
    low, high = median * ROW_COUNT_MIN_RATIO, median * ROW_COUNT_MAX_RATIO
    if not low <= count <= high:
        message = (
            f"{count} works is outside {low:.0f}..{high:.0f} "
            f"({ROW_COUNT_MIN_RATIO:.0%}-{ROW_COUNT_MAX_RATIO:.0%} of the median {median:.0f} "
            f"over the last {len(baseline)} days)"
        )
        return [message]
    return []


def run_checks(
    works: pl.DataFrame,
    topics: pl.DataFrame,
    day: date,
    today: date,
    previous_counts: list[int],
) -> dict[str, list[str]]:
    """Run every check for one day and return {check name: problems}."""
    return {
        "work_id not null and unique": check_work_id(works),
        "topics reference works": check_topics_reference_works(works, topics),
        "topics shape": check_topics_shape(works, topics),
        "publication date": check_publication_date(works, day, today),
        "row count vs recent days": check_row_count(len(works), previous_counts),
    }
