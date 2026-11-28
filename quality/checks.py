"""Data quality checks on the silver tables. Each check returns a list of problems (empty = passed)."""

import statistics
from datetime import date, timedelta
from typing import NamedTuple

import polars as pl

MAX_RANK = 3
ROW_COUNT_MIN_RATIO = 0.5
ROW_COUNT_MAX_RATIO = 2.0
ROW_COUNT_WEEKS = 4  # the same weekday of this many previous weeks is the baseline
ROW_COUNT_CHECK = "row count vs same weekday"  # quality.run defers this one for days still settling


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


TOPIC_NAME_COLUMNS = ("topic_name", "subfield_id", "subfield_name", "field_id", "field_name", "domain_id", "domain_name")


def check_topic_dimension(work_topics: pl.DataFrame, topics: pl.DataFrame) -> list[str]:
    """The day's `topics` table: one row per topic (so one parent each), complete hierarchy,
    and every topic used in work_topics is described."""
    problems = []
    nulls = topics["topic_id"].null_count()
    if nulls:
        problems.append(f"{nulls} topics rows have a null topic_id")
    conflicting = topics.filter(pl.col("topic_id").is_not_null() & pl.col("topic_id").is_duplicated())
    if not conflicting.is_empty():
        ids = conflicting["topic_id"].unique().sort().to_list()
        problems.append(f"{len(ids)} topics appear with more than one hierarchy: {', '.join(ids[:5])}")
    incomplete = topics.filter(pl.any_horizontal(pl.col(c).is_null() for c in TOPIC_NAME_COLUMNS)).height
    if incomplete:
        problems.append(f"{incomplete} topics rows miss a name or a subfield/field/domain")
    undescribed = (
        work_topics.filter(pl.col("topic_id").is_not_null())
        .select("topic_id")
        .unique()
        .join(topics.select("topic_id"), on="topic_id", how="anti")
        .height
    )
    if undescribed:
        problems.append(f"{undescribed} topic_ids in work_topics are missing from topics")
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


class RowCountBand(NamedTuple):
    low: float
    high: float
    median: float
    weeks: int  # how many baseline days the median is over

    def describe(self, count: int, side: str) -> str:
        bound, ratio = (self.low, ROW_COUNT_MIN_RATIO) if side == "below" else (self.high, ROW_COUNT_MAX_RATIO)
        return (
            f"{count} works is {side} {bound:.0f} ({ratio:.0%} of the median {self.median:.0f} "
            f"of the same weekday over the previous {self.weeks} weeks)"
        )


def row_count_band(day: date, counts: dict[date, int]) -> RowCountBand | None:
    """The expected range of works for `day`, or None without a baseline."""
    baseline = row_count_baseline(day, counts)
    if not baseline:
        return None
    median = statistics.median(baseline)
    return RowCountBand(median * ROW_COUNT_MIN_RATIO, median * ROW_COUNT_MAX_RATIO, median, len(baseline))


def check_row_count(day: date, count: int, counts: dict[date, int]) -> list[str]:
    """Blocking: an empty day, or one far below the median of the same weekday in previous weeks.

    Too few works is what a broken load looks like (missing pages, a wrong filter), or a day
    OpenAlex has not finished indexing; the gate defers the latter (see quality.run).
    `counts` holds works per day for any days; only the baseline days are used.
    """
    if count == 0:
        return ["silver has 0 works for this day"]
    band = row_count_band(day, counts)
    if band and count < band.low:
        return [band.describe(count, "below")]
    return []


def row_count_warnings(day: date, count: int, counts: dict[date, int]) -> list[str]:
    """Not blocking: a day far above the median of the same weekday.

    Too many works cannot come from the pipeline: duplicate loads are caught by the work_id
    check, and ingestion reconciles every day against OpenAlex's meta.count. So the excess is
    in the source (e.g. a bulk upload to Zenodo, 2026-11-15) and is left for gold to filter.
    The 1st of a month is exempt, since it runs high anyway (see row_count_baseline).
    """
    band = row_count_band(day, counts)
    if band and count > band.high and day.day != 1:
        return [band.describe(count, "above")]
    return []


def run_checks(
    tables: dict[str, pl.DataFrame], day: date, today: date, counts: dict[date, int]
) -> dict[str, list[str]]:
    """Run every check for one day and return {check name: problems}.

    `tables`: the day's silver tables by name (transform.silver.SILVER_TABLES);
    `counts`: works per day."""
    works, topics, topic_dim = tables["works"], tables["work_topics"], tables["topics"]
    return {
        "work_id not null and unique": check_work_id(works),
        "topics reference works": check_topics_reference_works(works, topics),
        "topics shape": check_topics_shape(works, topics),
        "topic dimension": check_topic_dimension(topics, topic_dim),
        "publication date": check_publication_date(works, day, today),
        ROW_COUNT_CHECK: check_row_count(day, len(works), counts),
    }


def run_warnings(works: pl.DataFrame, day: date, counts: dict[date, int]) -> dict[str, list[str]]:
    """Findings worth logging that do not stop promotion to gold: {check name: warnings}."""
    return {"row count far above same weekday": row_count_warnings(day, len(works), counts)}
