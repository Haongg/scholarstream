"""Tests for the pipeline health checks (pure functions, fixed clock)."""

import logging
from datetime import UTC, date, datetime, timedelta

import pytest

from monitoring import health
from monitoring.health import HealthInputs

NOW = datetime(2026, 11, 28, 6, 0, tzinfo=UTC)  # the DAG's 06:00 UTC run
TODAY = NOW.date()
DAY_AGO = NOW - timedelta(hours=5, minutes=45)  # the 00:15 ingest of the same day
MAX_AGE = timedelta(hours=26)


def healthy(**overrides) -> HealthInputs:
    inputs = {
        "last_gold_day": TODAY - timedelta(days=2),
        "last_ingest_success": DAY_AGO,
        "unchecked_days": [TODAY - timedelta(days=1)],  # yesterday is deferred: normal
        "recent_budget_exhausted": [False, False],
    }
    return HealthInputs(**{**inputs, **overrides})


def failing(results):
    return {name for name, problems in results.items() if problems}


def test_a_normal_morning_is_healthy():
    assert failing(health.run_checks(healthy(), NOW, 3, MAX_AGE, 7)) == set()


def test_gold_three_days_behind_is_still_fine_but_four_is_not():
    assert health.check_gold_freshness(TODAY - timedelta(days=3), TODAY, 3) == []
    assert health.check_gold_freshness(TODAY - timedelta(days=4), TODAY, 3) == [
        "latest gold day 2026-11-24 is 4 days old (max 3)"
    ]


def test_missing_gold_is_unhealthy():
    assert health.check_gold_freshness(None, TODAY, 3) == ["gold is empty or missing"]


def test_ingest_older_than_a_day_and_slack_is_unhealthy():
    assert health.check_ingest_recency(NOW - timedelta(hours=26), NOW, MAX_AGE) == []
    problems = health.check_ingest_recency(NOW - timedelta(hours=30), NOW, MAX_AGE)
    assert problems == ["last successful ingest finished 30 h ago (2026-11-27 00:00 UTC; max 26 h)"]


def test_no_ingest_on_record_is_unhealthy():
    assert health.check_ingest_recency(None, NOW, MAX_AGE) == ["no successful ingest_openalex run on record"]


def test_unchecked_days_are_stuck_only_once_past_the_lookback():
    settling = TODAY - timedelta(days=7)
    stuck = TODAY - timedelta(days=8)

    assert health.check_stuck_days([settling], TODAY, 7) == []
    assert health.check_stuck_days([stuck, settling], TODAY, 7) == [
        "1 silver day(s) past the 7-day lookback still lack _QUALITY_OK: 2026-11-20"
    ]


@pytest.mark.parametrize(
    ("recent", "unhealthy"),
    [([True, True], True), ([True, False], False), ([False, True], False), ([True], False), ([], False)],
)
def test_budget_alerts_only_on_two_spent_runs_in_a_row(recent, unhealthy):
    assert bool(health.check_budget(recent)) is unhealthy


def test_the_gap_of_2026_11_23_would_have_been_caught():
    # Last good state on 11-22 (gold up to 11-20, ingest 00:20); judged on the morning of 11-25.
    inputs = healthy(
        last_gold_day=date(2026, 11, 20), last_ingest_success=datetime(2026, 11, 22, 0, 20, tzinfo=UTC)
    )
    now = health.as_of_moment(date(2026, 11, 25))

    assert failing(health.run_checks(inputs, now, 3, MAX_AGE, 7)) == {health.GOLD_FRESHNESS, health.INGEST_RECENCY}


def test_as_of_means_06_utc_that_day():
    assert health.as_of_moment(date(2026, 12, 1)) == datetime(2026, 12, 1, 6, 0, tzinfo=UTC)


def test_config_reads_thresholds_from_env(monkeypatch):
    monkeypatch.setenv("HEALTH_MAX_GOLD_LAG_DAYS", "5")
    monkeypatch.setenv("HEALTH_MAX_INGEST_AGE_HOURS", "50")
    monkeypatch.setenv("INGEST_LOOKBACK_DAYS", "7")

    assert health.config() == (5, timedelta(hours=50), 7)


def test_cli_logs_each_check_and_exits_1_on_a_problem(monkeypatch, caplog):
    monkeypatch.setattr(health, "health", lambda now: {"gold freshness": ["stale"], "ingest recency": []})
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)

    assert health.main(["--as-of", "2026-12-01"]) == 1


def test_health_logs_ok_and_fail_lines(monkeypatch, caplog):
    monkeypatch.setattr(health, "collect", lambda s3, conn: healthy(last_gold_day=TODAY - timedelta(days=9)))
    monkeypatch.setattr("ingestion.bronze_writer.s3_client", lambda: None)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr("ingestion.watermark.connect", lambda: Conn())
    monkeypatch.setenv("INGEST_LOOKBACK_DAYS", "7")

    with caplog.at_level(logging.INFO, logger="monitoring.health"):
        results = health.health(NOW)

    assert failing(results) == {health.GOLD_FRESHNESS}
    assert "ok   ingest recency" in caplog.text
    assert "FAIL gold freshness" in caplog.text
    assert health.summary(results) == "gold freshness: latest gold day 2026-11-19 is 9 days old (max 3)"
