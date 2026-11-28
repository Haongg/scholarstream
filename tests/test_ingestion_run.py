"""Tests for the meta.count reconciliation, completion marker and lookback in ingestion.run."""

from datetime import date, timedelta

import pytest

from ingestion import run
from ingestion.run import (
    CountMismatch,
    ingest_day,
    ingest_day_with_retry,
    refresh_window,
    resolve_start,
)

DAY = date(2026, 10, 1)


def page(ids, count):
    return {"meta": {"count": count}, "results": [{"id": i} for i in ids]}


class FakeClient:
    """Returns one list of pages per call, so retries can see different data."""

    def __init__(self, *runs):
        self.runs = list(runs)
        self.calls = 0
        self.requests = 0  # one credit per page, like OpenAlexClient
        self.credits_remaining = None

    def iter_pages(self, from_date, to_date):
        self.calls += 1
        run = self.runs.pop(0)
        if isinstance(run, Exception):
            raise run
        for p in run:
            self.requests += 1
            yield p


class FakeWriter:
    def __init__(self):
        self.pages = {}
        self.cleared = 0
        self.complete = False

    def clear_day(self, day):
        self.cleared += 1
        self.pages.clear()
        self.complete = False

    def write_page(self, day, number, page):
        self.pages[number] = page

    def mark_complete(self, day):
        self.complete = True


def test_matching_count_passes():
    client = FakeClient([page(["W1", "W2"], 3), page(["W3"], 3)])

    assert ingest_day(client, FakeWriter(), DAY) == 3


def test_fewer_works_than_meta_count_fails():
    client = FakeClient([page(["W1", "W2"], 5)])

    with pytest.raises(CountMismatch, match="wrote 2 works but meta.count is 5"):
        ingest_day(client, FakeWriter(), DAY)


def test_meta_count_changing_between_pages_fails():
    client = FakeClient([page(["W1", "W2"], 3), page(["W3"], 4)])

    with pytest.raises(CountMismatch, match="changed during paging"):
        ingest_day(client, FakeWriter(), DAY)


def test_duplicate_ids_across_pages_fail():
    client = FakeClient([page(["W1", "W2"], 3), page(["W2"], 3)])

    with pytest.raises(CountMismatch, match="duplicate"):
        ingest_day(client, FakeWriter(), DAY)


def test_empty_day_returns_zero():
    assert ingest_day(FakeClient([]), FakeWriter(), DAY) == 0


def test_mismatch_is_retried_once_and_bronze_is_rewritten():
    client = FakeClient([page(["W1"], 2)], [page(["W1", "W2"], 2)])
    writer = FakeWriter()

    assert ingest_day_with_retry(client, writer, DAY) == 2
    assert client.calls == 2
    assert writer.cleared == 2  # the retry starts from a clean partition
    assert len(writer.pages) == 1


def test_persistent_mismatch_raises_after_retry():
    client = FakeClient([page(["W1"], 2)], [page(["W1"], 2)])

    writer = FakeWriter()
    with pytest.raises(CountMismatch):
        ingest_day_with_retry(client, writer, DAY)
    assert client.calls == 2
    assert not writer.complete


def test_reconciled_day_is_marked_complete():
    writer = FakeWriter()
    ingest_day(FakeClient([page(["W1"], 1)]), writer, DAY)

    assert writer.complete


def test_empty_day_is_marked_complete():
    writer = FakeWriter()
    ingest_day(FakeClient([]), writer, DAY)

    assert writer.complete


def test_mismatched_day_is_not_marked_complete():
    writer = FakeWriter()
    with pytest.raises(CountMismatch):
        ingest_day(FakeClient([page(["W1"], 2)]), writer, DAY)

    assert not writer.complete


def test_failure_mid_paging_leaves_day_unmarked():
    def broken_pages(from_date, to_date):
        yield page(["W1"], 2)
        raise RuntimeError("budget spent")

    client = FakeClient()
    client.iter_pages = broken_pages
    writer = FakeWriter()
    writer.complete = True  # left over from an earlier successful load

    with pytest.raises(RuntimeError):
        ingest_day(client, writer, DAY)
    assert not writer.complete  # clear_day dropped the old marker


END = date(2026, 10, 20)


def test_lookback_reaches_back_past_an_up_to_date_watermark():
    assert resolve_start(last_loaded=date(2026, 10, 19), end=END, lookback_days=7) == date(2026, 10, 14)


def test_watermark_older_than_lookback_wins():
    assert resolve_start(last_loaded=date(2026, 10, 1), end=END, lookback_days=7) == date(2026, 10, 2)


def test_lookback_zero_starts_after_watermark():
    assert resolve_start(last_loaded=date(2026, 10, 19), end=END, lookback_days=0) == END


def test_refresh_starts_right_before_the_lookback_window():
    start, end = refresh_window(END, lookback_days=7, refresh_days=60)
    lookback_start = resolve_start(last_loaded=END, end=END, lookback_days=7)

    assert end == lookback_start - timedelta(days=1)  # adjacent: no gap, no day fetched twice
    assert start == END - timedelta(days=59)  # together they cover the last 60 days
    assert (end - start).days + 1 == 53


def test_no_refresh_when_the_lookback_covers_it():
    assert refresh_window(END, lookback_days=60, refresh_days=60) is None


def test_cli_refresh_loads_the_window_before_to(monkeypatch):
    calls = []
    monkeypatch.setattr(run, "ingest", lambda start, end: calls.append((start, end)) or run.IngestResult())
    monkeypatch.setattr(run, "load_dotenv", lambda: None)
    monkeypatch.setenv("INGEST_LOOKBACK_DAYS", "7")
    monkeypatch.setenv("INGEST_REFRESH_DAYS", "60")

    assert run.main(["--refresh", "--to", END.isoformat()]) == 0
    assert calls == [(date(2026, 8, 22), date(2026, 10, 13))]


def test_cli_refresh_rejects_from():
    with pytest.raises(SystemExit):
        run.parse_args(["--refresh", "--from", "2026-10-01"])


@pytest.mark.parametrize(
    "outcome, code",
    [
        (run.IngestResult(loaded=[DAY]), 0),
        (run.NoWatermark("none"), 1),
        (run.IngestResult(budget_exhausted=True), 2),
        (CountMismatch("off"), 3),
    ],
)
def test_cli_exit_codes(monkeypatch, outcome, code):
    def fake_ingest(start, end):
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(run, "ingest", fake_ingest)
    monkeypatch.setattr(run, "load_dotenv", lambda: None)

    assert run.main([]) == code


class FakeWatermark:
    def __init__(self):
        self.advanced = []

    def advance(self, day):
        self.advanced.append(day)


def test_load_days_reports_works_per_day_and_credits():
    day2 = DAY + timedelta(days=1)
    client = FakeClient([page(["W1", "W2"], 3), page(["W3"], 3)], [page(["W4"], 1)])
    result = run.IngestResult()

    run.load_days(client, FakeWriter(), FakeWatermark(), DAY, day2, result)

    assert result.loaded == [DAY, day2]
    assert result.works == {DAY: 3, day2: 1}
    assert result.credits_used == 3
    assert not result.budget_exhausted


def test_spent_budget_still_reports_the_credits_used():
    day2 = DAY + timedelta(days=1)
    client = FakeClient([page(["W1"], 1)], run.BudgetExhausted("daily budget spent"))
    watermark = FakeWatermark()
    result = run.IngestResult()

    run.load_days(client, FakeWriter(), watermark, DAY, day2, result)

    assert result.budget_exhausted
    assert result.loaded == [DAY] == watermark.advanced
    assert result.works == {DAY: 1}
    assert result.credits_used == 1
