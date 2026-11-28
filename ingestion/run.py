"""Ingest OpenAlex works into bronze, one publication day at a time.

    python -m ingestion.run                       # lookback window + everything after the watermark
    python -m ingestion.run --from 2026-10-01 --to 2026-10-03   # explicit range / backfill
    python -m ingestion.run --refresh             # days older than the lookback window (weekly)

Each day is complete or absent: pages are rewritten from scratch and the watermark only
moves after a day finishes, so re-running a day never leaves duplicates. If the daily API
budget runs out, the run stops and the unfinished day is redone next time.

OpenAlex indexes works days to weeks after their publication date, so a day loaded once
is not final. Without --from, every run also re-fetches the last INGEST_LOOKBACK_DAYS days
(default 7) ending at --to, picking up late arrivals; re-fetching is safe since days are
rewritten whole. Works keep arriving (or moving to another day) for weeks, so a weekly
--refresh re-fetches the days just older than that window, up to INGEST_REFRESH_DAYS
(default 60) back, at a fraction of the cost of a 60-day daily lookback.

Each day is reconciled against OpenAlex's meta.count (same count on every page, total
written equals it, no duplicate ids). A mismatch is retried once, then the run stops.
Only a reconciled day gets its _SUCCESS marker; downstream reads only marked days.

Exit codes: 0 ok, 1 no watermark, 2 API budget spent, 3 count mismatch.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from dotenv import load_dotenv

from ingestion.bronze_writer import BronzeWriter
from ingestion.openalex_client import BudgetExhausted, OpenAlexClient
from ingestion.watermark import Watermark, connect

log = logging.getLogger("ingestion")


def daterange(start: date, end: date):
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


MAX_DAY_ATTEMPTS = 2  # a count mismatch is retried once, in case OpenAlex changed mid-paging
DEFAULT_LOOKBACK_DAYS = 7  # ~60 credits/day for CS, so a week costs ~400 of the 10,000 budget


def lookback_days() -> int:
    """How many recent days every run re-fetches (INGEST_LOOKBACK_DAYS), since OpenAlex indexes late."""
    return int(os.getenv("INGEST_LOOKBACK_DAYS") or DEFAULT_LOOKBACK_DAYS)


def resolve_start(last_loaded: date, end: date, lookback_days: int) -> date:
    """First day to load: the day after the watermark, or earlier to cover the lookback window."""
    after_watermark = last_loaded + timedelta(days=1)
    if lookback_days <= 0:
        return after_watermark
    return min(after_watermark, end - timedelta(days=lookback_days - 1))


# Measured 2026-11-22: ~6% of CS works appeared or changed day after leaving a 7-day window.
DEFAULT_REFRESH_DAYS = 60  # ~30 credits/day for CS, so a weekly refresh costs ~1,600


def refresh_days() -> int:
    """How far back the weekly refresh reaches (INGEST_REFRESH_DAYS), counted from end."""
    return int(os.getenv("INGEST_REFRESH_DAYS") or DEFAULT_REFRESH_DAYS)


def refresh_window(end: date, lookback_days: int, refresh_days: int) -> tuple[date, date] | None:
    """Days older than the lookback window ending at end, back to refresh_days before end.

    The daily run already covers [end - lookback + 1, end], so the refresh starts right
    before it and the two never fetch the same day. None if the lookback covers it all.
    """
    window_end = end - timedelta(days=max(lookback_days, 0))
    window_start = end - timedelta(days=refresh_days - 1)
    if window_start > window_end:
        return None
    return window_start, window_end


class CountMismatch(Exception):
    """What was written to bronze does not match what OpenAlex said the day holds."""


def ingest_day(client: OpenAlexClient, writer: BronzeWriter, day: date) -> int:
    """Rewrite one day in bronze and reconcile it against OpenAlex's meta.count."""
    writer.clear_day(day)
    works = 0
    ids = set()
    reported = set()  # meta.count of every page; should be a single value
    for number, page in enumerate(client.iter_pages(day, day), start=1):
        writer.write_page(day, number, page)
        works += len(page["results"])
        ids.update(work["id"] for work in page["results"])
        reported.add(page["meta"]["count"])

    if not reported:
        # No page came back, so there is no meta.count to reconcile against.
        log.warning("%s: OpenAlex returned no works", day)
    else:
        if len(reported) > 1:
            raise CountMismatch(f"{day}: meta.count changed during paging {sorted(reported)}")
        expected = reported.pop()
        if works != expected:
            raise CountMismatch(f"{day}: wrote {works} works but meta.count is {expected}")
        if len(ids) != works:
            raise CountMismatch(f"{day}: {works - len(ids)} duplicate work ids across pages")
    writer.mark_complete(day)
    return works


def ingest_day_with_retry(client: OpenAlexClient, writer: BronzeWriter, day: date) -> int:
    for attempt in range(1, MAX_DAY_ATTEMPTS + 1):
        try:
            return ingest_day(client, writer, day)
        except CountMismatch as exc:
            if attempt == MAX_DAY_ATTEMPTS:
                raise
            log.warning("%s; retrying the day (attempt %d/%d)", exc, attempt + 1, MAX_DAY_ATTEMPTS)
    raise RuntimeError("unreachable")


class NoWatermark(Exception):
    """No start date given and nothing loaded yet, so there is nothing to continue from."""


@dataclass
class IngestResult:
    loaded: list[date] = field(default_factory=list)  # days written, reconciled and marked
    budget_exhausted: bool = False  # stopped early; the remaining days wait for the next run


def ingest(start: date | None = None, end: date | None = None) -> IngestResult:
    """Load [start, end] into bronze. Used by the CLI below and by the Airflow DAG.

    start defaults to the lookback window / watermark, end to yesterday (UTC).
    Raises NoWatermark, or CountMismatch when a day still disagrees after its retry.
    """
    logging.getLogger("httpx").setLevel(logging.WARNING)  # request URLs are noisy and carry credentials
    yesterday = datetime.now(UTC).date() - timedelta(days=1)  # today is still incomplete
    end = end or yesterday
    result = IngestResult()

    with connect() as conn:
        watermark = Watermark(conn)
        if start is None:
            last = watermark.get()
            if last is None:
                raise NoWatermark("No watermark yet: pass --from YYYY-MM-DD for the first load")
            start = resolve_start(last, end, lookback_days())
        if start > end:
            log.info("Nothing to load (start %s is after end %s)", start, end)
            return result

        client = OpenAlexClient(os.getenv("OPENALEX_API_KEY"), os.getenv("OPENALEX_MAILTO"))
        writer = BronzeWriter()
        for day in daterange(start, end):
            try:
                works = ingest_day_with_retry(client, writer, day)
            except BudgetExhausted as exc:
                log.warning("Stopping at %s: %s. Re-run after the budget resets.", day, exc)
                result.budget_exhausted = True
                return result
            # A CountMismatch propagates: the watermark stays put, so the next run redoes the day.
            watermark.advance(day)
            result.loaded.append(day)
            log.info("Loaded %s: %d works", day, works)
    return result


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="re-fetch the days older than the lookback window, up to INGEST_REFRESH_DAYS before --to",
    )
    args = parser.parse_args(argv)
    if args.refresh and args.start:
        parser.error("--refresh sets its own start; use --to to move the window")
    return args


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    start, end = args.start, args.end
    if args.refresh:
        end = end or datetime.now(UTC).date() - timedelta(days=1)
        window = refresh_window(end, lookback_days(), refresh_days())
        if window is None:
            log.info("Nothing to refresh: the lookback window already covers %d days", refresh_days())
            return 0
        start, end = window
    try:
        result = ingest(start, end)
    except NoWatermark as exc:
        log.error("%s", exc)
        return 1
    except CountMismatch as exc:
        log.error("Stopping: %s", exc)
        return 3
    return 2 if result.budget_exhausted else 0


if __name__ == "__main__":
    sys.exit(main())
