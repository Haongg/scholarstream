"""Ingest OpenAlex works into bronze, one publication day at a time.

    python -m ingestion.run                       # lookback window + everything after the watermark
    python -m ingestion.run --from 2026-10-01 --to 2026-10-03   # explicit range / backfill

Each day is complete or absent: pages are rewritten from scratch and the watermark only
moves after a day finishes, so re-running a day never leaves duplicates. If the daily API
budget runs out, the run stops and the unfinished day is redone next time.

OpenAlex indexes works days to weeks after their publication date, so a day loaded once
is not final. Without --from, every run also re-fetches the last INGEST_LOOKBACK_DAYS days
(default 7) ending at --to, picking up late arrivals; re-fetching is safe since days are
rewritten whole.

Each day is reconciled against OpenAlex's meta.count (same count on every page, total
written equals it, no duplicate ids). A mismatch is retried once, then the run stops.
Only a reconciled day gets its _SUCCESS marker; downstream reads only marked days.

Exit codes: 0 ok, 1 no watermark, 2 API budget spent, 3 count mismatch.
"""

import argparse
import logging
import os
import sys
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


def resolve_start(last_loaded: date, end: date, lookback_days: int) -> date:
    """First day to load: the day after the watermark, or earlier to cover the lookback window."""
    after_watermark = last_loaded + timedelta(days=1)
    if lookback_days <= 0:
        return after_watermark
    return min(after_watermark, end - timedelta(days=lookback_days - 1))


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


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # request URLs are noisy and carry credentials
    args = parse_args(argv)

    yesterday = datetime.now(UTC).date() - timedelta(days=1)  # today is still incomplete
    end = args.end or yesterday

    with connect() as conn:
        watermark = Watermark(conn)
        start = args.start
        if start is None:
            last = watermark.get()
            if last is None:
                log.error("No watermark yet: pass --from YYYY-MM-DD for the first load")
                return 1
            lookback = int(os.getenv("INGEST_LOOKBACK_DAYS") or DEFAULT_LOOKBACK_DAYS)
            start = resolve_start(last, end, lookback)
        if start > end:
            log.info("Nothing to load (start %s is after end %s)", start, end)
            return 0

        client = OpenAlexClient(os.getenv("OPENALEX_API_KEY"), os.getenv("OPENALEX_MAILTO"))
        writer = BronzeWriter()
        for day in daterange(start, end):
            try:
                works = ingest_day_with_retry(client, writer, day)
            except BudgetExhausted as exc:
                log.warning("Stopping at %s: %s. Re-run after the budget resets.", day, exc)
                return 2
            except CountMismatch as exc:
                # Watermark stays put, so the next run redoes this day.
                log.error("Stopping: %s", exc)
                return 3
            watermark.advance(day)
            log.info("Loaded %s: %d works", day, works)
    return 0


if __name__ == "__main__":
    sys.exit(main())
