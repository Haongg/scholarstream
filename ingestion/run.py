"""Ingest OpenAlex works into bronze, one publication day at a time.

    python -m ingestion.run                       # from watermark + 1 day up to yesterday
    python -m ingestion.run --from 2026-10-01 --to 2026-10-03   # explicit range / backfill

Each day is complete or absent: pages are rewritten from scratch and the watermark only
moves after a day finishes, so re-running a day never leaves duplicates. If the daily API
budget runs out, the run stops and the unfinished day is redone next time.
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv

from ingestion.bronze_writer import BronzeWriter
from ingestion.openalex_client import BudgetExhausted, OpenAlexClient
from ingestion.watermark import Watermark, connect

log = logging.getLogger("ingestion")


def daterange(start: date, end: date):
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def ingest_day(client: OpenAlexClient, writer: BronzeWriter, day: date) -> int:
    writer.clear_day(day)
    works = 0
    for number, page in enumerate(client.iter_pages(day, day), start=1):
        writer.write_page(day, number, page)
        works += len(page["results"])
    return works


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

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)  # today is still incomplete
    end = args.end or yesterday

    with connect() as conn:
        watermark = Watermark(conn)
        start = args.start
        if start is None:
            last = watermark.get()
            if last is None:
                log.error("No watermark yet: pass --from YYYY-MM-DD for the first load")
                return 1
            start = last + timedelta(days=1)
        if start > end:
            log.info("Nothing to load (start %s is after end %s)", start, end)
            return 0

        client = OpenAlexClient(os.getenv("OPENALEX_API_KEY"), os.getenv("OPENALEX_MAILTO"))
        writer = BronzeWriter()
        for day in daterange(start, end):
            try:
                works = ingest_day(client, writer, day)
            except BudgetExhausted as exc:
                log.warning("Stopping at %s: %s. Re-run after the budget resets.", day, exc)
                return 2
            watermark.advance(day)
            log.info("Loaded %s: %d works", day, works)
    return 0


if __name__ == "__main__":
    sys.exit(main())
