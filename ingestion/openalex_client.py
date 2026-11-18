"""OpenAlex works client: cursor-paginated fetch of a publication-date range."""

import time
from collections.abc import Iterator
from datetime import date

import httpx

BASE_URL = "https://api.openalex.org"
COMPUTER_SCIENCE_FIELD_ID = 17
PER_PAGE = 100  # API maximum


class BudgetExhausted(Exception):
    """The daily API budget is used up; no point retrying until it resets (00:00 UTC)."""


class OpenAlexClient:
    def __init__(
        self,
        api_key: str | None = None,
        mailto: str | None = None,
        client: httpx.Client | None = None,
        max_retries: int = 5,
        backoff: float = 2.0,
        sleep=time.sleep,
    ):
        self.api_key = api_key
        self.mailto = mailto
        self.client = client or httpx.Client(base_url=BASE_URL, timeout=60)
        self.max_retries = max_retries
        self.backoff = backoff
        self._sleep = sleep

    def iter_pages(
        self,
        from_date: date,
        to_date: date,
        field_id: int = COMPUTER_SCIENCE_FIELD_ID,
    ) -> Iterator[dict]:
        """Yield raw response pages for works published in [from_date, to_date]."""
        params = {
            "filter": (
                f"primary_topic.field.id:{field_id},"
                f"from_publication_date:{from_date.isoformat()},"
                f"to_publication_date:{to_date.isoformat()}"
            ),
            "per-page": PER_PAGE,
            "cursor": "*",
        }
        while True:
            page = self._get("/works", params)
            if not page["results"]:
                return
            yield page
            cursor = page["meta"].get("next_cursor")
            if not cursor:
                return
            params["cursor"] = cursor

    def _get(self, path: str, params: dict) -> dict:
        params = dict(params)
        if self.mailto:
            params["mailto"] = self.mailto
        # Header, not query string, so the key never shows up in URLs or logs.
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

        for attempt in range(self.max_retries + 1):
            try:
                resp = self.client.get(path, params=params, headers=headers)
            except httpx.TransportError:
                if attempt == self.max_retries:
                    raise
                self._sleep(self.backoff**attempt)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                # Per-second limits cost nothing, so wait and retry; a spent daily budget won't recover.
                if resp.headers.get("x-ratelimit-remaining") == "0":
                    raise BudgetExhausted(
                        f"daily budget spent, resets in {resp.headers.get('x-ratelimit-reset', '?')}s"
                    )
            elif resp.status_code < 500:
                resp.raise_for_status()

            if attempt == self.max_retries:
                resp.raise_for_status()
            self._sleep(self.backoff**attempt)
        raise RuntimeError("unreachable")
