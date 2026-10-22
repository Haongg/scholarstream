from datetime import date

import httpx
import pytest

from ingestion.openalex_client import BudgetExhausted, OpenAlexClient


def make_client(handler, **kwargs):
    http = httpx.Client(base_url="https://api.openalex.org", transport=httpx.MockTransport(handler))
    return OpenAlexClient(api_key="k", mailto="me@example.com", client=http, sleep=lambda _: None, **kwargs)


def page(ids, next_cursor):
    return {"meta": {"next_cursor": next_cursor}, "results": [{"id": i} for i in ids]}


def test_follows_cursor_until_exhausted():
    cursors = []

    def handler(request):
        cursor = request.url.params["cursor"]
        cursors.append(cursor)
        if cursor == "*":
            return httpx.Response(200, json=page(["W1", "W2"], "abc"))
        return httpx.Response(200, json=page(["W3"], None))

    pages = list(make_client(handler).iter_pages(date(2026, 10, 1), date(2026, 10, 1)))

    assert cursors == ["*", "abc"]
    assert [w["id"] for p in pages for w in p["results"]] == ["W1", "W2", "W3"]


def test_sends_filter_and_credentials():
    seen = {}

    def handler(request):
        seen.update(request.url.params)
        seen["auth"] = request.headers["authorization"]
        return httpx.Response(200, json=page([], None))

    list(make_client(handler).iter_pages(date(2026, 10, 1), date(2026, 10, 2)))

    assert seen["filter"] == (
        "primary_topic.field.id:17,from_publication_date:2026-10-01,to_publication_date:2026-10-02"
    )
    assert seen["auth"] == "Bearer k"
    assert "api_key" not in seen
    assert seen["mailto"] == "me@example.com"


def test_retries_per_second_rate_limit():
    calls = iter([httpx.Response(429), httpx.Response(503), httpx.Response(200, json=page(["W1"], None))])

    pages = list(make_client(lambda request: next(calls)).iter_pages(date(2026, 10, 1), date(2026, 10, 1)))

    assert len(pages) == 1


def test_stops_when_daily_budget_is_spent():
    def handler(request):
        return httpx.Response(429, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "100"})

    with pytest.raises(BudgetExhausted):
        list(make_client(handler).iter_pages(date(2026, 10, 1), date(2026, 10, 1)))


def test_client_errors_are_not_retried():
    attempts = []

    def handler(request):
        attempts.append(1)
        return httpx.Response(403, json={"error": "key_not_activated"})

    with pytest.raises(httpx.HTTPStatusError):
        list(make_client(handler).iter_pages(date(2026, 10, 1), date(2026, 10, 1)))
    assert len(attempts) == 1
