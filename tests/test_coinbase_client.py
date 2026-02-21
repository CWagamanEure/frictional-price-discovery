"""Tests for Coinbase client request/retry behavior."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch
from urllib import error

import pytest

from ingestion.sources.coinbase import UrllibCoinbaseClient, fetch_coinbase_candles


class _FakeResponse:
    def __init__(self, payload: str) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload.encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        return False


class FakeCoinbaseClient:
    """Fake client for fetch_coinbase_candles unit tests."""

    def __init__(self, payload: list[list[float | int]]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get_json(self, path: str, query_params: Mapping[str, str]) -> Any:
        self.calls.append((path, dict(query_params)))
        return self.payload


def _http_error(code: int) -> error.HTTPError:
    return error.HTTPError(
        url="https://api.exchange.coinbase.com",
        code=code,
        msg=f"HTTP {code}",
        hdrs=None,
        fp=None,
    )


def test_fetch_coinbase_candles_builds_request_and_parses() -> None:
    client = FakeCoinbaseClient(payload=[[1735689600, 1, 2, 3, 4, 5]])

    rows = fetch_coinbase_candles(
        client,
        product_id="ETH-USD",
        interval_seconds=60,
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert len(rows) == 1
    path, query = client.calls[0]
    assert path == "products/ETH-USD/candles"
    assert query["granularity"] == "60"
    assert query["start"] == "2025-01-01T00:00:00Z"
    assert query["end"] == "2025-01-01T00:01:00Z"


def test_fetch_coinbase_candles_chunks_large_windows() -> None:
    class ChunkAwareFakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get_json(self, path: str, query_params: Mapping[str, str]) -> Any:
            del path
            self.calls.append(dict(query_params))
            start = datetime.fromisoformat(
                query_params["start"].replace("Z", "+00:00")
            ).astimezone(UTC)
            unix_ts = int(start.timestamp())
            return [[unix_ts, 1, 2, 3, 4, 5]]

    client = ChunkAwareFakeClient()
    rows = fetch_coinbase_candles(
        client,
        product_id="ETH-USD",
        interval_seconds=60,
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
    )

    assert len(client.calls) > 1
    assert len(rows) == len(client.calls)
    assert rows[0].timestamp_utc < rows[-1].timestamp_utc


def test_urllib_coinbase_client_retries_on_429_then_succeeds() -> None:
    calls = {"count": 0}

    def _fake_urlopen(*args: Any, **kwargs: Any) -> _FakeResponse:
        del args, kwargs
        calls["count"] += 1
        if calls["count"] == 1:
            raise _http_error(429)
        return _FakeResponse("[]")

    client = UrllibCoinbaseClient(max_retries=3, retry_backoff_seconds=0)

    with patch(
        "ingestion.sources.coinbase.request.urlopen",
        side_effect=_fake_urlopen,
    ):
        payload = client.get_json("products/ETH-USD/candles", {"granularity": "60"})

    assert payload == []
    assert calls["count"] == 2


def test_urllib_coinbase_client_retries_on_5xx_then_fails() -> None:
    def _fake_urlopen(*args: Any, **kwargs: Any) -> _FakeResponse:
        del args, kwargs
        raise _http_error(503)

    client = UrllibCoinbaseClient(max_retries=2, retry_backoff_seconds=0)

    with patch(
        "ingestion.sources.coinbase.request.urlopen",
        side_effect=_fake_urlopen,
    ):
        with pytest.raises(error.HTTPError):
            client.get_json("products/ETH-USD/candles", {"granularity": "60"})
