"""Tests for Uniswap Graph client pagination and fee-tier fetching."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch
from urllib import error

import pytest

from ingestion.sources.uniswap_graph import (
    GRAPH_GATEWAY_BASE_URL,
    MAINNET_NETWORK_LABEL,
    UrllibGraphClient,
    fetch_pool_minutes,
    fetch_two_fee_tiers,
    resolve_graph_endpoint,
)


class FakeGraphClient:
    """Simple fake Graph client with deterministic paged responses."""

    def __init__(self, pages_by_skip: Mapping[int, list[dict[str, Any]]]) -> None:
        self.pages_by_skip = dict(pages_by_skip)
        self.calls: list[dict[str, Any]] = []

    def post_json(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        del query
        self.calls.append(dict(variables))
        skip = int(variables["skip"])
        return {"data": {"poolMinuteDatas": self.pages_by_skip.get(skip, [])}}


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


def _row(unix_ts: int, token0_price: str = "1.0") -> dict[str, str | int]:
    return {
        "periodStartUnix": unix_ts,
        "token0Price": token0_price,
        "token1Price": "1.0",
        "volumeUSD": "10.0",
        "tvlUSD": "100.0",
    }


def test_fetch_pool_minutes_paginates_and_stops_on_empty_page() -> None:
    client = FakeGraphClient(
        {
            0: [_row(1735689720), _row(1735689660)],
            2: [_row(1735689780), _row(1735689840)],
        }
    )

    rows = fetch_pool_minutes(
        client,
        pool_id="0xPool",
        fee_tier_bps=30,
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 10, tzinfo=UTC),
        page_size=2,
    )

    assert [r.timestamp_utc for r in rows] == sorted(r.timestamp_utc for r in rows)
    assert len(rows) == 4
    assert [call["skip"] for call in client.calls] == [0, 2, 4]


def test_fetch_two_fee_tiers_includes_both_tiers() -> None:
    client = FakeGraphClient(
        {
            0: [_row(1735689600)],
            1: [_row(1735689660)],
        }
    )

    rows = fetch_two_fee_tiers(
        client,
        pools_by_fee_tier_bps={5: "0xpool5", 30: "0xpool30"},
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 2, tzinfo=UTC),
        page_size=1,
    )

    assert {r.fee_tier_bps for r in rows} == {5, 30}
    assert {r.pool_id for r in rows} == {"0xpool5", "0xpool30"}


def test_urllib_client_retries_transient_error_then_succeeds() -> None:
    calls = {"count": 0}

    def _fake_urlopen(*args: Any, **kwargs: Any) -> _FakeResponse:
        del args, kwargs
        calls["count"] += 1
        if calls["count"] < 2:
            raise error.URLError("temporary")
        return _FakeResponse('{"data": {"poolMinuteDatas": []}}')

    client = UrllibGraphClient(
        endpoint="https://example.com/subgraphs/id/mainnet",
        max_retries=3,
        retry_backoff_seconds=0,
    )

    with patch(
        "ingestion.sources.uniswap_graph.request.urlopen",
        side_effect=_fake_urlopen,
    ):
        payload = client.post_json("query", {"pool": "0xpool"})

    assert calls["count"] == 2
    assert payload["data"]["poolMinuteDatas"] == []


def test_urllib_client_raises_after_retry_exhaustion() -> None:
    def _fake_urlopen(*args: Any, **kwargs: Any) -> _FakeResponse:
        del args, kwargs
        raise error.URLError("down")

    client = UrllibGraphClient(
        endpoint="https://example.com/subgraphs/id/mainnet",
        max_retries=2,
        retry_backoff_seconds=0,
    )

    with patch(
        "ingestion.sources.uniswap_graph.request.urlopen",
        side_effect=_fake_urlopen,
    ):
        with pytest.raises(error.URLError):
            client.post_json("query", {"pool": "0xpool"})


def test_resolve_graph_endpoint_uses_explicit_endpoint() -> None:
    endpoint = resolve_graph_endpoint(endpoint="https://example.com/custom")
    assert endpoint == "https://example.com/custom"


def test_resolve_graph_endpoint_builds_gateway_url_for_mainnet() -> None:
    endpoint = resolve_graph_endpoint(api_key="abc", subgraph_id="subgraph123")
    assert endpoint == f"{GRAPH_GATEWAY_BASE_URL}/abc/subgraphs/id/subgraph123"
    assert MAINNET_NETWORK_LABEL == "ethereum-mainnet"
    assert "arbitrum" not in endpoint.lower()


def test_resolve_graph_endpoint_requires_inputs() -> None:
    with pytest.raises(ValueError):
        resolve_graph_endpoint()
