"""Tests for Uniswap Graph response parsing."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.sources.uniswap_graph import GraphAPIError, parse_pool_minute_page


def test_parse_pool_minute_page_maps_types_and_fields() -> None:
    payload = {
        "data": {
            "poolMinuteDatas": [
                {
                    "periodStartUnix": 1735689600,
                    "token0Price": "3500.1",
                    "token1Price": "0.0002857",
                    "volumeUSD": "1000.5",
                    "tvlUSD": "250000.0",
                }
            ]
        }
    }

    rows = parse_pool_minute_page(payload, pool_id="0xPool", fee_tier_bps=30)

    assert len(rows) == 1
    row = rows[0]
    assert row.timestamp_utc == datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert row.pool_id == "0xpool"
    assert row.fee_tier_bps == 30
    assert row.token0_price == 3500.1
    assert row.token1_price == 0.0002857
    assert row.volume_usd == 1000.5
    assert row.tvl_usd == 250000.0


def test_parse_pool_minute_page_rejects_bad_shape() -> None:
    with pytest.raises(ValueError):
        parse_pool_minute_page({"data": {}}, pool_id="0xpool", fee_tier_bps=5)


def test_parse_pool_minute_page_raises_on_graph_errors() -> None:
    payload = {"errors": [{"message": "rate limited"}], "data": {}}

    with pytest.raises(GraphAPIError):
        parse_pool_minute_page(payload, pool_id="0xpool", fee_tier_bps=5)
