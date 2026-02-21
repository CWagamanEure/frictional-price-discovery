"""Tests for Coinbase schema normalization."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.sources.coinbase import CoinbaseAPIError, parse_candle_rows


def test_parse_candle_rows_normalizes_schema_and_sorts() -> None:
    payload = [
        [1735689660, "3499.0", "3502.0", "3500.0", "3501.0", "1.23"],
        [1735689600, "3490.0", "3498.0", "3495.0", "3497.0", "2.00"],
    ]

    rows = parse_candle_rows(payload, product_id="ETH-USD", interval_seconds=60)

    assert [row.timestamp_utc for row in rows] == [
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
    ]
    row = rows[0]
    assert row.product_id == "ETH-USD"
    assert row.interval_seconds == 60
    assert row.open_price == 3495.0
    assert row.high_price == 3498.0
    assert row.low_price == 3490.0
    assert row.close_price == 3497.0
    assert row.volume == 2.0


def test_parse_candle_rows_handles_empty_payload() -> None:
    rows = parse_candle_rows([], product_id="ETH-USD", interval_seconds=60)
    assert rows == []


def test_parse_candle_rows_rejects_malformed_payload() -> None:
    with pytest.raises(CoinbaseAPIError):
        parse_candle_rows(
            payload={"bad": "shape"},
            product_id="ETH-USD",
            interval_seconds=60,
        )

    with pytest.raises(CoinbaseAPIError):
        parse_candle_rows(
            payload=[[1735689600, 1, 2]],
            product_id="ETH-USD",
            interval_seconds=60,
        )
