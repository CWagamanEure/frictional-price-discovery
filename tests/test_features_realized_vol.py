"""Feature tests for realized volatility behavior."""

from __future__ import annotations

import math
import statistics

import pytest

from ingestion.features import compute_features


def test_realized_vol_window_boundaries() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 101.0},
        {"minute_utc": "2025-01-01T00:02:00Z", "coinbase_close": 102.0},
        {"minute_utc": "2025-01-01T00:03:00Z", "coinbase_close": 104.0},
    ]

    out = compute_features(rows, realized_vol_window=3, annualization_minutes=1)

    assert out[0]["realized_vol_annualized"] is None
    assert out[1]["realized_vol_annualized"] is None
    assert out[2]["realized_vol_annualized"] is None

    returns = [
        math.log(101.0 / 100.0),
        math.log(102.0 / 101.0),
        math.log(104.0 / 102.0),
    ]
    expected = statistics.pstdev(returns)
    assert out[3]["realized_vol_annualized"] == pytest.approx(expected)


def test_realized_vol_invalid_price_returns_none() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 0.0},
    ]

    out = compute_features(rows, realized_vol_window=1, annualization_minutes=1)

    assert out[0]["realized_vol_annualized"] is None
    assert out[1]["realized_vol_annualized"] is None
