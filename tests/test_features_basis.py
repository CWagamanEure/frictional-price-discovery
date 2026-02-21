"""Feature tests for basis and implied cost-band proxies."""

from __future__ import annotations

import pytest

from ingestion.features import compute_features


def test_basis_and_cost_band_formulas() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.1,
            "uniswap30_token0_price": 99.7,
            "gas_base_fee_per_gas_wei": 30_000_000_000,
        }
    ]

    out = compute_features(rows, realized_vol_window=1, annualization_minutes=1)
    row = out[0]

    assert row["basis_5_bps"] == pytest.approx(10.0)
    assert row["basis_30_bps"] == pytest.approx(-30.0)
    assert row["basis_spread_bps"] == pytest.approx(-40.0)
    assert row["implied_band_5_bps"] == pytest.approx(5.6)
    assert row["implied_band_30_bps"] == pytest.approx(30.6)


def test_basis_handles_invalid_prices() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 0.0,
            "uniswap5_token0_price": 100.1,
            "uniswap30_token0_price": 99.7,
        }
    ]

    out = compute_features(rows)
    row = out[0]

    assert row["basis_5_bps"] is None
    assert row["basis_30_bps"] is None
    assert row["basis_spread_bps"] is None
