"""Feature tests for violation indicators and magnitudes."""

from __future__ import annotations

import pytest

from ingestion.features import compute_features


def test_violation_indicator_and_magnitude() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.2,
            "uniswap30_token0_price": 100.25,
            "gas_base_fee_per_gas_wei": 20_000_000_000,
        }
    ]

    out = compute_features(rows, realized_vol_window=1, annualization_minutes=1)
    row = out[0]

    assert row["basis_5_bps"] == pytest.approx(20.0)
    assert row["implied_band_5_bps"] == pytest.approx(5.4)
    assert row["violation_5"] is True
    assert row["violation_5_mag_bps"] == pytest.approx(14.6)

    assert row["basis_30_bps"] == pytest.approx(25.0)
    assert row["implied_band_30_bps"] == pytest.approx(30.4)
    assert row["violation_30"] is False
    assert row["violation_30_mag_bps"] == pytest.approx(0.0)


def test_violation_defaults_on_missing_basis() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": None,
            "uniswap5_token0_price": 100.0,
            "uniswap30_token0_price": 100.0,
        }
    ]

    out = compute_features(rows)
    row = out[0]

    assert row["violation_5"] is False
    assert row["violation_30"] is False
    assert row["violation_5_mag_bps"] == 0.0
    assert row["violation_30_mag_bps"] == 0.0
