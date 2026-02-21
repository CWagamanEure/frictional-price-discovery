"""Invariant checks for engineered feature outputs."""

from __future__ import annotations

from ingestion.features import compute_features


def test_violation_magnitude_zero_when_not_violating() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.01,
            "uniswap30_token0_price": 100.02,
            "gas_base_fee_per_gas_wei": 30_000_000_000,
        },
        {
            "minute_utc": "2025-01-01T00:01:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.4,
            "uniswap30_token0_price": 100.6,
            "gas_base_fee_per_gas_wei": 30_000_000_000,
        },
    ]

    out = compute_features(rows, realized_vol_window=1, annualization_minutes=1)

    for row in out:
        if not row["violation_5"]:
            assert row["violation_5_mag_bps"] == 0.0
        if not row["violation_30"]:
            assert row["violation_30_mag_bps"] == 0.0


def test_realized_vol_non_negative_when_present() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 99.0},
        {"minute_utc": "2025-01-01T00:02:00Z", "coinbase_close": 101.0},
    ]

    out = compute_features(rows, realized_vol_window=2, annualization_minutes=1)

    for row in out:
        vol = row["realized_vol_annualized"]
        if vol is not None:
            assert vol >= 0.0
