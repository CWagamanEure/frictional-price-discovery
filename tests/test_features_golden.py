"""Golden regression test for engineered features."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.features import compute_features


def _load_fixture(name: str) -> list[dict[str, object]]:
    path = Path("tests/fixtures") / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_features_match_golden_snapshot() -> None:
    input_rows = _load_fixture("features_input.json")
    expected_rows = _load_fixture("features_expected.json")

    out = compute_features(
        input_rows,
        realized_vol_window=1,
        annualization_minutes=1,
    )

    keys = [
        "minute_utc",
        "basis_5_bps",
        "basis_30_bps",
        "basis_spread_bps",
        "implied_band_5_bps",
        "implied_band_30_bps",
        "violation_5",
        "violation_30",
        "violation_5_mag_bps",
        "violation_30_mag_bps",
        "realized_vol_annualized",
    ]

    for got, expected in zip(out, expected_rows, strict=True):
        for key in keys:
            if expected[key] is None:
                assert got[key] is None
            elif isinstance(expected[key], bool):
                assert got[key] is expected[key]
            elif isinstance(expected[key], str):
                assert got[key] == expected[key]
            else:
                assert got[key] == pytest.approx(float(expected[key]))
