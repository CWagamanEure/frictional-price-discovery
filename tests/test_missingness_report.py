"""Tests for missingness report and gap calculations."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.reporting import build_missingness_report, write_missingness_report


def test_missingness_report_percentages_and_gap_lengths() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": None},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": None},
        {"minute_utc": "2025-01-01T00:02:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:03:00Z", "coinbase_close": None},
    ]

    report = build_missingness_report(rows)
    coinbase = report["per_column"]["coinbase_close"]

    assert report["total_rows"] == 4
    assert coinbase["missing_count"] == 3
    assert coinbase["missing_rate"] == 0.75
    assert coinbase["max_consecutive_missing"] == 2


def test_write_missingness_report_json(tmp_path: Path) -> None:
    report = {
        "total_rows": 1,
        "column_count": 1,
        "per_column": {"coinbase_close": {"missing_count": 0, "missing_rate": 0.0}},
    }
    output = tmp_path / "missingness.json"
    write_missingness_report(str(output), report)

    loaded = json.loads(output.read_text(encoding="utf-8"))
    assert loaded["total_rows"] == 1
    assert "coinbase_close" in loaded["per_column"]
