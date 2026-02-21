"""Tests for end-to-end processed pipeline orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.pipeline_processed import ValidationError, run_processed_pipeline


def _aligned_rows() -> list[dict[str, object]]:
    return [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.1,
            "uniswap30_token0_price": 99.8,
            "gas_base_fee_per_gas_wei": 20_000_000_000,
        },
        {
            "minute_utc": "2025-01-01T00:01:00Z",
            "coinbase_close": 101.0,
            "uniswap5_token0_price": 101.1,
            "uniswap30_token0_price": 100.9,
            "gas_base_fee_per_gas_wei": 20_000_000_000,
        },
    ]


def test_run_processed_pipeline_writes_artifacts(tmp_path: Path) -> None:
    result = run_processed_pipeline(
        _aligned_rows(),
        output_dir=str(tmp_path),
        dataset_name="features",
        realized_vol_window=1,
        annualization_minutes=1,
        fail_on_warnings=False,
    )

    assert Path(result.feature_json_path).exists()
    assert Path(result.report_json_path).exists()
    assert Path(result.parquet_path).exists()
    assert Path(result.metadata_path).exists()

    report = json.loads(Path(result.report_json_path).read_text(encoding="utf-8"))
    assert "validation_issues" in report


def test_run_processed_pipeline_respects_fail_on_warnings(tmp_path: Path) -> None:
    rows = _aligned_rows()
    rows[0]["coinbase_close"] = None
    rows[1]["coinbase_close"] = None

    with pytest.raises(ValidationError):
        run_processed_pipeline(
            rows,
            output_dir=str(tmp_path),
            dataset_name="features",
            realized_vol_window=1,
            annualization_minutes=1,
            fail_on_warnings=True,
        )
