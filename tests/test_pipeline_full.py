"""Tests for full pipeline orchestration and quality gates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.pipeline_full import evaluate_alignment_quality, run_full_pipeline
from ingestion.validation import ValidationError


@dataclass(frozen=True)
class _FakeRawResult:
    run_id: str
    files: dict[str, str]
    row_counts: dict[str, int]


@dataclass(frozen=True)
class _FakeProcessedResult:
    dataset_json_path: str
    report_json_path: str
    parquet_path: str
    metadata_path: str
    validation_issue_count: int


def test_evaluate_alignment_quality_flags_coverage_and_staleness() -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "uniswap5_token0_price": 100.0,
            "uniswap5_age_since_last_trade_min": 0,
            "uniswap30_token0_price": None,
            "uniswap30_age_since_last_trade_min": None,
        },
        {
            "minute_utc": "2025-01-01T00:01:00Z",
            "uniswap5_token0_price": 100.0,
            "uniswap5_age_since_last_trade_min": 120,
            "uniswap30_token0_price": None,
            "uniswap30_age_since_last_trade_min": None,
        },
    ]

    metrics, issues = evaluate_alignment_quality(
        rows,
        min_uniswap5_coverage=1.0,
        min_uniswap30_coverage=0.5,
        staleness_threshold_minutes=60,
        max_uniswap5_stale_share=0.2,
        max_uniswap30_stale_share=1.0,
    )

    assert metrics["total_minutes"] == 2
    assert metrics["coverage"]["uniswap30"] == 0.0
    codes = {issue["code"] for issue in issues}
    assert "low_uniswap30_coverage" in codes
    assert "high_uniswap5_staleness" in codes


def test_run_full_pipeline_writes_summary(monkeypatch, tmp_path: Path) -> None:
    run_log = tmp_path / "raw_ingestion_run_fake.json"
    run_log.write_text("{}", encoding="utf-8")

    aligned_path = tmp_path / "aligned.json"
    aligned_path.write_text(
        json.dumps(
            [
                {
                    "minute_utc": "2025-01-01T00:00:00Z",
                    "coinbase_close": 100.0,
                    "uniswap5_token0_price": 100.1,
                    "uniswap30_token0_price": 100.2,
                    "uniswap5_age_since_last_trade_min": 0,
                    "uniswap30_age_since_last_trade_min": 0,
                }
            ]
        ),
        encoding="utf-8",
    )

    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(
        json.dumps(
            [
                {
                    "minute_utc": "2025-01-01T00:00:00Z",
                    "coinbase_close": 100.0,
                    "uniswap5_token0_price": 100.1,
                    "uniswap30_token0_price": 100.2,
                    "realized_vol_annualized": None,
                }
            ]
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"
    report_path.write_text("{}", encoding="utf-8")
    parquet_path = tmp_path / "features.parquet"
    parquet_path.write_text("", encoding="utf-8")
    metadata_path = tmp_path / "features.metadata.json"
    metadata_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "ingestion.pipeline_full.run_raw_ingestion",
        lambda **kwargs: _FakeRawResult(
            run_id="fake_run",
            files={"run_log": str(run_log)},
            row_counts={"coinbase": 1},
        ),
    )
    monkeypatch.setattr(
        "ingestion.pipeline_full.build_aligned_from_raw_run",
        lambda **kwargs: str(aligned_path),
    )
    monkeypatch.setattr(
        "ingestion.pipeline_full.run_processed_pipeline",
        lambda *args, **kwargs: _FakeProcessedResult(
            dataset_json_path=str(dataset_path),
            report_json_path=str(report_path),
            parquet_path=str(parquet_path),
            metadata_path=str(metadata_path),
            validation_issue_count=0,
        ),
    )

    result = run_full_pipeline(
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
        processed_output_dir=str(tmp_path),
    )

    summary_path = Path(result.summary_json_path)
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["raw_run_id"] == "fake_run"
    assert summary["dataset_summary"]["row_count"] == 1
    assert summary["artifacts"]["dataset_json"] == str(dataset_path)


def test_run_full_pipeline_quality_gate_fail(monkeypatch, tmp_path: Path) -> None:
    run_log = tmp_path / "raw_ingestion_run_fake.json"
    run_log.write_text("{}", encoding="utf-8")

    aligned_path = tmp_path / "aligned.json"
    aligned_path.write_text(
        json.dumps([{"minute_utc": "2025-01-01T00:00:00Z"}]), encoding="utf-8"
    )

    monkeypatch.setattr(
        "ingestion.pipeline_full.run_raw_ingestion",
        lambda **kwargs: _FakeRawResult(
            run_id="fake_run",
            files={"run_log": str(run_log)},
            row_counts={},
        ),
    )
    monkeypatch.setattr(
        "ingestion.pipeline_full.build_aligned_from_raw_run",
        lambda **kwargs: str(aligned_path),
    )

    with pytest.raises(ValidationError):
        run_full_pipeline(
            start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            end_time_utc=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            fail_on_quality_warnings=True,
            min_uniswap5_coverage=1.0,
            min_uniswap30_coverage=1.0,
        )
