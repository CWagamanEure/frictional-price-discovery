"""End-to-end processed pipeline orchestration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.export import ExportResult, export_records
from ingestion.features import compute_features
from ingestion.reporting import build_missingness_report, write_missingness_report
from ingestion.validation import ValidationError, enforce_validation


@dataclass(frozen=True)
class ProcessedRunResult:
    """Artifacts and summary from a processed pipeline run."""

    feature_json_path: str
    report_json_path: str
    parquet_path: str
    metadata_path: str
    validation_issue_count: int


def _extract_window(records: list[dict[str, Any]]) -> tuple[datetime, datetime]:
    if not records:
        now = datetime.now(UTC)
        return now, now

    minutes: list[datetime] = []
    for row in records:
        value = row.get("minute_utc")
        if isinstance(value, str):
            minutes.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
        elif isinstance(value, datetime):
            minutes.append(value)

    if not minutes:
        now = datetime.now(UTC)
        return now, now

    start = min(minutes)
    end = max(minutes)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    return start.astimezone(UTC), end.astimezone(UTC)


def run_processed_pipeline(
    aligned_records: list[dict[str, Any]],
    *,
    output_dir: str = "data/processed",
    dataset_name: str = "features",
    realized_vol_window: int = 30,
    annualization_minutes: int = 525_600,
    gas_weight_bps_per_gwei: float = 0.02,
    fail_on_warnings: bool = False,
) -> ProcessedRunResult:
    """Compute features, validate output, emit report, and export parquet."""
    processed_dir = Path(output_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    feature_rows = compute_features(
        aligned_records,
        realized_vol_window=realized_vol_window,
        annualization_minutes=annualization_minutes,
        gas_weight_bps_per_gwei=gas_weight_bps_per_gwei,
    )

    issues = enforce_validation(
        feature_rows,
        timestamp_key="minute_utc",
        required_columns={"minute_utc"},
        numeric_ranges={
            "coinbase_close": (0.0, None),
        },
        warning_numeric_ranges={
            "basis_5_bps": (-10_000.0, 10_000.0),
            "basis_30_bps": (-10_000.0, 10_000.0),
            "realized_vol_annualized": (0.0, None),
        },
        warning_missing_thresholds={
            "coinbase_close": 0.2,
            "basis_5_bps": 0.2,
            "basis_30_bps": 0.2,
        },
        fail_on_warnings=fail_on_warnings,
    )

    report = build_missingness_report(
        feature_rows,
        expected_columns={
            "minute_utc",
            "coinbase_close",
            "basis_5_bps",
            "basis_30_bps",
        },
    )
    report["validation_issues"] = [
        {"severity": issue.severity, "code": issue.code, "message": issue.message}
        for issue in issues
    ]

    start_time_utc, end_time_utc = _extract_window(feature_rows)
    export_result: ExportResult = export_records(
        feature_rows,
        output_dir=str(processed_dir),
        dataset_name=dataset_name,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        config={
            "realized_vol_window": realized_vol_window,
            "annualization_minutes": annualization_minutes,
            "gas_weight_bps_per_gwei": gas_weight_bps_per_gwei,
            "fail_on_warnings": fail_on_warnings,
        },
    )

    run_tag = Path(export_result.parquet_path).stem
    feature_json_path = processed_dir / f"{run_tag}.features.json"
    report_json_path = processed_dir / f"{run_tag}.missingness.json"

    feature_json_path.write_text(json.dumps(feature_rows, indent=2), encoding="utf-8")
    write_missingness_report(str(report_json_path), report)

    return ProcessedRunResult(
        feature_json_path=str(feature_json_path),
        report_json_path=str(report_json_path),
        parquet_path=export_result.parquet_path,
        metadata_path=export_result.metadata_path,
        validation_issue_count=len(issues),
    )


__all__ = [
    "ProcessedRunResult",
    "run_processed_pipeline",
    "ValidationError",
]
