"""End-to-end dataset export orchestration from aligned records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.dataset_builder import build_dataset_rows
from ingestion.export import ExportResult, export_records
from ingestion.reporting import build_missingness_report, write_missingness_report
from ingestion.validation import ValidationError, enforce_validation


@dataclass(frozen=True)
class ProcessedRunResult:
    """Artifacts and summary from a processed dataset export run."""

    dataset_json_path: str
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
    dataset_name: str = "aligned_dataset",
    realized_vol_window: int = 30,
    annualization_minutes: int = 525_600,
    fail_on_warnings: bool = False,
) -> ProcessedRunResult:
    """Shape dataset, validate output, emit report, and export parquet."""
    processed_dir = Path(output_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    dataset_rows = build_dataset_rows(
        aligned_records,
        realized_vol_window=realized_vol_window,
        annualization_minutes=annualization_minutes,
    )

    issues = enforce_validation(
        dataset_rows,
        timestamp_key="minute_utc",
        required_columns={"minute_utc"},
        numeric_ranges={
            "coinbase_close": (0.0, None),
        },
        warning_numeric_ranges={
            "uniswap5_token0_price": (0.0, None),
            "uniswap30_token0_price": (0.0, None),
            "uniswap5_flow_usd": (0.0, None),
            "uniswap30_flow_usd": (0.0, None),
            "uniswap5_swap_count": (0.0, None),
            "uniswap30_swap_count": (0.0, None),
            "coinbase_volume": (0.0, None),
            "gas_base_fee_per_gas_wei": (0.0, None),
            "gas_base_fee_gwei": (0.0, None),
            "gas_usd": (0.0, None),
            "congestion_30d_pct": (0.0, 1.0),
            "realized_vol_annualized": (0.0, None),
        },
        warning_missing_thresholds={
            "coinbase_close": 0.2,
            "uniswap5_token0_price": 0.2,
            "uniswap30_token0_price": 0.95,
        },
        fail_on_warnings=fail_on_warnings,
    )

    report = build_missingness_report(
        dataset_rows,
        expected_columns={
            "minute_utc",
            "coinbase_close",
            "coinbase_volume",
            "coinbase_log_price",
            "coinbase_log_return",
            "uniswap5_token0_price",
            "uniswap30_token0_price",
            "uniswap5_log_price",
            "uniswap30_log_price",
            "uniswap5_log_return",
            "uniswap30_log_return",
            "wedge_5_price_diff",
            "wedge_30_price_diff",
            "wedge_5_bps",
            "wedge_30_bps",
            "gas_base_fee_gwei",
            "gas_usd",
            "congestion_30d_pct",
            "uniswap5_flow_usd",
            "uniswap30_flow_usd",
            "uniswap5_swap_count",
            "uniswap30_swap_count",
            "gas_base_fee_per_gas_wei",
            "uniswap5_age_since_last_trade_min",
            "uniswap30_age_since_last_trade_min",
            "uniswap5_fee_tier_bps",
            "uniswap30_fee_tier_bps",
            "realized_vol_annualized",
        },
    )
    report["validation_issues"] = [
        {"severity": issue.severity, "code": issue.code, "message": issue.message}
        for issue in issues
    ]

    start_time_utc, end_time_utc = _extract_window(dataset_rows)
    export_result: ExportResult = export_records(
        dataset_rows,
        output_dir=str(processed_dir),
        dataset_name=dataset_name,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        config={
            "realized_vol_window": realized_vol_window,
            "annualization_minutes": annualization_minutes,
            "fail_on_warnings": fail_on_warnings,
        },
    )

    run_tag = Path(export_result.parquet_path).stem
    dataset_json_path = processed_dir / f"{run_tag}.dataset.json"
    report_json_path = processed_dir / f"{run_tag}.missingness.json"

    dataset_json_path.write_text(json.dumps(dataset_rows, indent=2), encoding="utf-8")
    write_missingness_report(str(report_json_path), report)

    return ProcessedRunResult(
        dataset_json_path=str(dataset_json_path),
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
