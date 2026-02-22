"""Full pipeline orchestration: raw ingest -> align -> dataset export + quality gates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.pipeline_align import build_aligned_from_raw_run
from ingestion.pipeline_processed import ProcessedRunResult, run_processed_pipeline
from ingestion.pipeline_raw import RawIngestionResult, run_raw_ingestion
from ingestion.validation import ValidationError


@dataclass(frozen=True)
class FullRunResult:
    """Artifacts and summary for one end-to-end run."""

    raw_result: RawIngestionResult
    aligned_json_path: str
    processed_result: ProcessedRunResult
    summary_json_path: str
    quality_issue_count: int


def _read_json_list(path: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list payload in {path}")
    rows: list[dict[str, Any]] = []
    for row in payload:
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def evaluate_alignment_quality(
    aligned_records: list[dict[str, Any]],
    *,
    min_uniswap5_coverage: float = 0.9,
    min_uniswap30_coverage: float = 0.05,
    staleness_threshold_minutes: int = 60,
    max_uniswap5_stale_share: float = 0.5,
    max_uniswap30_stale_share: float = 0.95,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Compute coverage/staleness metrics and return warning-like issues."""
    total = len(aligned_records)
    if total == 0:
        metrics = {
            "total_minutes": 0,
            "coverage": {"uniswap5": 0.0, "uniswap30": 0.0},
            "staleness": {"threshold_minutes": staleness_threshold_minutes},
        }
        issues = [
            {
                "severity": "warning",
                "code": "empty_aligned_records",
                "message": "no rows",
            }
        ]
        return metrics, issues

    uni5_present = sum(
        1
        for row in aligned_records
        if _to_float(row.get("uniswap5_token0_price")) is not None
    )
    uni30_present = sum(
        1
        for row in aligned_records
        if _to_float(row.get("uniswap30_token0_price")) is not None
    )
    uni5_cov = uni5_present / total
    uni30_cov = uni30_present / total

    uni5_stale = sum(
        1
        for row in aligned_records
        if (_to_float(row.get("uniswap5_age_since_last_trade_min")) or -1)
        > staleness_threshold_minutes
    )
    uni30_stale = sum(
        1
        for row in aligned_records
        if (_to_float(row.get("uniswap30_age_since_last_trade_min")) or -1)
        > staleness_threshold_minutes
    )
    uni5_stale_share = uni5_stale / total
    uni30_stale_share = uni30_stale / total

    metrics = {
        "total_minutes": total,
        "coverage": {
            "uniswap5": uni5_cov,
            "uniswap30": uni30_cov,
            "minimums": {
                "uniswap5": min_uniswap5_coverage,
                "uniswap30": min_uniswap30_coverage,
            },
        },
        "staleness": {
            "threshold_minutes": staleness_threshold_minutes,
            "share_over_threshold": {
                "uniswap5": uni5_stale_share,
                "uniswap30": uni30_stale_share,
            },
            "maximums": {
                "uniswap5": max_uniswap5_stale_share,
                "uniswap30": max_uniswap30_stale_share,
            },
        },
    }

    issues: list[dict[str, str]] = []
    if uni5_cov < min_uniswap5_coverage:
        issues.append(
            {
                "severity": "warning",
                "code": "low_uniswap5_coverage",
                "message": (
                    f"uniswap5 coverage {uni5_cov:.3f} below minimum "
                    f"{min_uniswap5_coverage:.3f}"
                ),
            }
        )
    if uni30_cov < min_uniswap30_coverage:
        issues.append(
            {
                "severity": "warning",
                "code": "low_uniswap30_coverage",
                "message": (
                    f"uniswap30 coverage {uni30_cov:.3f} below minimum "
                    f"{min_uniswap30_coverage:.3f}"
                ),
            }
        )
    if uni5_stale_share > max_uniswap5_stale_share:
        issues.append(
            {
                "severity": "warning",
                "code": "high_uniswap5_staleness",
                "message": (
                    f"uniswap5 stale share {uni5_stale_share:.3f} above max "
                    f"{max_uniswap5_stale_share:.3f}"
                ),
            }
        )
    if uni30_stale_share > max_uniswap30_stale_share:
        issues.append(
            {
                "severity": "warning",
                "code": "high_uniswap30_staleness",
                "message": (
                    f"uniswap30 stale share {uni30_stale_share:.3f} above max "
                    f"{max_uniswap30_stale_share:.3f}"
                ),
            }
        )

    return metrics, issues


def _dataset_summary(dataset_rows: list[dict[str, Any]]) -> dict[str, Any]:
    columns: set[str] = set()
    for row in dataset_rows:
        columns.update(row.keys())
    realized_vol_non_null = sum(
        1 for row in dataset_rows if _to_float(row.get("realized_vol_annualized")) is not None
    )
    return {
        "row_count": len(dataset_rows),
        "column_count": len(columns),
        "realized_vol_non_null_count": realized_vol_non_null,
    }


def run_full_pipeline(
    *,
    start_time_utc: datetime,
    end_time_utc: datetime,
    raw_output_dir: str = "data/raw",
    interim_output_json: str = "data/interim/aligned_records.json",
    processed_output_dir: str = "data/processed",
    dataset_name: str = "aligned_dataset",
    graph_endpoint: str | None = None,
    graph_api_key: str | None = None,
    graph_subgraph_id: str | None = None,
    uniswap_pool_5_bps: str | None = None,
    uniswap_pool_30_bps: str | None = None,
    coinbase_product_id: str = "ETH-USD",
    coinbase_interval_seconds: int = 60,
    coinbase_base_url: str | None = None,
    rpc_url: str | None = None,
    rpc_mode: str = "auto",
    rpc_feehistory_blocks_per_request: int = 1024,
    rpc_progress_every_blocks: int = 1000,
    raw_format: str = "parquet",
    realized_vol_window: int = 30,
    annualization_minutes: int = 525_600,
    fail_on_warnings: bool = False,
    min_uniswap5_coverage: float = 0.9,
    min_uniswap30_coverage: float = 0.05,
    staleness_threshold_minutes: int = 60,
    max_uniswap5_stale_share: float = 0.5,
    max_uniswap30_stale_share: float = 0.95,
    fail_on_quality_warnings: bool = False,
) -> FullRunResult:
    """Run all stages and emit quality summary report."""
    raw_result = run_raw_ingestion(
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        output_dir=raw_output_dir,
        graph_endpoint=graph_endpoint,
        graph_api_key=graph_api_key,
        graph_subgraph_id=graph_subgraph_id,
        uniswap_pool_5_bps=uniswap_pool_5_bps,
        uniswap_pool_30_bps=uniswap_pool_30_bps,
        coinbase_product_id=coinbase_product_id,
        coinbase_interval_seconds=coinbase_interval_seconds,
        coinbase_base_url=coinbase_base_url,
        rpc_url=rpc_url,
        rpc_mode=rpc_mode,
        rpc_feehistory_blocks_per_request=rpc_feehistory_blocks_per_request,
        rpc_progress_every_blocks=rpc_progress_every_blocks,
        raw_format=raw_format,
    )

    aligned_json_path = build_aligned_from_raw_run(
        raw_run_log_path=raw_result.files.get("run_log"),
        output_json_path=interim_output_json,
    )
    aligned_records = _read_json_list(aligned_json_path)
    quality_metrics, quality_issues = evaluate_alignment_quality(
        aligned_records,
        min_uniswap5_coverage=min_uniswap5_coverage,
        min_uniswap30_coverage=min_uniswap30_coverage,
        staleness_threshold_minutes=staleness_threshold_minutes,
        max_uniswap5_stale_share=max_uniswap5_stale_share,
        max_uniswap30_stale_share=max_uniswap30_stale_share,
    )
    if fail_on_quality_warnings and quality_issues:
        summary = "; ".join(
            f"[{issue['severity']}:{issue['code']}] {issue['message']}"
            for issue in quality_issues
        )
        raise ValidationError(f"quality gate failed: {summary}")

    processed_result = run_processed_pipeline(
        aligned_records,
        output_dir=processed_output_dir,
        dataset_name=dataset_name,
        realized_vol_window=realized_vol_window,
        annualization_minutes=annualization_minutes,
        fail_on_warnings=fail_on_warnings,
    )
    dataset_rows = _read_json_list(processed_result.dataset_json_path)

    summary_payload = {
        "run_time_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "raw_run_id": raw_result.run_id,
        "raw_row_counts": raw_result.row_counts,
        "quality_metrics": quality_metrics,
        "quality_issues": quality_issues,
        "dataset_summary": _dataset_summary(dataset_rows),
        "artifacts": {
            "raw": raw_result.files,
            "aligned_json": aligned_json_path,
            "dataset_json": processed_result.dataset_json_path,
            "missingness_report_json": processed_result.report_json_path,
            "parquet": processed_result.parquet_path,
            "metadata_json": processed_result.metadata_path,
        },
    }

    processed_dir = Path(processed_output_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = processed_dir / f"full_run_summary_{raw_result.run_id}.json"
    summary_json_path.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return FullRunResult(
        raw_result=raw_result,
        aligned_json_path=aligned_json_path,
        processed_result=processed_result,
        summary_json_path=str(summary_json_path),
        quality_issue_count=len(quality_issues),
    )
