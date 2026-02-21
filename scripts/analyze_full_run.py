#!/usr/bin/env python3
"""Print a concise summary from full_run_summary artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze full_run_summary JSON output."
    )
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Path to full_run_summary_*.json. Defaults to latest in data/processed.",
    )
    return parser.parse_args()


def _latest_summary() -> Path:
    candidates = sorted(Path("data/processed").glob("full_run_summary_*.json"))
    if not candidates:
        raise FileNotFoundError("no full_run_summary files found in data/processed")
    return candidates[-1]


def _get_nested(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def main() -> int:
    args = parse_args()
    summary_path = Path(args.summary_json) if args.summary_json else _latest_summary()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(summary, dict):
        raise ValueError("summary must be a JSON object")

    run_id = summary.get("raw_run_id")
    row_counts = summary.get("raw_row_counts", {})
    quality_issues = summary.get("quality_issues", [])
    basis = summary.get("basis_summary", {})

    print(f"summary: {summary_path}")
    print(f"raw_run_id: {run_id}")
    print(f"raw_row_counts: {row_counts}")
    quality_issue_count = (
        len(quality_issues) if isinstance(quality_issues, list) else "n/a"
    )
    print(f"quality_issue_count: {quality_issue_count}")
    if isinstance(quality_issues, list):
        for issue in quality_issues:
            print(
                f"  - [{issue.get('severity')}:{issue.get('code')}] "
                f"{issue.get('message')}"
            )

    uni5_cov = _get_nested(summary, "quality_metrics", "coverage", "uniswap5")
    uni30_cov = _get_nested(summary, "quality_metrics", "coverage", "uniswap30")
    uni5_stale = _get_nested(
        summary,
        "quality_metrics",
        "staleness",
        "share_over_threshold",
        "uniswap5",
    )
    uni30_stale = _get_nested(
        summary,
        "quality_metrics",
        "staleness",
        "share_over_threshold",
        "uniswap30",
    )
    print(f"coverage_uniswap5: {uni5_cov}")
    print(f"coverage_uniswap30: {uni30_cov}")
    print(f"stale_share_uniswap5: {uni5_stale}")
    print(f"stale_share_uniswap30: {uni30_stale}")

    b5 = basis.get("basis_5_bps", {}) if isinstance(basis, dict) else {}
    b30 = basis.get("basis_30_bps", {}) if isinstance(basis, dict) else {}
    print(f"basis_5_bps: {b5}")
    print(f"basis_30_bps: {b30}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
