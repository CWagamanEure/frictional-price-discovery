"""Reporting utilities for missingness and data quality summaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _max_consecutive_missing(records: list[dict[str, Any]], column: str) -> int:
    best = 0
    current = 0
    for row in records:
        if row.get(column) is None:
            current += 1
            if current > best:
                best = current
        else:
            current = 0
    return best


def build_missingness_report(
    records: list[dict[str, Any]],
    *,
    expected_columns: set[str] | None = None,
) -> dict[str, Any]:
    """Compute missingness rates and consecutive gaps per column."""
    total_rows = len(records)

    columns: set[str] = set(expected_columns or set())
    for row in records:
        columns.update(row.keys())

    per_column: dict[str, dict[str, Any]] = {}
    for column in sorted(columns):
        missing_count = sum(1 for row in records if row.get(column) is None)
        missing_rate = (missing_count / total_rows) if total_rows else 0.0
        per_column[column] = {
            "missing_count": missing_count,
            "missing_rate": missing_rate,
            "max_consecutive_missing": _max_consecutive_missing(records, column),
        }

    return {
        "total_rows": total_rows,
        "column_count": len(columns),
        "per_column": per_column,
    }


def write_missingness_report(path: str, report: dict[str, Any]) -> None:
    """Write missingness report JSON to disk."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
