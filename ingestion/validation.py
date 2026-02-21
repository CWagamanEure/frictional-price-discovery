"""Validation checks for processed minute-level records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class ValidationError(RuntimeError):
    """Raised when hard validation checks fail."""


@dataclass(frozen=True)
class ValidationIssue:
    """One validation issue with severity and message."""

    severity: str
    code: str
    message: str


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError("timestamp value must be datetime or ISO-8601 string")


def validate_records(
    records: list[dict[str, Any]],
    *,
    timestamp_key: str = "minute_utc",
    required_columns: set[str] | None = None,
    numeric_ranges: dict[str, tuple[float | None, float | None]] | None = None,
    warning_numeric_ranges: dict[str, tuple[float | None, float | None]] | None = None,
    warning_missing_thresholds: dict[str, float] | None = None,
) -> list[ValidationIssue]:
    """Run schema, monotonicity, and value-range validations."""
    issues: list[ValidationIssue] = []
    required = required_columns or set()
    ranges = numeric_ranges or {}
    warning_ranges = warning_numeric_ranges or {}
    warning_thresholds = warning_missing_thresholds or {}

    if not isinstance(records, list):
        return [
            ValidationIssue(
                severity="error",
                code="invalid_records_type",
                message="records must be a list",
            )
        ]

    previous_ts: datetime | None = None
    for index, row in enumerate(records):
        if not isinstance(row, dict):
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="invalid_row_type",
                    message=f"row {index} is not an object",
                )
            )
            continue

        missing_required = sorted([column for column in required if column not in row])
        if missing_required:
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="missing_required_columns",
                    message=(
                        f"row {index} missing required columns: "
                        f"{', '.join(missing_required)}"
                    ),
                )
            )

        if timestamp_key in row:
            try:
                current_ts = _parse_timestamp(row[timestamp_key])
                if previous_ts is not None and current_ts <= previous_ts:
                    issues.append(
                        ValidationIssue(
                            severity="error",
                            code="non_monotonic_timestamp",
                            message=(
                                f"row {index} timestamp {current_ts.isoformat()} "
                                "is not strictly later than previous row"
                            ),
                        )
                    )
                previous_ts = current_ts
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="invalid_timestamp",
                        message=f"row {index} has invalid timestamp: {exc}",
                    )
                )

        for column, (min_value, max_value) in ranges.items():
            if column not in row or row[column] is None:
                continue
            try:
                value = float(row[column])
            except (TypeError, ValueError):
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="non_numeric_value",
                        message=f"row {index} column {column} is not numeric",
                    )
                )
                continue

            if min_value is not None and value < min_value:
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="value_below_min",
                        message=f"row {index} column {column} below min {min_value}",
                    )
                )
            if max_value is not None and value > max_value:
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="value_above_max",
                        message=f"row {index} column {column} above max {max_value}",
                    )
                )

        for column, (min_value, max_value) in warning_ranges.items():
            if column not in row or row[column] is None:
                continue
            try:
                value = float(row[column])
            except (TypeError, ValueError):
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        code="non_numeric_warning_value",
                        message=f"row {index} column {column} is not numeric",
                    )
                )
                continue

            if min_value is not None and value < min_value:
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        code="warning_value_below_min",
                        message=(
                            f"row {index} column {column} below warning min "
                            f"{min_value}"
                        ),
                    )
                )
            if max_value is not None and value > max_value:
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        code="warning_value_above_max",
                        message=(
                            f"row {index} column {column} above warning max "
                            f"{max_value}"
                        ),
                    )
                )

    if records:
        for column, threshold in warning_thresholds.items():
            missing = sum(1 for row in records if row.get(column) is None)
            missing_rate = missing / len(records)
            if missing_rate > threshold:
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        code="high_missingness",
                        message=(
                            f"column {column} missing rate {missing_rate:.3f} "
                            f"exceeds threshold {threshold:.3f}"
                        ),
                    )
                )

    return issues


def enforce_validation(
    records: list[dict[str, Any]],
    *,
    timestamp_key: str = "minute_utc",
    required_columns: set[str] | None = None,
    numeric_ranges: dict[str, tuple[float | None, float | None]] | None = None,
    warning_numeric_ranges: dict[str, tuple[float | None, float | None]] | None = None,
    warning_missing_thresholds: dict[str, float] | None = None,
    fail_on_warnings: bool = False,
) -> list[ValidationIssue]:
    """Validate records and raise ValidationError on configured failure modes."""
    issues = validate_records(
        records,
        timestamp_key=timestamp_key,
        required_columns=required_columns,
        numeric_ranges=numeric_ranges,
        warning_numeric_ranges=warning_numeric_ranges,
        warning_missing_thresholds=warning_missing_thresholds,
    )

    has_error = any(issue.severity == "error" for issue in issues)
    has_warning = any(issue.severity == "warning" for issue in issues)

    if has_error or (fail_on_warnings and has_warning):
        summary = "; ".join(f"[{i.severity}:{i.code}] {i.message}" for i in issues)
        raise ValidationError(summary)

    return issues
