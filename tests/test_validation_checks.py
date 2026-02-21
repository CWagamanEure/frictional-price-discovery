"""Tests for validation checks and failure modes."""

from __future__ import annotations

import pytest

from ingestion.validation import ValidationError, enforce_validation, validate_records


def test_validate_records_detects_non_monotonic_timestamp() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": 101.0},
    ]

    issues = validate_records(rows)

    assert any(issue.code == "non_monotonic_timestamp" for issue in issues)


def test_enforce_validation_raises_on_hard_errors() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": -1.0},
    ]

    with pytest.raises(ValidationError):
        enforce_validation(
            rows,
            numeric_ranges={"coinbase_close": (0.0, None)},
        )


def test_enforce_validation_soft_warning_vs_hard_fail() -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": None},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 101.0},
        {"minute_utc": "2025-01-01T00:02:00Z", "coinbase_close": None},
    ]

    issues = enforce_validation(
        rows,
        warning_missing_thresholds={"coinbase_close": 0.5},
        fail_on_warnings=False,
    )
    assert any(issue.severity == "warning" for issue in issues)

    with pytest.raises(ValidationError):
        enforce_validation(
            rows,
            warning_missing_thresholds={"coinbase_close": 0.5},
            fail_on_warnings=True,
        )


def test_validate_records_required_columns() -> None:
    rows = [{"minute_utc": "2025-01-01T00:00:00Z"}]

    issues = validate_records(rows, required_columns={"minute_utc", "coinbase_close"})

    assert any(issue.code == "missing_required_columns" for issue in issues)
