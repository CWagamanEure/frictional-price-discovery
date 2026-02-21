"""Smoke tests for initial project wiring."""

from datetime import UTC, datetime

from ingestion.utils_time import floor_to_utc_minute


def test_pytest_runs() -> None:
    assert True


def test_floor_to_utc_minute() -> None:
    ts = datetime(2025, 1, 1, 12, 34, 56, tzinfo=UTC)
    assert floor_to_utc_minute(ts) == datetime(2025, 1, 1, 12, 34, 0, tzinfo=UTC)
