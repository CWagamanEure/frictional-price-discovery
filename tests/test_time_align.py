"""Tests for UTC minute canonical index and source alignment."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.transforms.time_align import (
    align_records_to_minute_index,
    build_minute_index,
    merge_aligned_sources,
    missing_minutes_for_source,
    normalize_timestamp_to_minute,
    rows_to_records,
)


def test_build_minute_index_end_inclusive_behavior() -> None:
    start = datetime(2025, 1, 1, 0, 0, 10, tzinfo=UTC)
    end = datetime(2025, 1, 1, 0, 2, 59, tzinfo=UTC)

    inclusive = build_minute_index(start, end, end_inclusive=True)
    exclusive = build_minute_index(start, end, end_inclusive=False)

    assert inclusive == [
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 2, tzinfo=UTC),
    ]
    assert exclusive == [
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
    ]


def test_normalize_timestamp_to_minute_handles_naive_datetime() -> None:
    ts = datetime(2025, 1, 1, 0, 0, 45)
    assert normalize_timestamp_to_minute(ts) == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)


def test_align_records_duplicate_policy_last_and_first() -> None:
    minute_index = build_minute_index(
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
    )
    records = [
        {"timestamp_utc": "2025-01-01T00:00:10Z", "price": 100.0},
        {"timestamp_utc": "2025-01-01T00:00:20Z", "price": 101.0},
        {"timestamp_utc": "2025-01-01T00:01:10Z", "price": 102.0},
    ]

    last_map = align_records_to_minute_index(
        minute_index,
        records,
        timestamp_key="timestamp_utc",
        duplicate_policy="last",
    )
    first_map = align_records_to_minute_index(
        minute_index,
        records,
        timestamp_key="timestamp_utc",
        duplicate_policy="first",
    )

    assert last_map[datetime(2025, 1, 1, 0, 0, tzinfo=UTC)]["price"] == 101.0
    assert first_map[datetime(2025, 1, 1, 0, 0, tzinfo=UTC)]["price"] == 100.0


def test_align_records_rejects_bad_duplicate_policy() -> None:
    with pytest.raises(ValueError):
        align_records_to_minute_index(
            minute_index=[],
            records=[],
            timestamp_key="timestamp_utc",
            duplicate_policy="bad",
        )


def test_merge_aligned_sources_and_missing_minutes() -> None:
    minute_index = build_minute_index(
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 2, tzinfo=UTC),
    )

    coinbase_map = {
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC): {"close": 100.0},
        datetime(2025, 1, 1, 0, 2, tzinfo=UTC): {"close": 102.0},
    }
    gas_map = {
        datetime(2025, 1, 1, 0, 1, tzinfo=UTC): {"base_fee_per_gas_wei": 42},
    }

    rows = merge_aligned_sources(
        minute_index,
        source_maps={"coinbase": coinbase_map, "gas": gas_map},
    )
    records = rows_to_records(rows)

    assert records[0]["coinbase_close"] == 100.0
    assert "gas_base_fee_per_gas_wei" not in records[0]
    assert records[1]["gas_base_fee_per_gas_wei"] == 42

    missing_coinbase = missing_minutes_for_source(minute_index, coinbase_map)
    assert missing_coinbase == [datetime(2025, 1, 1, 0, 1, tzinfo=UTC)]
