"""Tests for minute-level basefee aggregation."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.models import GasBasefeeObservation
from ingestion.sources.ethereum_rpc import aggregate_basefee_to_minutes


def _obs(
    block_number: int,
    minute: int,
    second: int,
    *,
    base_fee: int,
    gas_used: int,
    gas_limit: int,
) -> GasBasefeeObservation:
    return GasBasefeeObservation(
        block_number=block_number,
        timestamp_utc=datetime(2025, 1, 1, 0, minute, second, tzinfo=UTC),
        base_fee_per_gas_wei=base_fee,
        gas_used=gas_used,
        gas_limit=gas_limit,
    )


def test_aggregate_basefee_to_minutes_uses_latest_block_in_minute() -> None:
    rows = [
        _obs(100, 0, 5, base_fee=10, gas_used=100, gas_limit=200),
        _obs(101, 0, 40, base_fee=11, gas_used=101, gas_limit=201),
        _obs(102, 1, 10, base_fee=12, gas_used=102, gas_limit=202),
    ]

    minute_rows = aggregate_basefee_to_minutes(rows)

    assert len(minute_rows) == 2
    first = minute_rows[0]
    assert first.minute_utc == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    assert first.block_number == 101
    assert first.base_fee_per_gas_wei == 11
    assert first.block_count == 2

    second = minute_rows[1]
    assert second.minute_utc == datetime(2025, 1, 1, 0, 1, tzinfo=UTC)
    assert second.block_number == 102
    assert second.base_fee_per_gas_wei == 12
    assert second.block_count == 1


def test_aggregate_basefee_to_minutes_empty_input() -> None:
    assert aggregate_basefee_to_minutes([]) == []
