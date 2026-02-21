"""Tests for Ethereum RPC client parsing and fetch behavior."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from ingestion.sources.ethereum_rpc import (
    EthereumRPCError,
    fetch_basefee_observations,
    parse_block_basefee,
)


class FakeEthereumRPCClient:
    """Fake Ethereum RPC client with deterministic block responses."""

    def __init__(
        self, blocks_by_number: Mapping[int, Mapping[str, Any] | None]
    ) -> None:
        self.blocks_by_number = dict(blocks_by_number)
        self._fee_history_calls: list[tuple[int, int]] = []

    def get_latest_block_number(self) -> int:
        return max(self.blocks_by_number)

    def get_block_by_number(self, block_number: int) -> Mapping[str, Any] | None:
        return self.blocks_by_number.get(block_number)

    def get_fee_history(self, block_count: int, newest_block: int) -> Mapping[str, Any]:
        self._fee_history_calls.append((block_count, newest_block))
        oldest_block = newest_block - block_count + 1
        return {
            "oldestBlock": hex(oldest_block),
            "baseFeePerGas": [hex(100 + i) for i in range(block_count + 1)],
            "gasUsedRatio": [0.5 for _ in range(block_count)],
        }


def _make_block(
    block_number: int,
    timestamp_unix: int,
    *,
    base_fee: int = 100,
    gas_used: int = 200,
    gas_limit: int = 300,
) -> dict[str, str]:
    return {
        "number": hex(block_number),
        "timestamp": hex(timestamp_unix),
        "baseFeePerGas": hex(base_fee),
        "gasUsed": hex(gas_used),
        "gasLimit": hex(gas_limit),
    }


def test_parse_block_basefee_converts_hex_values() -> None:
    block = _make_block(123, 1735689600, base_fee=10, gas_used=20, gas_limit=30)

    row = parse_block_basefee(block)

    assert row.block_number == 123
    assert row.timestamp_utc == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    assert row.base_fee_per_gas_wei == 10
    assert row.gas_used == 20
    assert row.gas_limit == 30


def test_parse_block_basefee_requires_number_and_timestamp() -> None:
    with pytest.raises(EthereumRPCError):
        parse_block_basefee({"number": "0x1"})


def test_fetch_basefee_observations_skips_missing_blocks_with_warning() -> None:
    client = FakeEthereumRPCClient(
        {
            0: _make_block(0, 1735689590),
            1: _make_block(1, 1735689600),
            2: None,
            3: _make_block(3, 1735689660),
            4: _make_block(4, 1735689720),
        }
    )

    with pytest.warns(RuntimeWarning, match="missing block 2"):
        rows = fetch_basefee_observations(
            client,
            start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            end_time_utc=datetime(2025, 1, 1, 0, 2, tzinfo=UTC),
            rpc_mode="blocks",
        )

    assert [row.block_number for row in rows] == [1, 3, 4]


def test_fetch_basefee_observations_feehistory_mode() -> None:
    client = FakeEthereumRPCClient(
        {
            0: _make_block(0, 1735689590, gas_limit=30_000_000),
            1: _make_block(1, 1735689600, gas_limit=30_000_000),
            2: _make_block(2, 1735689612, gas_limit=30_000_000),
            3: _make_block(3, 1735689624, gas_limit=30_000_000),
        }
    )

    rows = fetch_basefee_observations(
        client,
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 0, 24, tzinfo=UTC),
        rpc_mode="feehistory",
        feehistory_blocks_per_request=2,
    )

    assert [row.block_number for row in rows] == [1, 2, 3]
    assert rows[0].base_fee_per_gas_wei == 100
    assert rows[1].base_fee_per_gas_wei == 101
    assert rows[2].base_fee_per_gas_wei == 100
    assert rows[0].gas_used > 0


def test_fetch_basefee_observations_auto_falls_back_to_blocks() -> None:
    class _NoFeeHistoryClient(FakeEthereumRPCClient):
        def get_fee_history(
            self, block_count: int, newest_block: int
        ) -> Mapping[str, Any]:
            raise EthereumRPCError("method not available")

    client = _NoFeeHistoryClient(
        {
            0: _make_block(0, 1735689590),
            1: _make_block(1, 1735689600),
            2: _make_block(2, 1735689612),
        }
    )
    rows = fetch_basefee_observations(
        client,
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 0, 12, tzinfo=UTC),
        rpc_mode="auto",
    )
    assert [row.block_number for row in rows] == [1, 2]
