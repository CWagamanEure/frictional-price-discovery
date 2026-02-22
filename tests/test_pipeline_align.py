"""Tests for alignment pipeline from raw artifacts."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.pipeline_align import build_aligned_from_raw_run


def test_build_aligned_from_raw_run(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    coinbase = raw_dir / "coinbase.parquet"
    coinbase.write_text("", encoding="utf-8")

    # Use JSON artifacts for deterministic test fixtures.
    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {
                    "timestamp": 1735689600,
                    "token0Price": "100.0",
                },
                {
                    "timestamp": 1735689660,
                    "token0Price": "101.0",
                },
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [
                {
                    "timestamp_utc": "2025-01-01T00:00:00Z",
                    "close_price": 100.0,
                    "volume": 12.5,
                },
                {
                    "timestamp_utc": "2025-01-01T00:01:00Z",
                    "close_price": 101.0,
                    "volume": 20.0,
                },
            ]
        ),
        encoding="utf-8",
    )

    gas_path = raw_dir / "gas.json"
    gas_path.write_text(
        json.dumps(
            [
                {"timestamp_utc": "2025-01-01T00:00:20Z", "base_fee_per_gas_wei": 20},
                {"timestamp_utc": "2025-01-01T00:01:10Z", "base_fee_per_gas_wei": 25},
            ]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:01:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                    "ethereum_rpc_json": str(gas_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    written = build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
    )

    assert written == str(output_path)
    rows = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(rows) == 2
    assert rows[0]["minute_utc"] == "2025-01-01T00:00:00Z"
    assert rows[0]["coinbase_close"] == 100.0
    assert rows[0]["coinbase_volume"] == 12.5
    assert rows[0]["gas_base_fee_per_gas_wei"] == 20
    assert "uniswap5_token0_price" in rows[0]
    assert rows[0]["uniswap5_age_since_last_trade_min"] == 0
    assert rows[0]["uniswap5_swap_count"] == 1
    assert rows[0]["uniswap5_flow_usd"] == 0.0


def test_build_aligned_forward_fills_uniswap_with_age(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {"timestamp": 1735689600, "token0Price": "100.0"},
                {"timestamp": 1735689720, "token0Price": "105.0"},
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [
                {"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 100.0},
                {"timestamp_utc": "2025-01-01T00:01:00Z", "close_price": 101.0},
                {"timestamp_utc": "2025-01-01T00:02:00Z", "close_price": 102.0},
            ]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:02:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(rows) == 3
    assert rows[0]["uniswap5_token0_price"] == 100.0
    assert rows[0]["uniswap5_age_since_last_trade_min"] == 0
    assert rows[1]["uniswap5_token0_price"] == 100.0
    assert rows[1]["uniswap5_age_since_last_trade_min"] == 1
    assert rows[2]["uniswap5_token0_price"] == 105.0
    assert rows[2]["uniswap5_age_since_last_trade_min"] == 0


def test_build_aligned_uses_usd_per_eth_orientation_from_swap_amounts(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    # Similar shape to USDC/WETH swap amounts from raw Graph swaps.
    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {
                    "timestamp": 1735689600,
                    "amount0": "-1470.779695",
                    "amount1": "0.391320430316185363",
                }
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [{"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 3762.04}]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:00:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["uniswap5_token0_price"] > 1000.0


def test_build_aligned_filters_uniswap_outlier_and_carries_forward(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {"timestamp": 1735689600, "token0Price": "100.0"},
                {"timestamp": 1735689660, "token0Price": "10000000.0"},
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [
                {"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 100.0},
                {"timestamp_utc": "2025-01-01T00:01:00Z", "close_price": 100.0},
            ]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:01:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert rows[0]["uniswap5_token0_price"] == 100.0
    assert rows[0]["uniswap5_price_outlier_flag"] is False
    assert rows[1]["uniswap5_token0_price"] == 100.0
    assert rows[1]["uniswap5_age_since_last_trade_min"] == 1
    assert rows[1]["uniswap5_price_outlier_flag"] is True


def test_build_aligned_aggregates_uniswap_flow_and_swap_count_per_minute(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {
                    "timestamp": 1735689601,
                    "token0Price": "100.0",
                    "amountUSD": "10.5",
                },
                {
                    "timestamp": 1735689610,
                    "token0Price": "101.0",
                    "amountUSD": "5.25",
                },
                {
                    "timestamp": 1735689660,
                    "token0Price": "102.0",
                    "amountUSD": "7.0",
                },
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [
                {"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 100.0},
                {"timestamp_utc": "2025-01-01T00:01:00Z", "close_price": 102.0},
            ]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:01:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
        duplicate_policy="last",
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert rows[0]["uniswap5_swap_count"] == 2
    assert rows[0]["uniswap5_flow_usd"] == 15.75
    assert rows[0]["uniswap5_token0_price"] == 101.0
    assert rows[1]["uniswap5_swap_count"] == 1
    assert rows[1]["uniswap5_flow_usd"] == 7.0


def test_build_aligned_uses_first_duplicate_policy_for_uniswap_minute_price(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {"timestamp": 1735689601, "token0Price": "100.0", "amountUSD": "1"},
                {"timestamp": 1735689610, "token0Price": "101.0", "amountUSD": "2"},
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [{"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 100.0}]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:00:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
        duplicate_policy="first",
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert rows[0]["uniswap5_token0_price"] == 100.0
    assert rows[0]["uniswap5_swap_count"] == 2
    assert rows[0]["uniswap5_flow_usd"] == 3.0


def test_build_aligned_patches_isolated_uniswap_spike_with_neighbor_mean(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    uni5_path = raw_dir / "uni5.json"
    uni5_path.write_text(
        json.dumps(
            [
                {"timestamp": 1735689600, "token0Price": "100.0", "amountUSD": "1"},
                {"timestamp": 1735689660, "token0Price": "125.0", "amountUSD": "1"},
                {"timestamp": 1735689720, "token0Price": "101.0", "amountUSD": "1"},
            ]
        ),
        encoding="utf-8",
    )

    coinbase_path = raw_dir / "coinbase.json"
    coinbase_path.write_text(
        json.dumps(
            [
                {"timestamp_utc": "2025-01-01T00:00:00Z", "close_price": 100.0},
                {"timestamp_utc": "2025-01-01T00:01:00Z", "close_price": 100.5},
                {"timestamp_utc": "2025-01-01T00:02:00Z", "close_price": 101.0},
            ]
        ),
        encoding="utf-8",
    )

    run_log_path = raw_dir / "raw_ingestion_run_20250101T000000Z.json"
    run_log_path.write_text(
        json.dumps(
            {
                "start_time_utc": "2025-01-01T00:00:00Z",
                "end_time_utc": "2025-01-01T00:02:00Z",
                "files": {
                    "uniswap_5bps_json": str(uni5_path),
                    "coinbase_json": str(coinbase_path),
                },
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "interim" / "aligned_records.json"
    build_aligned_from_raw_run(
        raw_run_log_path=str(run_log_path),
        output_json_path=str(output_path),
    )
    rows = json.loads(output_path.read_text(encoding="utf-8"))

    assert rows[0]["uniswap5_token0_price"] == 100.0
    assert rows[1]["uniswap5_price_spike_patch_flag"] is True
    assert rows[1]["uniswap5_price_outlier_flag"] is True
    assert rows[1]["uniswap5_token0_price"] == 100.5
    assert rows[1]["uniswap5_age_since_last_trade_min"] == 0
    assert rows[2]["uniswap5_token0_price"] == 101.0
