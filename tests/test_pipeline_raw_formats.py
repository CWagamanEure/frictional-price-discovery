"""Tests for raw ingestion output formats."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion import pipeline_raw


class _FakeGraphClient:
    def __init__(self, endpoint: str, api_key: str | None = None) -> None:
        self.endpoint = endpoint
        self.api_key = api_key


class _FakeCoinbaseClient:
    base_url = "https://example.com"

    def __init__(self, base_url: str = "https://example.com") -> None:
        self.base_url = base_url


class _FakeRPCClient:
    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url


def test_run_raw_ingestion_parquet_only(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pipeline_raw,
        "resolve_graph_endpoint",
        lambda **kwargs: "https://graph.example",
    )
    monkeypatch.setattr(pipeline_raw, "UrllibGraphClient", _FakeGraphClient)
    monkeypatch.setattr(
        pipeline_raw,
        "fetch_pool_swaps_raw",
        lambda *args, **kwargs: [{"id": "1", "amountUSD": "10.0"}],
    )

    monkeypatch.setattr(pipeline_raw, "UrllibCoinbaseClient", _FakeCoinbaseClient)
    monkeypatch.setattr(
        pipeline_raw,
        "fetch_coinbase_candles",
        lambda *args, **kwargs: [
            type(
                "CoinbaseObs",
                (),
                {
                    "to_record": lambda self: {
                        "minute_utc": "2025-01-01T00:00:00Z",
                        "close": 1.0,
                    }
                },
            )()
        ],
    )
    monkeypatch.setattr(
        pipeline_raw,
        "coinbase_observations_to_records",
        lambda rows: [row.to_record() for row in rows],
    )

    monkeypatch.setattr(pipeline_raw, "UrllibEthereumRPCClient", _FakeRPCClient)
    monkeypatch.setattr(
        pipeline_raw,
        "fetch_basefee_observations",
        lambda *args, **kwargs: [
            type(
                "EthObs",
                (),
                {"to_record": lambda self: {"block_number": 1, "base_fee": 100}},
            )()
        ],
    )
    monkeypatch.setattr(
        pipeline_raw,
        "ethereum_observations_to_records",
        lambda rows: [row.to_record() for row in rows],
    )

    result = pipeline_raw.run_raw_ingestion(
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 10, tzinfo=UTC),
        output_dir=str(tmp_path),
        graph_endpoint="https://graph.example",
        uniswap_pool_5_bps="0xpool5",
        uniswap_pool_30_bps="0xpool30",
        rpc_url="https://rpc.example",
        raw_format="parquet",
    )

    assert "uniswap_5bps_parquet" in result.files
    assert "uniswap_5bps_json" not in result.files
    assert "coinbase_parquet" in result.files
    assert "ethereum_rpc_parquet" in result.files
    parquet_path = Path(result.files["coinbase_parquet"])
    assert parquet_path.exists()
    assert pq.read_table(parquet_path).num_rows == 1


def test_run_raw_ingestion_both_formats(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pipeline_raw,
        "resolve_graph_endpoint",
        lambda **kwargs: "https://graph.example",
    )
    monkeypatch.setattr(pipeline_raw, "UrllibGraphClient", _FakeGraphClient)
    monkeypatch.setattr(
        pipeline_raw,
        "fetch_pool_swaps_raw",
        lambda *args, **kwargs: [{"id": "1", "amountUSD": "10.0"}],
    )
    monkeypatch.setattr(pipeline_raw, "UrllibCoinbaseClient", _FakeCoinbaseClient)
    monkeypatch.setattr(
        pipeline_raw, "fetch_coinbase_candles", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        pipeline_raw, "coinbase_observations_to_records", lambda rows: []
    )

    result = pipeline_raw.run_raw_ingestion(
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 10, tzinfo=UTC),
        output_dir=str(tmp_path),
        graph_endpoint="https://graph.example",
        uniswap_pool_5_bps="0xpool5",
        raw_format="both",
    )

    assert "uniswap_5bps_json" in result.files
    assert "uniswap_5bps_parquet" in result.files
    assert "coinbase_json" in result.files
    assert "coinbase_parquet" in result.files
