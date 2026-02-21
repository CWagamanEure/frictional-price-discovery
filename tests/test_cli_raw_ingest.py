"""CLI tests for raw ingestion command wiring."""

from __future__ import annotations

from dataclasses import dataclass

from ingestion import cli


@dataclass
class _FakeResult:
    run_id: str
    files: dict[str, str]
    row_counts: dict[str, int]


def test_raw_ingest_calls_pipeline(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_raw_ingestion(**kwargs):
        captured.update(kwargs)
        return _FakeResult(run_id="fake", files={}, row_counts={"coinbase": 1})

    monkeypatch.setattr(cli, "run_raw_ingestion", _fake_run_raw_ingestion)

    exit_code = cli.main(
        [
            "raw-ingest",
            "--start-time-utc",
            "2025-01-01T00:00:00Z",
            "--end-time-utc",
            "2025-01-01T00:10:00Z",
            "--graph-endpoint",
            "https://example.com/graph",
            "--uniswap-pool-5-bps",
            "0xpool",
            "--coinbase-product-id",
            "ETH-USD",
            "--raw-format",
            "both",
        ]
    )

    assert exit_code == 0
    assert captured["output_dir"] == "data/raw"
    assert captured["graph_endpoint"] == "https://example.com/graph"
    assert captured["uniswap_pool_5_bps"] == "0xpool"
    assert captured["coinbase_product_id"] == "ETH-USD"
    assert captured["raw_format"] == "both"
