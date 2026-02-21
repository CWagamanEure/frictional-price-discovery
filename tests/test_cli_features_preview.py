"""CLI tests for feature preview command."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.cli import main


def test_features_preview_writes_output_file(tmp_path: Path) -> None:
    input_path = tmp_path / "aligned.json"
    output_path = tmp_path / "features.json"

    input_rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "uniswap5_token0_price": 100.1,
            "uniswap30_token0_price": 99.9,
            "gas_base_fee_per_gas_wei": 20_000_000_000,
        }
    ]
    input_path.write_text(json.dumps(input_rows), encoding="utf-8")

    exit_code = main(
        [
            "features-preview",
            "--input-json",
            str(input_path),
            "--output-json",
            str(output_path),
            "--realized-vol-window",
            "1",
            "--annualization-minutes",
            "1",
        ]
    )

    assert exit_code == 0
    output_rows = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(output_rows) == 1
    assert "basis_5_bps" in output_rows[0]
