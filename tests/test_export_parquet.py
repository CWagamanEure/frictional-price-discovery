"""Tests for Parquet export behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.export import export_records


def test_export_records_writes_parquet_roundtrip(tmp_path: Path) -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "basis_5_bps": 10.0,
        },
        {
            "minute_utc": "2025-01-01T00:01:00Z",
            "coinbase_close": 101.0,
            "basis_5_bps": 9.0,
        },
    ]

    result = export_records(
        rows,
        output_dir=str(tmp_path),
        dataset_name="features",
        start_time_utc=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        end_time_utc=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
        config={"version": 1},
    )

    parquet_path = Path(result.parquet_path)
    assert parquet_path.exists()

    table = pq.read_table(parquet_path)
    assert table.num_rows == 2
    assert set(table.column_names) == {"minute_utc", "coinbase_close", "basis_5_bps"}
