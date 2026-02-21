"""Tests for export metadata and idempotency."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion.export import export_records


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def test_export_records_writes_metadata_fields(tmp_path: Path) -> None:
    rows = [
        {
            "minute_utc": "2025-01-01T00:00:00Z",
            "coinbase_close": 100.0,
            "basis_5_bps": None,
        },
        {
            "minute_utc": "2025-01-01T00:01:00Z",
            "coinbase_close": None,
            "basis_5_bps": 8.0,
        },
    ]

    result = export_records(
        rows,
        output_dir=str(tmp_path),
        dataset_name="features",
        start_time_utc="2025-01-01T00:00:00Z",
        end_time_utc="2025-01-01T00:01:00Z",
        config={"source": "test"},
    )

    metadata_path = Path(result.metadata_path)
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert payload["dataset_name"] == "features"
    assert payload["row_count"] == 2
    assert payload["column_count"] == 3
    assert payload["window"]["start_time_utc"] == "2025-01-01T00:00:00Z"
    assert payload["window"]["end_time_utc"] == "2025-01-01T00:01:00Z"
    assert payload["null_counts"]["coinbase_close"] == 1
    assert payload["null_counts"]["basis_5_bps"] == 1
    assert isinstance(payload["config_hash"], str)
    assert len(payload["config_hash"]) == 64


def test_export_idempotent_for_identical_inputs(tmp_path: Path) -> None:
    rows = [
        {"minute_utc": "2025-01-01T00:00:00Z", "coinbase_close": 100.0},
        {"minute_utc": "2025-01-01T00:01:00Z", "coinbase_close": 101.0},
    ]
    kwargs = {
        "output_dir": str(tmp_path),
        "dataset_name": "features",
        "start_time_utc": datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        "end_time_utc": datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
        "config": {"mode": "test"},
    }

    first = export_records(rows, **kwargs)
    second = export_records(rows, **kwargs)

    first_parquet = Path(first.parquet_path)
    second_parquet = Path(second.parquet_path)
    first_metadata = Path(first.metadata_path)
    second_metadata = Path(second.metadata_path)

    assert first_parquet == second_parquet
    assert first_metadata == second_metadata
    assert _sha256(first_parquet) == _sha256(second_parquet)
    assert _sha256(first_metadata) == _sha256(second_metadata)
