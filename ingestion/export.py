"""Parquet + metadata export for processed datasets."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class ExportResult:
    """Paths and metadata from an export run."""

    parquet_path: str
    metadata_path: str
    metadata: dict[str, Any]


def _as_utc_iso(ts: datetime | str) -> str:
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    else:
        dt = ts
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _safe_slug(value: str) -> str:
    return (
        value.replace(":", "")
        .replace("-", "")
        .replace("T", "T")
        .replace("Z", "Z")
        .replace(" ", "_")
    )


def _config_hash(config: dict[str, Any]) -> str:
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _null_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    keys: set[str] = set()
    for row in records:
        keys.update(row.keys())

    counts = {key: 0 for key in sorted(keys)}
    for row in records:
        for key in counts:
            if row.get(key) is None:
                counts[key] += 1
    return counts


def export_records(
    records: list[dict[str, Any]],
    *,
    output_dir: str = "data/processed",
    dataset_name: str,
    start_time_utc: datetime | str,
    end_time_utc: datetime | str,
    config: dict[str, Any] | None = None,
) -> ExportResult:
    """Export records to Parquet and write metadata JSON."""
    export_config = config or {}
    config_hash = _config_hash(export_config)

    start_iso = _as_utc_iso(start_time_utc)
    end_iso = _as_utc_iso(end_time_utc)

    prefix = (
        f"{dataset_name}_"
        f"{_safe_slug(start_iso)}_"
        f"{_safe_slug(end_iso)}_"
        f"{config_hash[:12]}"
    )

    processed_dir = Path(output_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = processed_dir / f"{prefix}.parquet"
    metadata_path = processed_dir / f"{prefix}.metadata.json"

    table = pa.Table.from_pylist(records)
    pq.write_table(table, parquet_path)

    metadata: dict[str, Any] = {
        "dataset_name": dataset_name,
        "window": {
            "start_time_utc": start_iso,
            "end_time_utc": end_iso,
        },
        "row_count": len(records),
        "column_count": len(table.column_names),
        "columns": table.column_names,
        "null_counts": _null_counts(records),
        "config_hash": config_hash,
        "config": export_config,
        "parquet_file": str(parquet_path),
        "parquet_sha256": _file_sha256(parquet_path),
    }

    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return ExportResult(
        parquet_path=str(parquet_path),
        metadata_path=str(metadata_path),
        metadata=metadata,
    )
