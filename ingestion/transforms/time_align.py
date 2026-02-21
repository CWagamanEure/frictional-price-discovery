"""Canonical UTC minute index and source alignment helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from ingestion.utils_time import floor_to_utc_minute, to_utc


@dataclass(frozen=True)
class AlignedRow:
    """A canonical minute row with optional merged source payload."""

    minute_utc: datetime
    values: dict[str, Any]


def build_minute_index(
    start_time_utc: datetime,
    end_time_utc: datetime,
    *,
    end_inclusive: bool = True,
) -> list[datetime]:
    """Build canonical UTC minute grid from start floor to end boundary."""
    start_minute = floor_to_utc_minute(to_utc(start_time_utc))
    end_minute = floor_to_utc_minute(to_utc(end_time_utc))

    if end_inclusive:
        stop = end_minute
    else:
        stop = end_minute - timedelta(minutes=1)

    if stop < start_minute:
        return []

    minutes: list[datetime] = []
    current = start_minute
    while current <= stop:
        minutes.append(current)
        current = current + timedelta(minutes=1)

    return minutes


def normalize_timestamp_to_minute(ts: datetime) -> datetime:
    """Normalize arbitrary timestamp to UTC minute."""
    return floor_to_utc_minute(to_utc(ts))


def align_records_to_minute_index(
    minute_index: list[datetime],
    records: list[dict[str, Any]],
    *,
    timestamp_key: str,
    duplicate_policy: str = "last",
) -> dict[datetime, dict[str, Any]]:
    """Align source records to canonical minute index using duplicate policy."""
    if duplicate_policy not in {"last", "first"}:
        raise ValueError("duplicate_policy must be 'last' or 'first'")

    normalized: dict[datetime, dict[str, Any]] = {}
    for record in records:
        raw_ts = record[timestamp_key]
        if isinstance(raw_ts, str):
            parsed = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        elif isinstance(raw_ts, datetime):
            parsed = raw_ts
        else:
            raise ValueError("record timestamp must be str or datetime")

        minute = normalize_timestamp_to_minute(parsed)
        if minute not in minute_index:
            continue

        payload = {k: v for k, v in record.items() if k != timestamp_key}
        if minute not in normalized:
            normalized[minute] = payload
        elif duplicate_policy == "last":
            normalized[minute] = payload

    return normalized


def merge_aligned_sources(
    minute_index: list[datetime],
    source_maps: dict[str, dict[datetime, dict[str, Any]]],
) -> list[AlignedRow]:
    """Merge multiple aligned source maps onto canonical minute rows."""
    rows: list[AlignedRow] = []
    for minute in minute_index:
        merged_values: dict[str, Any] = {}
        for source_name, aligned_map in source_maps.items():
            source_values = aligned_map.get(minute, {})
            for key, value in source_values.items():
                merged_values[f"{source_name}_{key}"] = value
        rows.append(AlignedRow(minute_utc=minute, values=merged_values))

    return rows


def missing_minutes_for_source(
    minute_index: list[datetime],
    aligned_map: dict[datetime, dict[str, Any]],
) -> list[datetime]:
    """Return canonical minutes with no record for a given source."""
    return [minute for minute in minute_index if minute not in aligned_map]


def rows_to_records(rows: list[AlignedRow]) -> list[dict[str, Any]]:
    """Convert aligned rows to serializable records."""
    return [
        {
            "minute_utc": row.minute_utc.isoformat().replace("+00:00", "Z"),
            **row.values,
        }
        for row in rows
    ]
