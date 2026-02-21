"""Build aligned minute records from raw ingestion artifacts."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ingestion.transforms.time_align import (
    align_records_to_minute_index,
    build_minute_index,
    merge_aligned_sources,
    rows_to_records,
)


def _load_records(path: str) -> list[dict[str, Any]]:
    file_path = Path(path)
    if file_path.suffix == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"expected list payload in {path}")
        return payload
    if file_path.suffix == ".parquet":
        return pq.read_table(file_path).to_pylist()
    raise ValueError(f"unsupported artifact format: {path}")


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            dt = datetime.fromtimestamp(int(stripped), tz=UTC)
        else:
            dt = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    else:
        dt = datetime.fromtimestamp(int(value), tz=UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _sqrt_price_x96_to_price(sqrt_price_x96: Any) -> float | None:
    if sqrt_price_x96 in (None, "", 0, "0"):
        return None
    try:
        value = float(sqrt_price_x96)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return (value * value) / float(2**192)


def _price_from_swap_amounts(row: dict[str, Any]) -> float | None:
    amount0 = row.get("amount0")
    amount1 = row.get("amount1")
    if amount0 in (None, "", 0, "0") or amount1 in (None, "", 0, "0"):
        return None
    try:
        amount0_float = float(amount0)
        amount1_float = float(amount1)
    except (TypeError, ValueError):
        return None
    if amount0_float == 0 or amount1_float == 0:
        return None

    ratio_1_over_0 = abs(amount1_float / amount0_float)
    ratio_0_over_1 = abs(amount0_float / amount1_float)
    if not math.isfinite(ratio_1_over_0) or not math.isfinite(ratio_0_over_1):
        return None

    # For ETH-USD style downstream analysis, choose the USD-per-ETH orientation.
    return max(ratio_1_over_0, ratio_0_over_1)


def _normalize_uniswap_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in raw_rows:
        ts = row.get("timestamp_utc") or row.get("timestamp")
        if ts is None:
            continue

        token0_price = _price_from_swap_amounts(row)

        for candidate in ("token1Price", "token1_price", "token0Price", "token0_price"):
            if token0_price is None and candidate in row and row[candidate] is not None:
                try:
                    token0_price = float(row[candidate])
                except (TypeError, ValueError):
                    token0_price = None
                break
        if token0_price is None:
            token0_price = _sqrt_price_x96_to_price(row.get("sqrtPriceX96"))
            if token0_price is not None and token0_price < 1:
                token0_price = 1.0 / token0_price

        normalized.append(
            {
                "timestamp_utc": _parse_timestamp(ts)
                .isoformat()
                .replace("+00:00", "Z"),
                "token0_price": token0_price,
            }
        )

    return normalized


def _normalize_coinbase_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in raw_rows:
        ts = row.get("timestamp_utc") or row.get("time")
        if ts is None:
            continue

        close_value = row.get("close_price", row.get("close"))
        normalized.append(
            {
                "timestamp_utc": _parse_timestamp(ts)
                .isoformat()
                .replace("+00:00", "Z"),
                "close": close_value,
            }
        )
    return normalized


def _normalize_gas_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in raw_rows:
        ts = row.get("timestamp_utc")
        if ts is None:
            continue

        normalized.append(
            {
                "timestamp_utc": _parse_timestamp(ts)
                .isoformat()
                .replace("+00:00", "Z"),
                "base_fee_per_gas_wei": row.get(
                    "base_fee_per_gas_wei", row.get("base_fee")
                ),
            }
        )
    return normalized


def _pick_source_file(files: dict[str, str], key_prefix: str) -> str | None:
    for suffix in ("parquet", "json"):
        key = f"{key_prefix}_{suffix}"
        if key in files:
            return files[key]
    # Backward compatibility for early raw-ingest runs that stored unsuffixed keys.
    if key_prefix in files:
        return files[key_prefix]
    return None


def _latest_raw_run_log(raw_dir: str = "data/raw") -> str:
    candidates = sorted(Path(raw_dir).glob("raw_ingestion_run_*.json"))
    if not candidates:
        raise FileNotFoundError("no raw ingestion run logs found in data/raw")
    return str(candidates[-1])


def _parse_minute_utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("minute_utc must be a string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _as_valid_price(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _forward_fill_uniswap_mid_prices(records: list[dict[str, Any]]) -> None:
    configs = (
        ("uniswap5_token0_price", "uniswap5_age_since_last_trade_min"),
        ("uniswap30_token0_price", "uniswap30_age_since_last_trade_min"),
    )

    for price_key, age_key in configs:
        last_price: float | None = None
        last_trade_minute: datetime | None = None

        for row in records:
            minute = _parse_minute_utc(row["minute_utc"])
            observed_price = _as_valid_price(row.get(price_key))
            if observed_price is not None:
                last_price = observed_price
                last_trade_minute = minute
                row[price_key] = observed_price
                row[age_key] = 0
                continue

            if last_price is not None and last_trade_minute is not None:
                age_minutes = int((minute - last_trade_minute).total_seconds() // 60)
                row[price_key] = last_price
                row[age_key] = max(0, age_minutes)
            else:
                row[age_key] = None


def build_aligned_from_raw_run(
    *,
    raw_run_log_path: str | None = None,
    output_json_path: str = "data/interim/aligned_records.json",
    duplicate_policy: str = "last",
) -> str:
    """Build aligned minute records from one raw ingestion run log."""
    run_log_path = raw_run_log_path or _latest_raw_run_log()
    run_log = json.loads(Path(run_log_path).read_text(encoding="utf-8"))

    start_time_utc = _parse_timestamp(run_log["start_time_utc"])
    end_time_utc = _parse_timestamp(run_log["end_time_utc"])
    minute_index = build_minute_index(start_time_utc, end_time_utc)

    files: dict[str, str] = run_log.get("files", {})

    source_maps: dict[str, dict[datetime, dict[str, Any]]] = {}

    coinbase_file = _pick_source_file(files, "coinbase")
    if coinbase_file:
        coinbase_rows = _normalize_coinbase_rows(_load_records(coinbase_file))
        source_maps["coinbase"] = align_records_to_minute_index(
            minute_index,
            coinbase_rows,
            timestamp_key="timestamp_utc",
            duplicate_policy=duplicate_policy,
        )

    gas_file = _pick_source_file(files, "ethereum_rpc")
    if gas_file:
        gas_rows = _normalize_gas_rows(_load_records(gas_file))
        source_maps["gas"] = align_records_to_minute_index(
            minute_index,
            gas_rows,
            timestamp_key="timestamp_utc",
            duplicate_policy=duplicate_policy,
        )

    uni5_file = _pick_source_file(files, "uniswap_5bps")
    if uni5_file:
        uni5_rows = _normalize_uniswap_rows(_load_records(uni5_file))
        source_maps["uniswap5"] = align_records_to_minute_index(
            minute_index,
            uni5_rows,
            timestamp_key="timestamp_utc",
            duplicate_policy=duplicate_policy,
        )

    uni30_file = _pick_source_file(files, "uniswap_30bps")
    if uni30_file:
        uni30_rows = _normalize_uniswap_rows(_load_records(uni30_file))
        source_maps["uniswap30"] = align_records_to_minute_index(
            minute_index,
            uni30_rows,
            timestamp_key="timestamp_utc",
            duplicate_policy=duplicate_policy,
        )

    aligned_rows = merge_aligned_sources(minute_index, source_maps)
    records = rows_to_records(aligned_rows)
    _forward_fill_uniswap_mid_prices(records)

    output_path = Path(output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    return str(output_path)
