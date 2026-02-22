"""Lightweight dataset shaping for aligned minute-level market data."""

from __future__ import annotations

import bisect
from collections import deque
import math
import statistics
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _realized_vol_annualized(
    prices: list[float | None],
    index: int,
    *,
    window: int,
    annualization_minutes: int,
) -> float | None:
    if window < 1 or index < window:
        return None

    returns: list[float] = []
    for offset in range(index - window + 1, index + 1):
        prev = prices[offset - 1]
        curr = prices[offset]
        if prev is None or curr is None or prev <= 0 or curr <= 0:
            return None
        returns.append(math.log(curr / prev))

    realized_std = statistics.pstdev(returns)
    return realized_std * math.sqrt(float(annualization_minutes))


def _log_price(price: float | None) -> float | None:
    if price is None or price <= 0:
        return None
    return math.log(price)


def _log_return(curr_price: float | None, prev_price: float | None) -> float | None:
    if (
        curr_price is None
        or prev_price is None
        or curr_price <= 0
        or prev_price <= 0
    ):
        return None
    return math.log(curr_price / prev_price)


def _wedge_price_diff(dex_price: float | None, cex_price: float | None) -> float | None:
    if dex_price is None or cex_price is None:
        return None
    return dex_price - cex_price


def _wedge_bps_from_logs(dex_price: float | None, cex_price: float | None) -> float | None:
    dex_log = _log_price(dex_price)
    cex_log = _log_price(cex_price)
    if dex_log is None or cex_log is None:
        return None
    return 10_000.0 * (dex_log - cex_log)


def _gas_usd(
    gas_base_fee_wei: float | None,
    eth_usd_price: float | None,
    *,
    gas_units_assumption: int,
) -> float | None:
    if (
        gas_base_fee_wei is None
        or eth_usd_price is None
        or gas_base_fee_wei < 0
        or eth_usd_price <= 0
    ):
        return None
    gas_price_eth = gas_base_fee_wei / 1_000_000_000_000_000_000.0
    return float(gas_units_assumption) * gas_price_eth * eth_usd_price


def _rolling_percentile_rank(
    values: list[float | None],
    *,
    window_size: int,
) -> list[float | None]:
    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    out: list[float | None] = []
    window_queue: deque[float | None] = deque()
    sorted_window: list[float] = []

    for value in values:
        window_queue.append(value)
        if value is not None:
            bisect.insort(sorted_window, value)

        while len(window_queue) > window_size:
            removed = window_queue.popleft()
            if removed is None:
                continue
            idx = bisect.bisect_left(sorted_window, removed)
            if idx < len(sorted_window) and sorted_window[idx] == removed:
                sorted_window.pop(idx)

        if value is None or not sorted_window:
            out.append(None)
            continue

        # Inclusive percentile rank in [0, 1], using current trailing window.
        rank = bisect.bisect_right(sorted_window, value)
        out.append(rank / len(sorted_window))

    return out


def build_dataset_rows(
    aligned_records: list[dict[str, Any]],
    *,
    cex_price_key: str = "coinbase_close",
    realized_vol_window: int = 30,
    annualization_minutes: int = 525_600,
    gas_base_fee_wei_key: str = "gas_base_fee_per_gas_wei",
    gas_units_assumption: int = 200_000,
    congestion_window_minutes: int = 30 * 24 * 60,
) -> list[dict[str, Any]]:
    """Return aligned rows with explicit DEX fee tiers and realized volatility."""
    output: list[dict[str, Any]] = []
    cex_prices: list[float | None] = []
    dex5_prices: list[float | None] = []
    dex30_prices: list[float | None] = []
    gas_usd_series: list[float | None] = []

    for record in aligned_records:
        cex_price = _to_float(record.get(cex_price_key))
        dex5_price = _to_float(record.get("uniswap5_token0_price"))
        dex30_price = _to_float(record.get("uniswap30_token0_price"))
        gas_base_fee_wei = _to_float(record.get(gas_base_fee_wei_key))
        cex_prices.append(cex_price)
        dex5_prices.append(dex5_price)
        dex30_prices.append(dex30_price)
        gas_usd_series.append(
            _gas_usd(
                gas_base_fee_wei,
                cex_price,
                gas_units_assumption=gas_units_assumption,
            )
        )

    congestion_series = _rolling_percentile_rank(
        gas_usd_series,
        window_size=congestion_window_minutes,
    )

    for row_index, record in enumerate(aligned_records):
        cex_price = cex_prices[row_index]
        dex5_price = dex5_prices[row_index]
        dex30_price = dex30_prices[row_index]
        gas_base_fee_wei = _to_float(record.get(gas_base_fee_wei_key))
        gas_base_fee_gwei = (
            None if gas_base_fee_wei is None else gas_base_fee_wei / 1_000_000_000.0
        )

        realized_vol = _realized_vol_annualized(
            cex_prices,
            row_index,
            window=realized_vol_window,
            annualization_minutes=annualization_minutes,
        )

        prev_cex = cex_prices[row_index - 1] if row_index > 0 else None
        prev_dex5 = dex5_prices[row_index - 1] if row_index > 0 else None
        prev_dex30 = dex30_prices[row_index - 1] if row_index > 0 else None

        enriched = {
            **record,
            "coinbase_log_price": _log_price(cex_price),
            "uniswap5_log_price": _log_price(dex5_price),
            "uniswap30_log_price": _log_price(dex30_price),
            "coinbase_log_return": _log_return(cex_price, prev_cex),
            "uniswap5_log_return": _log_return(dex5_price, prev_dex5),
            "uniswap30_log_return": _log_return(dex30_price, prev_dex30),
            "wedge_5_price_diff": _wedge_price_diff(dex5_price, cex_price),
            "wedge_30_price_diff": _wedge_price_diff(dex30_price, cex_price),
            "wedge_5_bps": _wedge_bps_from_logs(dex5_price, cex_price),
            "wedge_30_bps": _wedge_bps_from_logs(dex30_price, cex_price),
            "gas_base_fee_gwei": gas_base_fee_gwei,
            "gas_usd": gas_usd_series[row_index],
            "congestion_30d_pct": congestion_series[row_index],
            "uniswap5_fee_tier_bps": 5,
            "uniswap30_fee_tier_bps": 30,
            # Alias for readability while preserving existing age field names.
            "uniswap5_staleness_min": record.get("uniswap5_age_since_last_trade_min"),
            "uniswap30_staleness_min": record.get("uniswap30_age_since_last_trade_min"),
            "realized_vol_annualized": realized_vol,
        }
        output.append(enriched)

    return output


__all__ = ["build_dataset_rows"]
