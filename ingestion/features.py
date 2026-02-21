"""Feature engineering for aligned minute-level market data."""

from __future__ import annotations

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


def _basis_bps(dex_price: float | None, cex_price: float | None) -> float | None:
    if dex_price is None or cex_price is None or cex_price <= 0:
        return None
    return ((dex_price - cex_price) / cex_price) * 10_000.0


def _gas_gwei(gas_base_fee_wei: float | None) -> float | None:
    if gas_base_fee_wei is None or gas_base_fee_wei < 0:
        return None
    return gas_base_fee_wei / 1_000_000_000.0


def _cost_band_proxy_bps(
    fee_tier_bps: float,
    gas_base_fee_wei: float | None,
    *,
    gas_weight_bps_per_gwei: float,
) -> float:
    gas_gwei = _gas_gwei(gas_base_fee_wei)
    if gas_gwei is None:
        return fee_tier_bps
    return fee_tier_bps + (gas_gwei * gas_weight_bps_per_gwei)


def _violation(
    basis_bps: float | None,
    threshold_bps: float,
) -> tuple[bool, float]:
    if basis_bps is None:
        return False, 0.0

    magnitude = max(abs(basis_bps) - threshold_bps, 0.0)
    return magnitude > 0.0, magnitude


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


def compute_features(
    aligned_records: list[dict[str, Any]],
    *,
    cex_price_key: str = "coinbase_close",
    dex_price_5_key: str = "uniswap5_token0_price",
    dex_price_30_key: str = "uniswap30_token0_price",
    gas_base_fee_wei_key: str = "gas_base_fee_per_gas_wei",
    gas_weight_bps_per_gwei: float = 0.02,
    realized_vol_window: int = 30,
    annualization_minutes: int = 525_600,
) -> list[dict[str, Any]]:
    """Compute basis, cost-band proxies, violations, and realized vol."""
    output: list[dict[str, Any]] = []
    cex_prices: list[float | None] = []

    for record in aligned_records:
        cex_price = _to_float(record.get(cex_price_key))
        dex_5 = _to_float(record.get(dex_price_5_key))
        dex_30 = _to_float(record.get(dex_price_30_key))
        gas_base_fee_wei = _to_float(record.get(gas_base_fee_wei_key))

        cex_prices.append(cex_price)

        basis_5_bps = _basis_bps(dex_5, cex_price)
        basis_30_bps = _basis_bps(dex_30, cex_price)

        implied_band_5_bps = _cost_band_proxy_bps(
            5.0,
            gas_base_fee_wei,
            gas_weight_bps_per_gwei=gas_weight_bps_per_gwei,
        )
        implied_band_30_bps = _cost_band_proxy_bps(
            30.0,
            gas_base_fee_wei,
            gas_weight_bps_per_gwei=gas_weight_bps_per_gwei,
        )

        violation_5, violation_5_mag_bps = _violation(basis_5_bps, implied_band_5_bps)
        violation_30, violation_30_mag_bps = _violation(
            basis_30_bps,
            implied_band_30_bps,
        )

        row_index = len(cex_prices) - 1
        realized_vol = _realized_vol_annualized(
            cex_prices,
            row_index,
            window=realized_vol_window,
            annualization_minutes=annualization_minutes,
        )

        enriched = {
            **record,
            "basis_5_bps": basis_5_bps,
            "basis_30_bps": basis_30_bps,
            "basis_spread_bps": (
                None
                if basis_5_bps is None or basis_30_bps is None
                else basis_30_bps - basis_5_bps
            ),
            "implied_band_5_bps": implied_band_5_bps,
            "implied_band_30_bps": implied_band_30_bps,
            "violation_5": violation_5,
            "violation_30": violation_30,
            "violation_5_mag_bps": violation_5_mag_bps,
            "violation_30_mag_bps": violation_30_mag_bps,
            "realized_vol_annualized": realized_vol,
        }
        output.append(enriched)

    return output
