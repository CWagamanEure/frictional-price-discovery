# Data Ingestion Blueprint

This document translates `docs/method_spec.md` into an implementation-ready ingestion design for the Jan 2025 to Jan 2026 ETH/USDC study window.

## 1. Canonical conventions

- Time standard: `UTC`
- Base frequency: `1-minute` bins
- Minute labeling: `minute_ts` is the minute start timestamp (e.g., `12:34:00Z`)
- Minute close rule: within each minute, use the **last** observed value
- Wedge definition (canonical): `basis_bps = 10000 * (log_p_cex - log_p_dex)` (CEX minus DEX)
- Notional for cost conversions: `Q_USD` fixed per run (default proposal: `50000`)
- Pool granularity: run per fee tier (`5`, `30`, `100` bps), then optionally pooled

## 2. Data layers and storage

Use medallion-style layers with immutable raw snapshots.

- `data/raw/`: source-native pulls, partitioned by source/date
- `data/staging/`: cleaned + schema-normalized tables at native frequency
- `data/marts/`: 1-minute aligned feature tables for modeling

Recommended file format: `parquet` for staging/marts.  
Recommended partitioning:
- high-frequency tables: `dt=YYYY-MM-DD`
- block-level tables: `block_date=YYYY-MM-DD`

Run metadata table (one row per run):
- `run_id`, `run_started_at_utc`, `git_sha`, `config_hash`, `window_start`, `window_end`, `status`

## 3. Source map and ingestion tables

## 3.1 DEX (Uniswap v3, Ethereum mainnet)

Raw source entities:
- pool state events / observations to derive `sqrtPriceX96`
- swaps (for swap count/volume staleness control)
- pool liquidity/tick context to compute depth proxy near current price

Staging table: `stg_uniswap_pool_state`
- Keys: `chain_id`, `pool_address`, `block_number`, `tx_index`, `log_index`
- Columns:
  - `block_timestamp_utc` (timestamp)
  - `sqrt_price_x96` (numeric/string-safe)
  - `tick` (int)
  - `liquidity` (numeric)
  - `fee_tier_bps` (int)
  - `token0`, `token1`

Staging table: `stg_uniswap_swaps`
- Keys: `chain_id`, `pool_address`, `tx_hash`, `log_index`
- Columns:
  - `block_timestamp_utc`
  - `amount0`, `amount1`
  - `amount_usd` (nullable if computed later)
  - `sqrt_price_x96_post` (if available)

Minute table: `mrt_dex_minute`
- Keys: `minute_ts`, `pool_address`
- Columns:
  - `fee_tier_bps`
  - `p_dex_mid` (minute-close mid from `sqrtPriceX96`)
  - `log_p_dex`
  - `dex_swap_count`
  - `dex_swap_volume_usd`
  - `depth_usd_pm25bps` (liquidity near price; fixed window)
  - `dex_stale_minute_flag` (`1` if no swap in minute)

## 3.2 CEX (Coinbase ETH-USD)

Raw source entities:
- best bid/ask quotes (or top-of-book snapshots)
- trades (optional, for robustness)
- fee schedule input (taker fee assumption)

Staging table: `stg_coinbase_quotes`
- Keys: `event_ts_utc`, `sequence_id` (or source event id)
- Columns:
  - `bid_px`, `ask_px`
  - `bid_sz`, `ask_sz` (if available)
  - `product_id` (`ETH-USD`)

Minute table: `mrt_cex_minute`
- Keys: `minute_ts`
- Columns:
  - `p_cex_mid` (`(bid+ask)/2` at minute close)
  - `log_p_cex`
  - `cex_spread_bps` (`10000 * (ask-bid)/mid`)
  - `cex_bid_depth`, `cex_ask_depth` (nullable)
  - `cex_trade_volume_usd` (nullable)
  - `cex_taker_fee_bps` (config or schedule-derived)

## 3.3 Gas and ETHUSD for gas-cost conversion

Raw source entities:
- Ethereum base fee and priority fee observations
- ETHUSD minute price (can reuse CEX minute close)

Staging table: `stg_eth_fees`
- Keys: `block_number`
- Columns:
  - `block_timestamp_utc`
  - `base_fee_gwei`
  - `priority_fee_gwei` (median proxy if available)
  - `effective_gas_gwei` (`base + priority` when both available)

Minute table: `mrt_gas_minute`
- Keys: `minute_ts`
- Columns:
  - `base_fee_gwei_med`
  - `priority_fee_gwei_med`
  - `effective_gas_gwei_med`
  - `ethusd_close` (from `mrt_cex_minute.p_cex_mid` unless overridden)
  - `gas_usd` (`GU * gwei * 1e-9 * ethusd_close`)
  - `gas_cost_bps_q` (`10000 * gas_usd / Q_USD`)

## 4. Unified feature mart

Primary modeling table: `mrt_features_minute`

Primary key:
- `minute_ts`, `pool_address`

Core columns:
- IDs/time:
  - `minute_ts`, `pool_address`, `fee_tier_bps`
  - `hour_utc`, `dow_utc`, `month_utc`
- prices:
  - `p_cex_mid`, `p_dex_mid`, `log_p_cex`, `log_p_dex`
  - `r_cex_1m`, `r_dex_1m`
- spread/basis:
  - `basis_bps`
  - `abs_basis_bps`
  - `delta_basis_bps`
- cost-band components:
  - `c_gas_bps`
  - `c_dex_fee_bps` (`fee_tier_bps`)
  - `c_impact_bps` (`10000 * Q_USD / depth_usd_pm25bps`)
  - `c_cex_bps` (`cex_taker_fee_bps + 0.5 * cex_spread_bps`)
  - `c_mev_bps` (default `0`; optional proxy later)
  - `c_total_bps`
- outcomes:
  - `violation_flag` (`abs_basis_bps > c_total_bps`)
  - `violation_mag_bps` (`max(0, abs_basis_bps - c_total_bps)`)
- controls:
  - `depth_usd_pm25bps`
  - `cex_spread_bps`
  - `dex_swap_count`
  - `realized_vol_30m` (or chosen rolling window)
  - `congestion_pct_30d`
  - `congestion_z_30d` (robustness)
- quality:
  - `is_interpolated`
  - `is_late_arrival_revision`
  - `source_coverage_score`

## 5. Join keys and alignment rules

- DEX/CEX/gas join key: `minute_ts` in UTC
- DEX pool dimension join key: `pool_address`
- Fee tier from pool metadata (`fee_tier_bps`) should be carried into all downstream tables
- Minute-close selection:
  - for each source stream, choose last event where `event_ts >= minute_ts` and `< minute_ts + 1 minute`
- If no event in minute:
  - do not forward-fill prices in primary spec
  - emit nulls and mark quality flags; forward-fill only in explicitly labeled robustness marts

## 6. Late data and revision policy

Define two output modes:
- `provisional`: includes currently available data; can be revised
- `frozen`: immutable snapshot used for estimation

Policy:
- Recompute rolling window of last `N=3` days on each daily run to capture late arrivals
- Mark changed rows with:
  - `is_late_arrival_revision=1`
  - `revised_at_utc`
- Freeze cadence:
  - monthly freeze (e.g., freeze Jan 2025 on Feb 7, etc.)
- Never overwrite frozen partitions; write versioned paths or version column

## 7. Validation and observability checks

Run checks at staging and mart layers.

Schema checks:
- required columns present and typed
- key uniqueness (`minute_ts,pool_address` in marts)

Freshness checks:
- max timestamp within expected lag threshold
- expected coverage by date in study window

Range checks:
- `p_cex_mid > 0`, `p_dex_mid > 0`
- `cex_spread_bps >= 0`
- `depth_usd_pm25bps > 0` for impact calculation
- `congestion_pct_30d in [0,1]`

Distribution/anomaly checks:
- minute-to-minute return outlier rate above threshold
- share of `violation_flag=1` outside historical band
- share of null `p_cex_mid` / `p_dex_mid` by day

Reconciliation checks:
- Compare ETHUSD from gas conversion input vs CEX close divergence threshold
- Pool-tier coverage (5/30/100 bps) present for expected active periods

Output run report:
- rows ingested per table
- rows dropped for bad schema/range
- null rates for key features
- count of revised rows
- pass/fail by check

## 8. Backfill and incremental strategy

Backfill (one-time):
- Pull full history Jan 1, 2025 to Jan 31, 2026 (UTC)
- Build raw -> staging -> marts sequentially per month
- Validate monthly, then global reconciliation

Incremental (recurring):
- Daily append for new day partitions
- Rebuild last 3 days for late-arrival correction
- Recompute rolling features that depend on trailing windows:
  - `congestion_pct_30d`
  - volatility windows

## 9. Open decisions before coding

- Exact data providers/APIs for:
  - Uniswap state and swaps
  - Coinbase quotes/trades
  - gas/priority fee source
- Final `Q_USD` choice (`25k` vs `50k`)
- Gas units constant `GU` value (single constant vs scenario set)
- Whether primary mart excludes minutes with zero DEX swaps, or keeps with staleness flag
- Whether to include fee tier `100 bps` in baseline or robustness only

Once these are set, implementation can proceed with stable interfaces and tests.
