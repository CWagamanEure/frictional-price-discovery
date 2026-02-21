# Execution Frictions and Cross-Venue Price Discovery in Crypto Markets 

## Research Question

- **How do execution frictions shape the implied no-arbitrage band and the speed of cross-venue price adjustment, and does price discovery migrate off-chain during congestion?**

- We study whether observed cross-venue wedge deviations:

1. Lie within the implied arbitrage cost band.
2. Widen when execution costs increase.
3. Close more slowly during congestion.

## Economic Framework

This project is grounded in:

1. Law of One Price with Limits-to-Arbitrage
2. Cointegration and Error-Correction Dynamics
3. Market Microstructure Theory
4. Friction-Dependent Price Discovery

If CEX and DEX prices are non-stationary but cointegrated, then the inter-exchange wedge is a mean-reverting process. Execution costs may widen the no-arbitrage band and slow the speed of convergence.

## Empirical Strategy

### Stage 1: Reduced-Form Analysis

1. **No-Arbitrage Band Violations**

$$Violations_t = f(Gas, Liquidity, Volatility, Fees, MEV. Time FE)$$

Questions:

1. Which frictions increase the probability or magnitude of band violations?

2. **Error-Correction / Mean Reversion**

$$Δb_t = αb_{t-1} + βFrictions_t + Controls + ϵ_t$$

Questions:

1. Does wedge close more slowly when gas is high?
2. Does adjustment speed vary across fee tiers?
3. Does price discovery shift toward CEX during congestion?

### Stage 2: Structural Extension

Estimate a state-space model of latent efficient price to quantify:

1. Friction-dependent observation noise
2. Information share shifts
3. Regime-dependent adjustment dynamics

## Dataset

### Keys

- `timestamp`
- `block_number`

### Prices

- `CEX mid price`
- `DEX mid price`

### Derived Variables

- `wedge` = DEX mid - CEX mid
- `abs_wedge`
- `wedge_sign`
- `log returns`

### Execution Frictions

#### DEX

- `Base fee` (gwei)
- `Priority fee` (gwei)
- `Fee tier` (e.g. 5bps / 30bps)
- `Liquidity near price`
- `Swap volume`
- `Realized slippage` (price impact proxy)

#### CEX

- `Taker fee`
- `Spread`
- `Bid depth`
- `Ask depth`
- `Trade volume`
-`Price impact proxy`

### Market Conditions

- `Realized Volatility`
- `Congestion regime indicator`
- `Hour of day fixed effects`
- `Weekday fixed effects`

## Core Tests

1. Does observed wedge remain within implied arbitrage cost band?

Compare: Observed wedge vs Implied arb cost = Gas + DEX fee + Slippage + CEX taker fee + MEV

2. Do frictions widen deviations?

Regression of violation magnitude on frictions.

3. Does congestions slow error correction?

Estimate speed of mean reversion conditional on gas, liquidity, volatility.

4. Does price discovery migrate off-chain during congestion?

Measure:
1. Relative return leadership
2. Adjustment half-life
3. Information share shifts

## Expected Contributions

- Empirical evidence on friction-dependent arbitrage bands.
- Measurement of congestion-induced slowdown in cross-venue convergence.
- Evidence on migration of price discovery between DEXs and CEXs
- Foundation for structural latent price modeling.


## Definitions and scope

- Here, I will define the various terms that I will be throwing around in this research project, along with the exact specifications of which markets and token will be researched. 

- **Wedge $w_t$** - the difference between DEX and CEX prices for the same asset at time t.

$w_t := p_t^{DEX} - p_t^{CEX}$

- **Implied-arbitrage band $C_t$** - An estimate of total round-trip execution costs required to  move price pressure across venues (gas + DEX fees + price impact/slippage + CEX taker fee + MEV penalty).

$C_t := C_t^{gas} + C_t^{dex fee} + C_t^{impact} + C_t^{cex fee} + C_t^{mev}$

Where "within-band" means $|b_t| \leq C_t$

- **Violation** - Where the wedge exceeds the no-arbitrage band. 

- Violation indicator: $V_t := 1{|b_t| > C_t}$

- Violation magnitude: $M_t := max(0,|b_t| - C_t)$

- **Reversion Equation** - 

If $P^{DEX}$ and $p^{CEX}$ are cointegrated:

$Δp_t^{DEX} = α * (p_{t-1}^{DEX} - p_{t-1}^{CEX}) + β'X_t + ϵ_t$

- **Market Scope** - 

We study WETH/USDC 0.05% and 0.30% fee tiers on Uniswap v3 (Ethereum Mainnet) as the DEX venue, and ETH USD mid prices from Coinbase. We will use 1-minute sampling for the core regressions, in order to capture congestion and frictions most effectively. It also avoids the very noisy microstructure data of 1s. Block level gas data and on-chain features will be aggregated to the minute. 

## Development Commands

Use Poetry for local tooling and tests:

```bash
poetry install
poetry run pytest -q
poetry run ruff check .
poetry run ruff format .
```

### Pipeline Commands

```bash
make daily MODE=provisional COLLECTION_MODE=synthetic START_DATE=2025-01-01 END_DATE=2025-01-01
make backfill MODE=provisional COLLECTION_MODE=synthetic START_DATE=2025-01-01 END_DATE=2025-01-31
make backfill MODE=provisional COLLECTION_MODE=live START_DATE=2025-01-01 END_DATE=2025-01-31
```

Use `COLLECTION_MODE=required_raw` (or omit it) when raw partitions are already landed.
Use `COLLECTION_MODE=live` for real data collection and set `ALCHEMY_API_KEY` in your environment.

You can also put the key in a local `.env` file (see `.env.example`).
