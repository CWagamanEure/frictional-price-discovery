# Execution Frictions and Cross-Venue Price Discovery in Crypto Markets 

## Research Question

- **How do execution frictions shape the implied no-arbitrage band and the speed of cross-venue price adjustment, and does price discovery migrate off-chain during congestion?**

- We study whether observed cross-venue basis deviations:

1. Lie within the implied arbitrage cost band.
2. Widen when execution costs increase.
3. Close more slowly during congestion.

## Economic Framework

This project is grounded in:

1. Law of One Price with Limits-to-Arbitrage
2. Cointegration and Error-Correction Dynamics
3. Market Microstructure Theory
4. Friction-Dependent Price Discovery

If CEX and DEX prices are non-stationary but cointegrated, then the inter-exchange basis is a mean-reverting process. Execution costs may widen the no-arbitrage band and slow the speed of convergence.

## Empirical Strategy

### Stage 1: Reduced-Form Analysis

1. **No-Arbitrage Band Violations**

$$Violations_t = f(Gas, Liquidity, Volatility, Fees, MEV. Time FE)$$

Questions:

1. Which frictions increase the probability or magnitude of band violations?

2. **Error-Correction / Mean Reversion**

$$Δb_t = αb_{t-1} + βFrictions_t + Controls + ϵ_t$$

Questions:

1. Does basis close more slowly when gas is high?
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

- `basis` = DEX mid - CEX mid
- `abs_basis`
- `basis_sign`
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

1. Does observed basis remain within implied arbitrage cost band?

Compare: Observed basis vs Implied arb cost = Gas + DEX fee + Slippage + CEX taker fee + MEV

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






