# Method Spec: Execution Frictions and Cross-Venue Price Discovery (ETH)

## 0. Objective

Quantify how time-varying execution frictions affect:

1. **Cross-venue adjustment speed** of the DEX–CEX wedge (error correction / mean reversion).
2. **Information leadership** across venues (price discovery migration toward CEX during high congestion).

---

## 1. Market Scope and Sampling

- **Asset:** ETH  
- **DEX:** Uniswap v3 WETH/USDC (Ethereum mainnet)
  - Fee tiers: 0.05% (5 bps), 0.30% (30 bps)
- **CEX:** Coinbase ETH/USD
- **Frequency:** 1-minute bins (UTC)
- **Gas:** block-level base fee aggregated to minute (median/mean; configurable)

All variables are aligned to the same 1-minute timestamps. Within each minute \(t\), the “close” is the last observation within that minute.

---

## 2. Variables and Construction

### 2.1 Prices and Returns

Let end-of-minute midprices be \(P_t^{CEX}\) and \(P_t^{DEX}\). Define log prices:

\[
p_t^{CEX}=\log(P_t^{CEX}), \qquad p_t^{DEX}=\log(P_t^{DEX})
\]

Minute log returns:

\[
r_t^{CEX}=p_t^{CEX}-p_{t-1}^{CEX}, \qquad r_t^{DEX}=p_t^{DEX}-p_{t-1}^{DEX}
\]

### 2.2 Wedge / Basis

Define the signed wedge:

\[
b_t = p_t^{DEX}-p_t^{CEX}
\]

Convert to basis points:

\[
b_t^{bps}=10{,}000\cdot b_t, \qquad |b_t|^{bps}=|b_t^{bps}|
\]

If the two price series are cointegrated, the wedge (or wedge relative to a slowly varying target) is mean-reverting.

### 2.3 On-Chain Friction Measures

#### 2.3.1 Gas (USD)

Let \(g_t\) be an effective gas price in gwei at minute \(t\) (e.g., base fee or base+priority), \(ETHUSD_t\) be ETH price in USD, and \(GU\) a constant gas-units assumption for an “arb-relevant” transaction:

\[
GasUSD_t = GU \cdot g_t \cdot 10^{-9}\cdot ETHUSD_t
\]

#### 2.3.2 Congestion (Relative Measure)

To avoid sensitivity to slow-moving baseline changes, define congestion as a rolling percentile rank of `gas_usd` in a trailing window \(W(t)\) (default: 30 days):

\[
Congestion_t=\text{PctRank}_{u\in W(t)}(GasUSD_u)\in[0,1]
\]

### 2.4 Liquidity, Flow, Volatility, and Quote Quality

- **DEX liquidity near price:** `depth_usd_t` (e.g., within ±N bps/ticks of current price); use \(\log(depth_usd_t)\).
- **Flow-based impact proxy:** rolling minute USD flow, `flow_usd_roll_t`.
- **Realized volatility:** rolling window realized volatility of CEX (or mid) returns, `rv_t`.
- **CEX spread proxy (optional):** `cex_spread_bps_t`.
- **DEX quote staleness (required):**
  - `dex_stale_seconds_t` = seconds since last swap / last pool state update.
  - Use \(\log(1+dex\_stale\_seconds_t)\) as a control.

---

## 3. Fixed Effects and Standard Errors

### 3.1 Fixed Effects

Include:
- hour-of-day fixed effects \(\delta_{h(t)}\)
- day-of-week fixed effects \(\delta_{d(t)}\)
- month fixed effects \(\delta_{m(t)}\) (or week fixed effects)

### 3.2 Standard Errors

Minute data is serially correlated. Use HAC/Newey–West standard errors (e.g., 60-minute lag as a baseline), and report robustness to alternative lag lengths.

---

## 4. Primary Test 1: Congestion-Dependent Error Correction

Define \(\Delta b_t^{bps}=b_t^{bps}-b_{t-1}^{bps}\).

### 4.1 Baseline ECM (Mean Reversion)

\[
\Delta b_t^{bps}=\alpha+\phi\, b_{t-1}^{bps}+\gamma'X_{t-1}+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t
\]

Interpretation:
- \(\phi<0\) implies mean reversion of the wedge.

### 4.2 Congestion Interaction (Primary Identification)

\[
\Delta b_t^{bps}=\alpha+\phi\, b_{t-1}^{bps}
+\kappa\left(b_{t-1}^{bps}\times Congestion_{t-1}\right)
+\gamma'X_{t-1}+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t
\]

Interpretation:
- effective correction slope at congestion level \(c\): \(\phi+\kappa c\)
- slower convergence under congestion corresponds to \(\phi+\kappa c\) being closer to 0 at high \(c\)
- with \(\phi<0\), slower convergence typically implies \(\kappa>0\)

### 4.3 Half-Life Reporting

Approximate local AR(1) behavior \(b_t \approx (1+\lambda(c))b_{t-1}\), where \(\lambda(c)=\phi+\kappa c\).

Half-life (minutes):

\[
HL(c)=\frac{\ln(0.5)}{\ln\left|1+\lambda(c)\right|}
\]

Report \(HL\) at low vs high congestion (e.g., \(c=p10\) and \(c=p90\)).

---

## 5. Primary Test 2: Price Discovery / Information Leadership Shift

### 5.1 Lead–Lag Regression (DEX as Dependent)

\[
r_t^{DEX}
= a
+\sum_{k=1}^{K}\beta_k r_{t-k}^{CEX}
+\sum_{k=1}^{K}\gamma_k r_{t-k}^{DEX}
+\sum_{k=1}^{K}\delta_k \left(r_{t-k}^{CEX}\times Congestion_{t-1}\right)
+\psi'X_{t-1}
+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}
+\epsilon_t
\]

Interpretation:
- \(\delta_k>0\) indicates lagged CEX returns have increased predictive content for DEX returns under congestion, consistent with a shift of price discovery toward the CEX.

---

## 6. Supporting Construct: Implied No-Arbitrage Cost Band 

### 6.1 Band Construction (bps)

\[
C_t = C_t^{gas}+C^{dexfee}+C_t^{impact}+C^{cex}+C_t^{mev}
\]

Components (example implementation):
- **DEX fee tier:** \(C^{dexfee}\in\{5,30\}\) bps
- **CEX execution cost:** taker fee bps plus optional half-spread
- **Gas cost (bps of notional):**
  \[
  C_t^{gas}=10{,}000\cdot \frac{GasUSD_t}{Q}
  \]
- **Impact proxy:** flow/depth-based proxy per tier (parameterized and sensitivity-tested)
- Possible MEV proxy

### 6.2 Violations and Severity 

\[
V_t=\mathbf{1}\{|b_t|^{bps}>C_t\}
\]

\[
M_t=\max\left(0,\ |b_t|^{bps}-C_t\right)
\]

Usage:
- descriptive plots (violation rate and severity by congestion decile)
- robustness conditioning (e.g., re-estimate ECM on \(V_t=1\) minutes)

**Mechanical dependence caution:** Because \(C_t\) is constructed from frictions, regressions of \(V_t\) on the same frictions can be mechanically biased. Cost-band analysis is therefore restricted to descriptive reporting and robustness conditioning.

---

## 7. Estimation Details

### 7.1 Per-Tier Estimation

All primary models are estimated **separately** by fee tier (5 bps and 30 bps). 

### 7.2 Transformations

- Use \(\log(1+x)\) for heavy-tailed series (e.g., flow, staleness, gas_usd).
- Standardize key continuous regressors as needed for interpretability, while preserving sign.

### 7.3 Staleness Robustness

Run primary regressions under:
1. full sample (with staleness control included)
2. minutes with at least one swap
3. minutes with staleness below a threshold (e.g., ≤120 seconds)

---

## 8. Robustness Checklist

1. Congestion definition: rolling percentile vs z-score vs \(\log(1+GasUSD)\)
2. Lag structure: use \(X_{t-1}\) vs \(X_t\); vary \(K\) in lead–lag regressions
3. Time aggregation: 1-minute baseline vs alternative binning (e.g., 5-minute)
4. DEX price definition: pool mid vs swap VWAP (if available)
5. Impact proxy sensitivity: notional \(Q\), exponent \(\alpha\), scale \(k\), floor
6. Gas calibration sensitivity: \(GU\) ±25%
7. Regime splits: top 10% vs bottom 10% congestion; high/low volatility; high/low depth

---

## 9. Fields (Per Minute, Per Tier)

Required:
- `timestamp` (UTC minute)
- `P_cex_mid` (or `p_cex`)
- `P_dex_mid` (or `p_dex`)
- `base_fee_gwei` (and optionally `priority_fee_gwei`)
- `ethusd` (or a consistent ETHUSD proxy)
- `depth_usd` (DEX liquidity near price)
- `flow_usd` or rolling variant
- `dex_stale_seconds`
- `rv` (realized volatility)
- fixed-effect keys: hour, day-of-week, month (or week)

Optional:
- `cex_spread_bps`, `cex_volume`, depth proxies
- DEX swap count/volume

---

## 10. Regression 

Construct:
- \(b_t^{bps}=10{,}000(\log P_t^{DEX}-\log P_t^{CEX})\)
- \(\Delta b_t^{bps}=b_t^{bps}-b_{t-1}^{bps}\)
- \(Congestion_t=\text{PctRank}_{u\in W(t)}(GasUSD_u)\)

Run per tier:
1. Baseline ECM:
   \[
   \Delta b_t^{bps}=\alpha+\phi b_{t-1}^{bps}+\gamma'X_{t-1}+FE+\varepsilon_t
   \]
2. ECM with congestion interaction (primary):
   \[
   \Delta b_t^{bps}=\alpha+\phi b_{t-1}^{bps}+\kappa(b_{t-1}^{bps}\times Congestion_{t-1})+\gamma'X_{t-1}+FE+\varepsilon_t
   \]
3. Lead–lag price discovery:
   \[
   r_t^{DEX}=a+\sum_{k=1}^K\beta_k r_{t-k}^{CEX}+\sum_{k=1}^K\gamma_k r_{t-k}^{DEX}+\sum_{k=1}^K\delta_k(r_{t-k}^{CEX}\times Congestion_{t-1})+\psi'X_{t-1}+FE+\epsilon_t
   \]

