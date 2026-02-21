# Method Spec

This research studies how **cross-venue trading frictions** affect (i) how often the **CEX–DEX price wedge** exceeds an **implied no-arbitrage cost band**, (ii) how quickly that wedge **mean-reverts**, and (iii) whether **price discovery shifts** toward the CEX during periods of high on-chain congestion.

The empirical design has two layers:

1. **Reduced-form regressions:** quantify how frictions correlate with (a) *violations* of the implied cost band and (b) *error-correction speed* (mean reversion) of the wedge.
2. **Structural extension:** a **state-space model** (efficient price + spread) estimated by **Kalman filtering** to separate a latent efficient price process from a latent stationary spread process and study how frictions alter spread dynamics and measurement noise.

---

## 1. Market scope

- **Asset / pair:** ETH/USDC  
- **DEX:** Uniswap v3 pools at multiple fee tiers (e.g., 5/30/100 bps)  
- **CEX:** Coinbase  
- **Sampling frequency:** 1-minute bins  
- **DEX price type:** pool mid (from pool state / `sqrtPriceX96`)  
- **Time window:** Jan 2025 – Jan 2026  
- **Time standardization:** all timestamps are aligned to 1-minute bins in a single time standard (e.g., UTC). Fixed effects (hour/day/month) use the same standard.

All variables are aligned to the same 1-minute timestamps. Within each minute $t$, the “close” value is the last observation within that minute.

---

## 2. Economic model and intuition

### 2.1 Law of one price with limits-to-arbitrage
Under limits-to-arbitrage, the cross-venue price wedge is allowed to persist **within** a cost band defined by execution costs and frictions. When the wedge exceeds this band, profitable arbitrage should pull prices back together, implying **mean reversion** in the wedge—potentially **slower** when congestion and execution frictions are high.

### 2.2 Cointegration framing (motivation)
Let $p_t^{CEX}$ and $p_t^{DEX}$ be log prices. If CEX and DEX prices are cointegrated, their difference can be stationary (mean-reverting). With time-varying frictions, the wedge may revert around a **time-varying target** related to costs rather than a constant mean.

---

## 3. Variable definitions

### 3.1 Prices and returns (1-minute)
Let $P_t^{CEX}$ and $P_t^{DEX}$ be end-of-minute midprices, and define log prices:
$$p_t^{CEX}=\log(P_t^{CEX}), \qquad p_t^{DEX}=\log(P_t^{DEX}).$$

Define 1-minute log returns:
$$r_t^{CEX}=p_t^{CEX}-p_{t-1}^{CEX}, \qquad r_t^{DEX}=p_t^{DEX}-p_{t-1}^{DEX}.$$

### 3.2 Wedge / cross-venue spread (in bps)
Define the signed wedge:
$$b_t=p_t^{CEX}-p_t^{DEX}.$$
Convert to bps:
$$b_t^{bps}=10{,}000\cdot b_t,$$
and define
$$|b_t|^{bps}=|b_t^{bps}|.$$

### 3.3 Implied arbitrage cost band (in bps)
Define an implied cost band $C_t$ representing approximate **one-way** costs required to execute cross-venue arbitrage (gas + fees + impact + other frictions). The band is constructed in **bps of notional**, requiring a fixed notional size.

Choose a fixed notional $Q$ (USD) (e.g., $Q=\$25k$ or $Q=\$50k$) used to convert gas and impact into bps.

Construct:
$$C_t=C_t^{gas}+C_t^{dexfee}+C_t^{impact}+C_t^{cex}+C_t^{mev}.$$

**(a) DEX fee tier**  
If the Uniswap v3 pool fee tier is $f$ bps:
$$C_t^{dexfee}=f.$$

**(b) CEX execution cost**  
Using CEX mid implies crossing half-spread for execution plus taker fees:
$$C_t^{cex}=\text{TakerFee}_{bps}+\frac{1}{2}\text{CEXSpread}_{bps,t}.$$
(If spread is unavailable, use taker fee only.)

**(c) Gas cost (bps of notional)**  
Let $g_t$ be effective gas price in gwei (e.g., base + priority, minute median), $ETHUSD_t$ be ETH price in USD at minute close, and $GU$ be gas units per arbitrage-relevant swap (constant or estimated). Then
$$GasUSD_t=GU\cdot g_t\cdot 10^{-9}\cdot ETHUSD_t,$$
and
$$C_t^{gas}=10{,}000\cdot \frac{GasUSD_t}{Q}.$$

**(d) Price impact / slippage proxy (pool-mid setting)**  
Because pool mid does not directly give execution price, use a depth-based proxy. Let $DepthUSD_t$ be DEX liquidity “near price” converted to USD within a fixed window (e.g., $\pm 25$ bps around the current tick). Define:
$$C_t^{impact}=10{,}000\cdot \frac{Q}{DepthUSD_t}.$$

**(e) MEV proxy**  
Baseline:
$$C_t^{mev}=0.$$
Robustness (example):
$$C_t^{mev}=\lambda \cdot \text{PriorityFee}_{gwei,t},$$
with $\lambda$ calibrated or used as sensitivity.

### 3.4 Violations and magnitude
A **violation** occurs when the wedge exceeds the implied band:
$$V_t=\mathbf{1}\{|b_t|^{bps}>C_t\}.$$
Violation magnitude (excess beyond the band):
$$M_t=\max(0,\ |b_t|^{bps}-C_t).$$

### 3.5 Congestion measure comparable across baseline regime shifts
Because baseline fees can drift over time, the main congestion variable is defined **relative to recent history** rather than in raw levels.

Let $W(t)$ denote the trailing 30-day window of minutes ending at $t$. Define the rolling percentile:
$$Congestion_t=\text{PctRank}_{u\in W(t)}(GasUSD_u)\in[0,1],$$
where $\text{PctRank}$ is the percentile rank of $GasUSD_t$ among $\{GasUSD_u: u\in W(t)\}$.

Robustness alternatives:
- rolling z-score: $CongestionZ_t=(GasUSD_t-\mu_{W(t)})/\sigma_{W(t)}$
- raw level: $\log(1+GasUSD_t)$

---

## 4. Econometric equations (reduced form)

### Fixed effects
All baseline regressions include:
- hour-of-day fixed effects: $\delta_{h(t)}$
- day-of-week fixed effects: $\delta_{d(t)}$
- month fixed effects: $\delta_{m(t)}$ (absorbs slow-moving baseline changes)

### 4.1 Part I — Frictions vs band violations
Let $X_t$ include:
- $Congestion_t$ (rolling percentile) as the primary congestion measure
- $\log(DepthUSD_t)$
- $Vol_t$ realized volatility (rolling window)
- $CEXSpread_{bps,t}$
- MEV proxy (if used)
- fee-tier controls (if pooling fee tiers)

#### (A) Main: linear probability model (LPM)
$$V_t=\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$

#### (B) Robustness: logit
$$\Pr(V_t=1\mid X_t)=\text{logit}^{-1}\!\left(\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}\right).$$

#### (C) Severity regression
$$\log(1+M_t)=\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$

**Mechanical dependence note:** since $V_t$ is defined using $C_t$, regressing $V_t$ on variables that also build $C_t$ can create mechanical relationships (band widens $\Rightarrow$ fewer violations by construction). The primary implementation is to run analyses **separately by fee tier** and interpret $X_t$ primarily as market conditions (liquidity, volatility, spreads, relative congestion). A pooled specification includes $C_t$ as a single summary regressor:
$$V_t=\alpha+\beta_C C_t+\beta'\tilde{X}_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$

### 4.2 Part II — Do frictions slow mean reversion (error correction)?
Define $\Delta b_t^{bps}=b_t^{bps}-b_{t-1}^{bps}$.

#### (A) Baseline mean reversion toward 0
$$\Delta b_t^{bps}=-\kappa\, b_{t-1}^{bps}+\gamma'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$
Interpretation: $\kappa>0$ implies mean reversion.

#### (B) Congestion-dependent correction speed (key test)
$$\Delta b_t^{bps}=-\kappa\, b_{t-1}^{bps}+\theta\left(b_{t-1}^{bps}\times Congestion_t\right)+\gamma'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$
Interpretation: $\theta<0$ means the pull back to 0 weakens when congestion is high relative to its recent baseline, implying slower error correction.

### 4.3 Part III — Price discovery shifts (reduced-form lead–lag)
$$r_t^{DEX}=\alpha+\sum_{k=1}^{K}\beta_k r_{t-k}^{CEX}+\sum_{k=1}^{K}\gamma_k r_{t-k}^{DEX}+\sum_{k=1}^{K}\delta_k\left(r_{t-k}^{CEX}\times Congestion_t\right)+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t.$$
If $\delta_k>0$, CEX leads DEX more during high relative congestion, consistent with off-chain price discovery gaining importance.

---

## 5. Structural extension (state-space / Kalman filtering)

### 5.1 States
We model a latent **efficient price** and a latent **spread/wedge**:
$$x_t=\begin{bmatrix}p_t^*\\ s_t\end{bmatrix}.$$

### 5.2 State dynamics
Efficient price as random walk:
$$p_t^*=p_{t-1}^*+\nu_t.$$
Spread as mean-reverting around a friction-driven target:
$$s_t-\mu_t=\phi\left(s_{t-1}-\mu_{t-1}\right)+\eta_t,\qquad |\phi|<1,$$
with
$$\mu_t=a+\lambda C_t.$$

Frictions enter by shifting $\mu_t$ through $C_t$ and by allowing the mean-reversion parameter $\phi$ to differ across bins of $Congestion_t$ (e.g., top decile vs bottom decile).

### 5.3 Measurement equations (observed quotes)
$$p_t^{CEX}=p_t^*+\frac{1}{2}s_t+\epsilon_t^{CEX}, \qquad p_t^{DEX}=p_t^*-\frac{1}{2}s_t+\epsilon_t^{DEX}.$$
Measurement noise is allowed to vary with market conditions (pool-mid staleness/measurement quality):
$$\mathrm{Var}(\epsilon_t^{DEX})=f(Congestion_t,DepthUSD_t,\text{DEXSwapCount}_t).$$

Outputs:
- filtered $\hat{p}_t^*$ and $\hat{s}_t$
- spread mean-reversion estimates by congestion bins
- comparison to reduced-form error-correction estimates for consistency

---

## 6. Estimation details

- **Time aggregation:** 1-minute bins; align CEX and DEX timestamps.
- **Fee tiers:** run separately by fee tier (preferred), and/or pooled with tier fixed effects.
- **Fixed effects:** $\delta_{h(t)}$, $\delta_{d(t)}$, $\delta_{m(t)}$.
- **Standard errors:** use HAC (Newey–West) or cluster by time blocks due to autocorrelation.
- **Transforms:** heavy-tailed variables use $\log(1+x)$ where appropriate.
- **Lags:** for endogeneity mitigation, use lagged frictions $X_{t-1}$ as robustness.
- **Staleness control (pool mid):** include $\log(1+\text{DEXSwapCount}_t)$ or restrict to minutes with at least one swap as a robustness check.

---

## 7. Data requirements (exact fields)

### Core (minimum)
- **DEX (per minute):** pool mid price $P_t^{DEX}$ (from `sqrtPriceX96`), fee tier, liquidity near price (to form $DepthUSD_t$)
- **CEX (per minute):** mid price $P_t^{CEX}$, bid/ask (for spread), taker fee (bps)
- **Gas (per minute):** base fee, priority fee (or effective gas price), plus ETHUSD price to compute $GasUSD_t$
- **Market conditions:** realized volatility $Vol_t$ (rolling window)
- **Fixed effects fields:** hour-of-day, day-of-week, month identifier

### Recommended (for robustness/interpretation)
- DEX swap count and swap volume (to address pool-mid staleness)
- CEX trade volume / depth
- DEX liquidity/volume share across pools (pool share)
- alternative DEX price construction (swap VWAP)

---

## 8. Identification assumptions (what is assumed / what is not claimed)

This paper primarily estimates **conditional correlations**. Interpretations rely on:

1. limits-to-arbitrage validity: deviations beyond the implied band are economically meaningful and should mean-revert absent frictions
2. measurement adequacy: the constructed band $C_t$ reasonably approximates true execution costs for a representative notional $Q$
3. baseline drift control: month fixed effects $\delta_{m(t)}$ absorb slow-moving shifts and $Congestion_t$ is defined relative to recent history
4. conditional exogeneity (limited): after controls and fixed effects, residual variation in frictions is informative about violations and adjustment speed; causal claims are not the primary objective
5. stationarity: either $b_t$ is stationary, or $b_t-\mu_t$ is stationary once costs are accounted for

---

## 9. Planned robustness checks

1. congestion definition: rolling percentile (main) vs rolling z-score vs $\log(1+GasUSD_t)$
2. time aggregation: 10s / 5m vs 1m
3. DEX price definition: pool mid vs swap VWAP
4. impact proxy: $Q/DepthUSD_t$ vs $\sqrt{Q/DepthUSD_t}$
5. gas calibration: vary $GU$ ±25%; vary notional $Q$
6. MEV proxy: exclude vs include proxy; compare sensitivity
7. directionality: signed violations ($b_t>0$ vs $b_t<0$)
8. regime splits: by $Congestion_t$ deciles (top 10% vs bottom 10%), high/low volatility, high/low liquidity
9. staleness: control for swap count/volume; restrict to minutes with swaps
10. two-part model for severity: logit for $V_t$, then OLS on $\log(1+M_t)$ conditional on $V_t=1$

---

## 10. Summary of exact equations run (for checklist)

**Constructed variables:**
- $b_t^{bps}=10{,}000(\log P_t^{CEX}-\log P_t^{DEX})$
- $C_t=C_t^{gas}+C_t^{dexfee}+C_t^{impact}+C_t^{cex}+C_t^{mev}$
- $V_t=\mathbf{1}\{|b_t|^{bps}>C_t\}$
- $M_t=\max(0,\ |b_t|^{bps}-C_t)$
- $Congestion_t=\text{PctRank}_{u\in W(t)}(GasUSD_u)$ with $W(t)$ trailing 30 days

**Regressions:**
1. $V_t=\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t$ (LPM)
2. $\Pr(V_t=1\mid X_t)=\text{logit}^{-1}(\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)})$ (Logit)
3. $\log(1+M_t)=\alpha+\beta'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t$ (Severity)
4. $\Delta b_t^{bps}=-\kappa b_{t-1}^{bps}+\theta(b_{t-1}^{bps}\times Congestion_t)+\gamma'X_t+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t$ (Error correction)
5. $r_t^{DEX}=\alpha+\sum_{k\le K}\beta_k r_{t-k}^{CEX}+\sum_{k\le K}\delta_k(r_{t-k}^{CEX}\times Congestion_t)+\sum_{k\le K}\gamma_k r_{t-k}^{DEX}+\delta_{h(t)}+\delta_{d(t)}+\delta_{m(t)}+\varepsilon_t$ (Price discovery)
