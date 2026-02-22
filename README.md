# Cross-Venue Frictions & Price Discovery in Crypto (CEX vs DEX)

This repo studies how **time-varying execution frictions** (gas, congestion, liquidity, market conditions) affect:

1) **Adjustment speed** of cross-venue price deviations (error correction / mean reversion)  
2) **Price discovery / information leadership** between a centralized exchange (CEX) and a decentralized exchange (DEX)

The analysis focuses on **dynamic efficiency** and **information flow** 

---

## Market scope

- **Asset:** ETH  
- **DEX:** Uniswap v3 WETH/USDC (Ethereum mainnet)  
  - Fee tiers: **0.05% (5 bps)** and **0.30% (30 bps)**  
- **CEX:** Coinbase ETH/USD  
- **Sampling:** 1-minute (UTC)  
- **Gas:** block-level base fee aggregated to minute 

---

## Core objects

Let end-of-minute midprices be \(P_t^{DEX}\) and \(P_t^{CEX}\). Define log prices:

- \(p_t^{DEX}=\log(P_t^{DEX})\)  
- \(p_t^{CEX}=\log(P_t^{CEX})\)

**Cross-venue wedge (signed, in bps):**
\[
b_t^{bps}=10{,}000\cdot(p_t^{DEX}-p_t^{CEX})
\]

**Returns:**
- \(r_t^{DEX}=p_t^{DEX}-p_{t-1}^{DEX}\)  
- \(r_t^{CEX}=p_t^{CEX}-p_{t-1}^{CEX}\)

**Frictions / controls (minute-level):**
- gas / congestion measures (e.g., `gas_usd`, rolling percentile)
- DEX liquidity near price (depth proxy)
- flow-based impact proxy (rolling minute flow)
- volatility (rolling realized)
- CEX volume proxies 
- **DEX staleness** (seconds since last swap; crucial with forward-filled DEX mid)

---

## Repo workflow

### 1) Environment

This repo uses Poetry.

```bash
poetry install
poetry run pytest -q
poetry run ruff check .
poetry run ruff format .
