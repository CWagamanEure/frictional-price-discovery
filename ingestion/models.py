"""Typed records used by ingestion sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class UniswapMinuteObservation:
    """Normalized Uniswap v3 minute observation from The Graph."""

    timestamp_utc: datetime
    pool_id: str
    fee_tier_bps: int
    token0_price: float
    token1_price: float
    volume_usd: float
    tvl_usd: float
    source: str = "the_graph"

    def to_record(self) -> dict[str, object]:
        """Convert observation into a serializable record."""
        record = asdict(self)
        record["timestamp_utc"] = self.timestamp_utc.isoformat()
        return record


@dataclass(frozen=True)
class CoinbaseCandleObservation:
    """Normalized Coinbase candle observation."""

    timestamp_utc: datetime
    product_id: str
    interval_seconds: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    source: str = "coinbase"

    def to_record(self) -> dict[str, object]:
        """Convert observation into a serializable record."""
        record = asdict(self)
        record["timestamp_utc"] = self.timestamp_utc.isoformat()
        return record


@dataclass(frozen=True)
class GasBasefeeObservation:
    """Block-level gas/basefee observation from Ethereum RPC."""

    block_number: int
    timestamp_utc: datetime
    base_fee_per_gas_wei: int
    gas_used: int
    gas_limit: int
    source: str = "ethereum_rpc"

    def to_record(self) -> dict[str, object]:
        """Convert observation into a serializable record."""
        record = asdict(self)
        record["timestamp_utc"] = self.timestamp_utc.isoformat()
        return record


@dataclass(frozen=True)
class MinuteGasObservation:
    """Minute-level gas/basefee aggregate from block observations."""

    minute_utc: datetime
    base_fee_per_gas_wei: int
    gas_used: int
    gas_limit: int
    block_number: int
    block_count: int
    source: str = "ethereum_rpc"

    def to_record(self) -> dict[str, object]:
        """Convert observation into a serializable record."""
        record = asdict(self)
        record["minute_utc"] = self.minute_utc.isoformat()
        return record
