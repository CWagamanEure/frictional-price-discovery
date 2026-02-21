"""Configuration contract for ingestion workflows."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def parse_utc_datetime(value: datetime | str) -> datetime:
    """Parse a datetime-like value and normalize it to UTC."""
    if isinstance(value, str):
        raw = value.replace("Z", "+00:00")
        dt_value = datetime.fromisoformat(raw)
    else:
        dt_value = value

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=UTC)

    return dt_value.astimezone(UTC)


try:
    from pydantic import BaseModel, Field, field_validator, model_validator

    class AppConfig(BaseModel):
        """Typed runtime settings for ingestion jobs."""

        start_time_utc: datetime
        end_time_utc: datetime
        output_dir: str = Field(default="data/processed")
        uniswap_graph_api_key_env: str = Field(default="UNISWAP_GRAPH_API_KEY")
        uniswap_graph_subgraph_id_env: str = Field(
            default="UNISWAP_V3_MAINNET_SUBGRAPH_ID"
        )
        coinbase_api_key_env: str = Field(default="COINBASE_API_KEY")
        ethereum_rpc_url_env: str = Field(default="ETHEREUM_RPC_URL")
        log_level: str = Field(default="INFO")

        @field_validator("start_time_utc", "end_time_utc", mode="before")
        @classmethod
        def _parse_datetimes(cls, value: datetime | str) -> datetime:
            return parse_utc_datetime(value)

        @model_validator(mode="after")
        def _validate_window(self) -> "AppConfig":
            if self.end_time_utc <= self.start_time_utc:
                raise ValueError("end_time_utc must be later than start_time_utc")
            return self

except ModuleNotFoundError:

    @dataclass
    class AppConfig:
        """Fallback settings model used when pydantic is unavailable."""

        start_time_utc: datetime | str
        end_time_utc: datetime | str
        output_dir: str = "data/processed"
        uniswap_graph_api_key_env: str = "UNISWAP_GRAPH_API_KEY"
        uniswap_graph_subgraph_id_env: str = "UNISWAP_V3_MAINNET_SUBGRAPH_ID"
        coinbase_api_key_env: str = "COINBASE_API_KEY"
        ethereum_rpc_url_env: str = "ETHEREUM_RPC_URL"
        log_level: str = "INFO"

        def __post_init__(self) -> None:
            self.start_time_utc = parse_utc_datetime(self.start_time_utc)
            self.end_time_utc = parse_utc_datetime(self.end_time_utc)
            if self.end_time_utc <= self.start_time_utc:
                raise ValueError("end_time_utc must be later than start_time_utc")


def load_config(**kwargs: Any) -> AppConfig:
    """Build and validate application configuration."""
    return AppConfig(**kwargs)
