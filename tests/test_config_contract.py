"""Tests for config and CLI contract in Ticket 1."""

from __future__ import annotations

import pytest

from ingestion.cli import main
from ingestion.config import load_config
from ingestion.logging import get_logger


def test_config_defaults_and_env_names() -> None:
    config = load_config(
        start_time_utc="2025-01-01T00:00:00Z",
        end_time_utc="2025-01-01T00:01:00Z",
    )
    assert config.output_dir == "data/processed"
    assert config.uniswap_graph_api_key_env == "UNISWAP_GRAPH_API_KEY"
    assert config.uniswap_graph_subgraph_id_env == "UNISWAP_V3_MAINNET_SUBGRAPH_ID"
    assert config.coinbase_api_key_env == "COINBASE_API_KEY"
    assert config.ethereum_rpc_url_env == "ETHEREUM_RPC_URL"


def test_config_requires_window_fields() -> None:
    with pytest.raises((TypeError, ValueError)):
        load_config(start_time_utc="2025-01-01T00:00:00Z")


def test_config_rejects_inverted_window() -> None:
    with pytest.raises(ValueError):
        load_config(
            start_time_utc="2025-01-01T00:02:00Z",
            end_time_utc="2025-01-01T00:01:00Z",
        )


def test_cli_help_exits_zero() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_cli_placeholder_command_exits_zero() -> None:
    result = main(
        [
            "placeholder",
            "--start-time-utc",
            "2025-01-01T00:00:00Z",
            "--end-time-utc",
            "2025-01-01T00:01:00Z",
        ]
    )
    assert result == 0


def test_logger_reuses_single_handler() -> None:
    logger_one = get_logger(name="ingestion.test")
    handler_count = len(logger_one.handlers)

    logger_two = get_logger(name="ingestion.test")

    assert logger_one is logger_two
    assert len(logger_two.handlers) == handler_count == 1
