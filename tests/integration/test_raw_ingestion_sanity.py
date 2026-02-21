"""Opt-in live sanity checks for raw ingestion sources.

These tests are intentionally integration-only and require explicit env vars.
Run with: `pytest -m integration tests/integration/test_raw_ingestion_sanity.py`
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from urllib import error

import pytest

from ingestion.sources.coinbase import UrllibCoinbaseClient, fetch_coinbase_candles
from ingestion.sources.ethereum_rpc import (
    UrllibEthereumRPCClient,
    aggregate_basefee_to_minutes,
    fetch_basefee_observations,
)
from ingestion.sources.uniswap_graph import (
    UrllibGraphClient,
    fetch_pool_swaps_raw,
    resolve_graph_endpoint,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"missing required env var: {name}")
    return value


def _optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _graph_api_key() -> str | None:
    return _optional_env("UNISWAP_GRAPH_API_KEY") or _optional_env("GRAPH_API_KEY")


def _ethereum_rpc_url() -> str:
    rpc_url = _optional_env("ETHEREUM_RPC_URL")
    if rpc_url:
        return rpc_url

    alchemy_key = _optional_env("ALCHEMY_API_KEY")
    if alchemy_key:
        return f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"

    pytest.skip("set ETHEREUM_RPC_URL or ALCHEMY_API_KEY")
    return ""


def _window() -> tuple[datetime, datetime]:
    start_raw = os.getenv("INGEST_SANITY_START_UTC", "2024-06-01T00:00:00Z")
    end_raw = os.getenv("INGEST_SANITY_END_UTC", "2024-06-01T00:10:00Z")
    start = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(UTC)
    end = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).astimezone(UTC)
    if end <= start:
        pytest.skip("invalid window: INGEST_SANITY_END_UTC must be after start")
    return start, end


@pytest.mark.integration
def test_uniswap_live_raw_ingestion_sanity() -> None:
    graph_endpoint = _optional_env("UNISWAP_GRAPH_ENDPOINT")
    graph_api_key = _graph_api_key()
    graph_subgraph_id = _optional_env("UNISWAP_V3_MAINNET_SUBGRAPH_ID")

    try:
        endpoint = resolve_graph_endpoint(
            endpoint=graph_endpoint,
            api_key=graph_api_key,
            subgraph_id=graph_subgraph_id,
        )
    except ValueError:
        pytest.skip(
            "set UNISWAP_GRAPH_ENDPOINT or both UNISWAP_GRAPH_API_KEY and "
            "UNISWAP_V3_MAINNET_SUBGRAPH_ID"
        )

    pool_fee_5 = _optional_env("UNISWAP_POOL_ID_5_BPS")
    pool_fee_30 = _optional_env("UNISWAP_POOL_ID_30_BPS")
    if not pool_fee_5 and not pool_fee_30:
        pytest.skip("set UNISWAP_POOL_ID_5_BPS and/or UNISWAP_POOL_ID_30_BPS")

    start_time_utc, end_time_utc = _window()
    client = UrllibGraphClient(endpoint=endpoint, api_key=graph_api_key)

    total_rows = 0
    try:
        if pool_fee_5:
            rows_5 = fetch_pool_swaps_raw(
                client,
                pool_id=pool_fee_5,
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc,
            )
            total_rows += len(rows_5)

        if pool_fee_30:
            rows_30 = fetch_pool_swaps_raw(
                client,
                pool_id=pool_fee_30,
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc,
            )
            total_rows += len(rows_30)
    except error.HTTPError as exc:
        if exc.code in {401, 403}:
            pytest.fail(
                "Uniswap sanity check unauthorized/forbidden "
                f"(HTTP {exc.code}). Check Graph endpoint/API key permissions."
            )
        pytest.skip(f"network unavailable for Uniswap sanity check: {exc!r}")
    except error.URLError as exc:
        pytest.skip(f"network unavailable for Uniswap sanity check: {exc!r}")

    assert total_rows > 0


@pytest.mark.integration
def test_coinbase_live_raw_ingestion_sanity() -> None:
    product_id = os.getenv("COINBASE_PRODUCT_ID", "ETH-USD")
    interval_seconds = int(os.getenv("COINBASE_INTERVAL_SECONDS", "60"))
    base_url = os.getenv("COINBASE_BASE_URL", UrllibCoinbaseClient.base_url)

    start_time_utc, end_time_utc = _window()
    client = UrllibCoinbaseClient(base_url=base_url)

    try:
        rows = fetch_coinbase_candles(
            client,
            product_id=product_id,
            interval_seconds=interval_seconds,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
        )
    except error.HTTPError as exc:
        if exc.code in {401, 403}:
            pytest.fail(
                "Coinbase sanity check unauthorized/forbidden "
                f"(HTTP {exc.code}). Check egress policy or endpoint access."
            )
        pytest.skip(f"network unavailable for Coinbase sanity check: {exc!r}")
    except error.URLError as exc:
        pytest.skip(f"network unavailable for Coinbase sanity check: {exc!r}")

    assert len(rows) > 0


@pytest.mark.integration
def test_ethereum_rpc_live_raw_ingestion_sanity() -> None:
    rpc_url = _ethereum_rpc_url()
    start_time_utc, end_time_utc = _window()

    client = UrllibEthereumRPCClient(rpc_url=rpc_url)
    try:
        block_rows = fetch_basefee_observations(
            client,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
        )
    except error.HTTPError as exc:
        if exc.code in {401, 403}:
            pytest.fail(
                "Ethereum RPC sanity check unauthorized/forbidden "
                f"(HTTP {exc.code}). Check RPC key/project permissions."
            )
        pytest.skip(f"network unavailable for Ethereum RPC sanity check: {exc!r}")
    except error.URLError as exc:
        pytest.skip(f"network unavailable for Ethereum RPC sanity check: {exc!r}")
    minute_rows = aggregate_basefee_to_minutes(block_rows)

    assert len(block_rows) > 0
    assert len(minute_rows) > 0
