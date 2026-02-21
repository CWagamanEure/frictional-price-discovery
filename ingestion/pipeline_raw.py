"""Raw ingestion runner that writes source outputs to data/raw."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.logging import get_logger
from ingestion.sources.coinbase import (
    UrllibCoinbaseClient,
    fetch_coinbase_candles,
)
from ingestion.sources.coinbase import (
    observations_to_records as coinbase_observations_to_records,
)
from ingestion.sources.ethereum_rpc import (
    UrllibEthereumRPCClient,
    fetch_basefee_observations,
)
from ingestion.sources.ethereum_rpc import (
    observations_to_records as ethereum_observations_to_records,
)
from ingestion.sources.uniswap_graph import (
    UrllibGraphClient,
    fetch_pool_swaps_raw,
    resolve_graph_endpoint,
)


@dataclass(frozen=True)
class RawIngestionResult:
    """Summary of one raw ingestion run."""

    run_id: str
    files: dict[str, str]
    row_counts: dict[str, int]


def _run_id(now: datetime | None = None) -> str:
    dt = (now or datetime.now(UTC)).astimezone(UTC)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_parquet(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)


def _persist_records(
    *,
    base_path: Path,
    records: list[dict[str, Any]],
    raw_format: str,
) -> dict[str, str]:
    files: dict[str, str] = {}
    if raw_format in {"json", "both"}:
        json_path = base_path.with_suffix(".json")
        _write_json(json_path, records)
        files["json"] = str(json_path)
    if raw_format in {"parquet", "both"}:
        parquet_path = base_path.with_suffix(".parquet")
        _write_parquet(parquet_path, records)
        files["parquet"] = str(parquet_path)
    return files


def run_raw_ingestion(
    *,
    start_time_utc: datetime,
    end_time_utc: datetime,
    output_dir: str = "data/raw",
    graph_endpoint: str | None = None,
    graph_api_key: str | None = None,
    graph_subgraph_id: str | None = None,
    uniswap_pool_5_bps: str | None = None,
    uniswap_pool_30_bps: str | None = None,
    coinbase_product_id: str = "ETH-USD",
    coinbase_interval_seconds: int = 60,
    coinbase_base_url: str | None = None,
    rpc_url: str | None = None,
    rpc_mode: str = "auto",
    rpc_feehistory_blocks_per_request: int = 1024,
    rpc_progress_every_blocks: int = 1000,
    raw_format: str = "parquet",
) -> RawIngestionResult:
    """Run raw source ingestion and persist outputs under data/raw."""
    logger = get_logger()
    if raw_format not in {"json", "parquet", "both"}:
        raise ValueError("raw_format must be one of: json, parquet, both")
    run_id = _run_id()
    raw_dir = Path(output_dir)

    files: dict[str, str] = {}
    row_counts: dict[str, int] = {}

    endpoint = resolve_graph_endpoint(
        endpoint=graph_endpoint,
        api_key=graph_api_key,
        subgraph_id=graph_subgraph_id,
    )
    graph_client = UrllibGraphClient(endpoint=endpoint, api_key=graph_api_key)

    if uniswap_pool_5_bps:
        swaps_5 = fetch_pool_swaps_raw(
            graph_client,
            pool_id=uniswap_pool_5_bps,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
        )
        artifact_5 = _persist_records(
            base_path=raw_dir / f"uniswap_swaps_5bps_{run_id}",
            records=swaps_5,
            raw_format=raw_format,
        )
        for ext, path in artifact_5.items():
            files[f"uniswap_5bps_{ext}"] = path
        row_counts["uniswap_5bps"] = len(swaps_5)
        logger.info("raw ingestion wrote %s rows for uniswap_5bps", len(swaps_5))

    if uniswap_pool_30_bps:
        swaps_30 = fetch_pool_swaps_raw(
            graph_client,
            pool_id=uniswap_pool_30_bps,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
        )
        artifact_30 = _persist_records(
            base_path=raw_dir / f"uniswap_swaps_30bps_{run_id}",
            records=swaps_30,
            raw_format=raw_format,
        )
        for ext, path in artifact_30.items():
            files[f"uniswap_30bps_{ext}"] = path
        row_counts["uniswap_30bps"] = len(swaps_30)
        logger.info("raw ingestion wrote %s rows for uniswap_30bps", len(swaps_30))

    coinbase_client = UrllibCoinbaseClient(
        base_url=coinbase_base_url or UrllibCoinbaseClient.base_url
    )
    coinbase_rows = fetch_coinbase_candles(
        coinbase_client,
        product_id=coinbase_product_id,
        interval_seconds=coinbase_interval_seconds,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
    )
    coinbase_records = coinbase_observations_to_records(coinbase_rows)
    coinbase_artifacts = _persist_records(
        base_path=raw_dir / f"coinbase_{coinbase_product_id}_{run_id}",
        records=coinbase_records,
        raw_format=raw_format,
    )
    for ext, path in coinbase_artifacts.items():
        files[f"coinbase_{ext}"] = path
    row_counts["coinbase"] = len(coinbase_records)
    logger.info("raw ingestion wrote %s rows for coinbase", len(coinbase_records))

    if rpc_url:
        rpc_client = UrllibEthereumRPCClient(rpc_url=rpc_url)
        eth_rows = fetch_basefee_observations(
            rpc_client,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            rpc_mode=rpc_mode,
            feehistory_blocks_per_request=rpc_feehistory_blocks_per_request,
            progress_every_blocks=rpc_progress_every_blocks,
        )
        eth_records = ethereum_observations_to_records(eth_rows)
        eth_artifacts = _persist_records(
            base_path=raw_dir / f"ethereum_blocks_{run_id}",
            records=eth_records,
            raw_format=raw_format,
        )
        for ext, path in eth_artifacts.items():
            files[f"ethereum_rpc_{ext}"] = path
        row_counts["ethereum_rpc"] = len(eth_records)
        logger.info("raw ingestion wrote %s rows for ethereum_rpc", len(eth_records))

    run_log = {
        "run_id": run_id,
        "start_time_utc": start_time_utc.isoformat().replace("+00:00", "Z"),
        "end_time_utc": end_time_utc.isoformat().replace("+00:00", "Z"),
        "raw_format": raw_format,
        "row_counts": row_counts,
        "files": files,
    }
    run_log_file = raw_dir / f"raw_ingestion_run_{run_id}.json"
    _write_json(run_log_file, run_log)
    files["run_log"] = str(run_log_file)
    logger.info("raw ingestion run log written to %s", run_log_file)

    return RawIngestionResult(run_id=run_id, files=files, row_counts=row_counts)
