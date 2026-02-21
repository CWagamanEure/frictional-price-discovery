"""CLI skeleton for ingestion workflows."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from ingestion.config import load_config, parse_utc_datetime
from ingestion.export import export_records
from ingestion.features import compute_features
from ingestion.logging import get_logger
from ingestion.pipeline_align import build_aligned_from_raw_run
from ingestion.pipeline_full import run_full_pipeline
from ingestion.pipeline_processed import run_processed_pipeline
from ingestion.pipeline_raw import run_raw_ingestion
from ingestion.reporting import build_missingness_report, write_missingness_report
from ingestion.sources.coinbase import (
    UrllibCoinbaseClient,
    fetch_coinbase_candles,
)
from ingestion.sources.coinbase import (
    observations_to_records as coinbase_observations_to_records,
)
from ingestion.sources.ethereum_rpc import (
    UrllibEthereumRPCClient,
    aggregate_basefee_to_minutes,
    fetch_basefee_observations,
)
from ingestion.sources.uniswap_graph import (
    MAINNET_NETWORK_LABEL,
    UrllibGraphClient,
    fetch_pool_minutes,
    observations_to_records,
    resolve_graph_endpoint,
)
from ingestion.validation import ValidationError, enforce_validation


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for ingestion commands."""
    parser = argparse.ArgumentParser(prog="ingestion-cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    placeholder = subparsers.add_parser(
        "placeholder", help="Load and validate config without running ingestion."
    )
    placeholder.add_argument("--start-time-utc", required=True)
    placeholder.add_argument("--end-time-utc", required=True)
    placeholder.add_argument("--output-dir", default="data/processed")
    placeholder.add_argument("--log-level", default="INFO")

    uniswap_preview = subparsers.add_parser(
        "uniswap-preview",
        help="Fetch Uniswap v3 minute data for one pool and print row count.",
    )
    uniswap_preview.add_argument("--pool-id", required=True)
    uniswap_preview.add_argument("--fee-tier-bps", required=True, type=int)
    uniswap_preview.add_argument("--start-time-utc", required=True)
    uniswap_preview.add_argument("--end-time-utc", required=True)
    uniswap_preview.add_argument("--page-size", default=1000, type=int)
    uniswap_preview.add_argument("--graph-endpoint", default=None)
    uniswap_preview.add_argument("--graph-api-key", default=None)
    uniswap_preview.add_argument("--graph-subgraph-id", default=None)

    coinbase_preview = subparsers.add_parser(
        "coinbase-preview",
        help="Fetch Coinbase candles and print row count.",
    )
    coinbase_preview.add_argument("--product-id", required=True)
    coinbase_preview.add_argument("--interval-seconds", required=True, type=int)
    coinbase_preview.add_argument("--start-time-utc", required=True)
    coinbase_preview.add_argument("--end-time-utc", required=True)
    coinbase_preview.add_argument("--base-url", default=None)

    gas_preview = subparsers.add_parser(
        "gas-preview",
        help="Fetch Ethereum block basefee/gas and print block/minute row counts.",
    )
    gas_preview.add_argument("--rpc-url", required=True)
    gas_preview.add_argument("--start-time-utc", required=True)
    gas_preview.add_argument("--end-time-utc", required=True)
    gas_preview.add_argument(
        "--rpc-mode", default="auto", choices=["auto", "blocks", "feehistory"]
    )
    gas_preview.add_argument(
        "--rpc-feehistory-blocks-per-request",
        default=1024,
        type=int,
    )
    gas_preview.add_argument("--rpc-progress-every-blocks", default=1000, type=int)

    raw_ingest = subparsers.add_parser(
        "raw-ingest",
        help="Run raw source ingestion and write outputs under data/raw.",
    )
    raw_ingest.add_argument("--start-time-utc", required=True)
    raw_ingest.add_argument("--end-time-utc", required=True)
    raw_ingest.add_argument("--output-dir", default="data/raw")
    raw_ingest.add_argument("--graph-endpoint", default=None)
    raw_ingest.add_argument("--graph-api-key", default=None)
    raw_ingest.add_argument("--graph-subgraph-id", default=None)
    raw_ingest.add_argument("--uniswap-pool-5-bps", default=None)
    raw_ingest.add_argument("--uniswap-pool-30-bps", default=None)
    raw_ingest.add_argument("--coinbase-product-id", default="ETH-USD")
    raw_ingest.add_argument("--coinbase-interval-seconds", default=60, type=int)
    raw_ingest.add_argument("--coinbase-base-url", default=None)
    raw_ingest.add_argument("--rpc-url", default=None)
    raw_ingest.add_argument(
        "--rpc-mode", default="auto", choices=["auto", "blocks", "feehistory"]
    )
    raw_ingest.add_argument(
        "--rpc-feehistory-blocks-per-request",
        default=1024,
        type=int,
    )
    raw_ingest.add_argument("--rpc-progress-every-blocks", default=1000, type=int)
    raw_ingest.add_argument(
        "--raw-format",
        default="parquet",
        choices=["json", "parquet", "both"],
    )

    features_preview = subparsers.add_parser(
        "features-preview",
        help="Compute engineered features from aligned minute records JSON.",
    )
    features_preview.add_argument("--input-json", required=True)
    features_preview.add_argument("--output-json", default=None)
    features_preview.add_argument("--realized-vol-window", default=30, type=int)
    features_preview.add_argument("--annualization-minutes", default=525600, type=int)
    features_preview.add_argument("--gas-weight-bps-per-gwei", default=0.02, type=float)

    export_preview = subparsers.add_parser(
        "export-preview",
        help="Export JSON records to Parquet + metadata JSON.",
    )
    export_preview.add_argument("--input-json", required=True)
    export_preview.add_argument("--output-dir", default="data/processed")
    export_preview.add_argument("--dataset-name", required=True)
    export_preview.add_argument("--start-time-utc", default=None)
    export_preview.add_argument("--end-time-utc", default=None)

    validate_preview = subparsers.add_parser(
        "validate-preview",
        help="Run validation checks and write missingness report from JSON records.",
    )
    validate_preview.add_argument("--input-json", required=True)
    validate_preview.add_argument(
        "--report-json",
        default="data/processed/missingness_report.json",
    )
    validate_preview.add_argument("--fail-on-warnings", action="store_true")

    align_run = subparsers.add_parser(
        "align-run",
        help="Build data/interim aligned records from raw ingestion artifacts.",
    )
    align_run.add_argument("--raw-run-log", default=None)
    align_run.add_argument(
        "--output-json",
        default="data/interim/aligned_records.json",
    )
    align_run.add_argument(
        "--duplicate-policy", default="last", choices=["first", "last"]
    )

    process_run = subparsers.add_parser(
        "process-run",
        help="Run features + validation/report + parquet export from aligned JSON.",
    )
    process_run.add_argument("--input-json", required=True)
    process_run.add_argument("--output-dir", default="data/processed")
    process_run.add_argument("--dataset-name", default="features")
    process_run.add_argument("--realized-vol-window", default=30, type=int)
    process_run.add_argument("--annualization-minutes", default=525600, type=int)
    process_run.add_argument("--gas-weight-bps-per-gwei", default=0.02, type=float)
    process_run.add_argument("--fail-on-warnings", action="store_true")

    full_run = subparsers.add_parser(
        "full-run",
        help="Run raw-ingest -> align-run -> process-run in one command.",
    )
    full_run.add_argument("--start-time-utc", required=True)
    full_run.add_argument("--end-time-utc", required=True)
    full_run.add_argument("--raw-output-dir", default="data/raw")
    full_run.add_argument(
        "--interim-output-json",
        default="data/interim/aligned_records.json",
    )
    full_run.add_argument("--processed-output-dir", default="data/processed")
    full_run.add_argument("--dataset-name", default="features")
    full_run.add_argument("--graph-endpoint", default=None)
    full_run.add_argument("--graph-api-key", default=None)
    full_run.add_argument("--graph-subgraph-id", default=None)
    full_run.add_argument("--uniswap-pool-5-bps", default=None)
    full_run.add_argument("--uniswap-pool-30-bps", default=None)
    full_run.add_argument("--coinbase-product-id", default="ETH-USD")
    full_run.add_argument("--coinbase-interval-seconds", default=60, type=int)
    full_run.add_argument("--coinbase-base-url", default=None)
    full_run.add_argument("--rpc-url", default=None)
    full_run.add_argument(
        "--rpc-mode", default="auto", choices=["auto", "blocks", "feehistory"]
    )
    full_run.add_argument(
        "--rpc-feehistory-blocks-per-request",
        default=1024,
        type=int,
    )
    full_run.add_argument("--rpc-progress-every-blocks", default=1000, type=int)
    full_run.add_argument(
        "--raw-format",
        default="parquet",
        choices=["json", "parquet", "both"],
    )
    full_run.add_argument("--realized-vol-window", default=30, type=int)
    full_run.add_argument("--annualization-minutes", default=525600, type=int)
    full_run.add_argument("--gas-weight-bps-per-gwei", default=0.02, type=float)
    full_run.add_argument("--fail-on-warnings", action="store_true")
    full_run.add_argument("--min-uniswap5-coverage", default=0.9, type=float)
    full_run.add_argument("--min-uniswap30-coverage", default=0.05, type=float)
    full_run.add_argument("--staleness-threshold-minutes", default=60, type=int)
    full_run.add_argument("--max-uniswap5-stale-share", default=0.5, type=float)
    full_run.add_argument("--max-uniswap30-stale-share", default=0.95, type=float)
    full_run.add_argument("--fail-on-quality-warnings", action="store_true")

    return parser


def run_placeholder(args: argparse.Namespace) -> int:
    """Placeholder command that only validates runtime config."""
    config = load_config(
        start_time_utc=args.start_time_utc,
        end_time_utc=args.end_time_utc,
        output_dir=args.output_dir,
        log_level=args.log_level,
    )
    logger = get_logger(level=config.log_level)
    logger.info(
        "placeholder command loaded config from %s to %s",
        config.start_time_utc,
        config.end_time_utc,
    )
    return 0


def run_uniswap_preview(args: argparse.Namespace) -> int:
    """Preview fetch for Uniswap Graph data without downstream transforms."""
    logger = get_logger()
    endpoint = resolve_graph_endpoint(
        endpoint=args.graph_endpoint,
        api_key=args.graph_api_key,
        subgraph_id=args.graph_subgraph_id,
    )
    client = UrllibGraphClient(endpoint=endpoint, api_key=args.graph_api_key)

    start_time_utc = parse_utc_datetime(args.start_time_utc)
    end_time_utc = parse_utc_datetime(args.end_time_utc)

    observations = fetch_pool_minutes(
        client,
        pool_id=args.pool_id,
        fee_tier_bps=args.fee_tier_bps,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        page_size=args.page_size,
    )
    records = observations_to_records(observations)
    logger.info(
        "uniswap preview fetched %s rows (%s)",
        len(records),
        MAINNET_NETWORK_LABEL,
    )
    return 0


def run_coinbase_preview(args: argparse.Namespace) -> int:
    """Preview fetch for Coinbase data without downstream transforms."""
    logger = get_logger()
    client = UrllibCoinbaseClient(
        base_url=args.base_url or UrllibCoinbaseClient.base_url
    )

    start_time_utc = parse_utc_datetime(args.start_time_utc)
    end_time_utc = parse_utc_datetime(args.end_time_utc)

    observations = fetch_coinbase_candles(
        client,
        product_id=args.product_id,
        interval_seconds=args.interval_seconds,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
    )
    records = coinbase_observations_to_records(observations)
    logger.info("coinbase preview fetched %s rows", len(records))
    return 0


def run_gas_preview(args: argparse.Namespace) -> int:
    """Preview fetch for Ethereum gas/basefee without downstream transforms."""
    logger = get_logger()
    client = UrllibEthereumRPCClient(rpc_url=args.rpc_url)
    start_time_utc = parse_utc_datetime(args.start_time_utc)
    end_time_utc = parse_utc_datetime(args.end_time_utc)

    block_rows = fetch_basefee_observations(
        client,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        rpc_mode=args.rpc_mode,
        feehistory_blocks_per_request=args.rpc_feehistory_blocks_per_request,
        progress_every_blocks=args.rpc_progress_every_blocks,
    )
    minute_rows = aggregate_basefee_to_minutes(block_rows)
    logger.info(
        "gas preview fetched %s block rows and %s minute rows",
        len(block_rows),
        len(minute_rows),
    )
    return 0


def run_features_preview(args: argparse.Namespace) -> int:
    """Compute feature set from aligned records in a JSON file."""
    logger = get_logger()
    input_path = Path(args.input_json)
    records = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("input JSON must be a list of records")

    features = compute_features(
        records,
        realized_vol_window=args.realized_vol_window,
        annualization_minutes=args.annualization_minutes,
        gas_weight_bps_per_gwei=args.gas_weight_bps_per_gwei,
    )

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(features, indent=2), encoding="utf-8")
        logger.info(
            "features preview computed %s rows and wrote %s",
            len(features),
            output_path,
        )
    else:
        logger.info("features preview computed %s rows", len(features))
    return 0


def run_raw_ingest(args: argparse.Namespace) -> int:
    """Run raw ingestion pipeline and persist source outputs."""
    logger = get_logger()
    _load_dotenv_fallback()
    start_time_utc = parse_utc_datetime(args.start_time_utc)
    end_time_utc = parse_utc_datetime(args.end_time_utc)

    graph_api_key = (
        args.graph_api_key
        or os.getenv("UNISWAP_GRAPH_API_KEY")
        or os.getenv("GRAPH_API_KEY")
    )
    graph_endpoint = args.graph_endpoint or os.getenv("UNISWAP_GRAPH_ENDPOINT")
    graph_subgraph_id = args.graph_subgraph_id or os.getenv(
        "UNISWAP_V3_MAINNET_SUBGRAPH_ID"
    )
    pool_5_bps = args.uniswap_pool_5_bps or os.getenv("UNISWAP_POOL_ID_5_BPS")
    pool_30_bps = args.uniswap_pool_30_bps or os.getenv("UNISWAP_POOL_ID_30_BPS")
    rpc_url = args.rpc_url or os.getenv("ETHEREUM_RPC_URL")
    if not rpc_url:
        alchemy_key = os.getenv("ALCHEMY_API_KEY")
        if alchemy_key:
            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"

    result = run_raw_ingestion(
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        output_dir=args.output_dir,
        graph_endpoint=graph_endpoint,
        graph_api_key=graph_api_key,
        graph_subgraph_id=graph_subgraph_id,
        uniswap_pool_5_bps=pool_5_bps,
        uniswap_pool_30_bps=pool_30_bps,
        coinbase_product_id=args.coinbase_product_id,
        coinbase_interval_seconds=args.coinbase_interval_seconds,
        coinbase_base_url=args.coinbase_base_url,
        rpc_url=rpc_url,
        rpc_mode=args.rpc_mode,
        rpc_feehistory_blocks_per_request=args.rpc_feehistory_blocks_per_request,
        rpc_progress_every_blocks=args.rpc_progress_every_blocks,
        raw_format=args.raw_format,
    )
    logger.info(
        "raw ingestion completed run_id=%s row_counts=%s",
        result.run_id,
        result.row_counts,
    )
    return 0


def run_export_preview(args: argparse.Namespace) -> int:
    """Export JSON records to Parquet + metadata files."""
    logger = get_logger()
    input_path = Path(args.input_json)
    records = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("input JSON must be a list of records")

    if args.start_time_utc:
        start_time_utc = parse_utc_datetime(args.start_time_utc)
    else:
        start_time_utc = datetime.now(UTC)

    if args.end_time_utc:
        end_time_utc = parse_utc_datetime(args.end_time_utc)
    else:
        end_time_utc = start_time_utc

    result = export_records(
        records,
        output_dir=args.output_dir,
        dataset_name=args.dataset_name,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        config={
            "source_file": str(input_path),
            "command": "export-preview",
        },
    )
    logger.info(
        "export preview wrote parquet=%s metadata=%s",
        result.parquet_path,
        result.metadata_path,
    )
    return 0


def run_validate_preview(args: argparse.Namespace) -> int:
    """Validate records and emit missingness report JSON."""
    logger = get_logger()
    records = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("input JSON must be a list of records")

    try:
        issues = enforce_validation(
            records,
            timestamp_key="minute_utc",
            required_columns={"minute_utc"},
            numeric_ranges={
                "coinbase_close": (0.0, None),
            },
            warning_numeric_ranges={
                "basis_5_bps": (-10_000.0, 10_000.0),
                "basis_30_bps": (-10_000.0, 10_000.0),
                "realized_vol_annualized": (0.0, None),
            },
            warning_missing_thresholds={
                "coinbase_close": 0.2,
                "basis_5_bps": 0.2,
                "basis_30_bps": 0.2,
            },
            fail_on_warnings=args.fail_on_warnings,
        )
    except ValidationError as exc:
        logger.error("validation failed: %s", exc)
        return 1

    report = build_missingness_report(
        records,
        expected_columns={
            "minute_utc",
            "coinbase_close",
            "basis_5_bps",
            "basis_30_bps",
        },
    )
    report["validation_issues"] = [
        {"severity": issue.severity, "code": issue.code, "message": issue.message}
        for issue in issues
    ]
    write_missingness_report(args.report_json, report)
    logger.info("validation passed; report written to %s", args.report_json)
    return 0


def run_process_run(args: argparse.Namespace) -> int:
    """Run the processed pipeline from aligned records JSON input."""
    logger = get_logger()
    records = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("input JSON must be a list of records")

    try:
        result = run_processed_pipeline(
            records,
            output_dir=args.output_dir,
            dataset_name=args.dataset_name,
            realized_vol_window=args.realized_vol_window,
            annualization_minutes=args.annualization_minutes,
            gas_weight_bps_per_gwei=args.gas_weight_bps_per_gwei,
            fail_on_warnings=args.fail_on_warnings,
        )
    except ValidationError as exc:
        logger.error("process-run failed validation: %s", exc)
        return 1

    logger.info(
        "process-run completed features=%s report=%s parquet=%s metadata=%s issues=%s",
        result.feature_json_path,
        result.report_json_path,
        result.parquet_path,
        result.metadata_path,
        result.validation_issue_count,
    )
    return 0


def run_full_run(args: argparse.Namespace) -> int:
    """Run end-to-end pipeline with quality gates and summary output."""
    logger = get_logger()
    _load_dotenv_fallback()
    start_time_utc = parse_utc_datetime(args.start_time_utc)
    end_time_utc = parse_utc_datetime(args.end_time_utc)

    graph_api_key = (
        args.graph_api_key
        or os.getenv("UNISWAP_GRAPH_API_KEY")
        or os.getenv("GRAPH_API_KEY")
    )
    graph_endpoint = args.graph_endpoint or os.getenv("UNISWAP_GRAPH_ENDPOINT")
    graph_subgraph_id = args.graph_subgraph_id or os.getenv(
        "UNISWAP_V3_MAINNET_SUBGRAPH_ID"
    )
    pool_5_bps = args.uniswap_pool_5_bps or os.getenv("UNISWAP_POOL_ID_5_BPS")
    pool_30_bps = args.uniswap_pool_30_bps or os.getenv("UNISWAP_POOL_ID_30_BPS")
    rpc_url = args.rpc_url or os.getenv("ETHEREUM_RPC_URL")
    if not rpc_url:
        alchemy_key = os.getenv("ALCHEMY_API_KEY")
        if alchemy_key:
            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"

    try:
        result = run_full_pipeline(
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            raw_output_dir=args.raw_output_dir,
            interim_output_json=args.interim_output_json,
            processed_output_dir=args.processed_output_dir,
            dataset_name=args.dataset_name,
            graph_endpoint=graph_endpoint,
            graph_api_key=graph_api_key,
            graph_subgraph_id=graph_subgraph_id,
            uniswap_pool_5_bps=pool_5_bps,
            uniswap_pool_30_bps=pool_30_bps,
            coinbase_product_id=args.coinbase_product_id,
            coinbase_interval_seconds=args.coinbase_interval_seconds,
            coinbase_base_url=args.coinbase_base_url,
            rpc_url=rpc_url,
            rpc_mode=args.rpc_mode,
            rpc_feehistory_blocks_per_request=args.rpc_feehistory_blocks_per_request,
            rpc_progress_every_blocks=args.rpc_progress_every_blocks,
            raw_format=args.raw_format,
            realized_vol_window=args.realized_vol_window,
            annualization_minutes=args.annualization_minutes,
            gas_weight_bps_per_gwei=args.gas_weight_bps_per_gwei,
            fail_on_warnings=args.fail_on_warnings,
            min_uniswap5_coverage=args.min_uniswap5_coverage,
            min_uniswap30_coverage=args.min_uniswap30_coverage,
            staleness_threshold_minutes=args.staleness_threshold_minutes,
            max_uniswap5_stale_share=args.max_uniswap5_stale_share,
            max_uniswap30_stale_share=args.max_uniswap30_stale_share,
            fail_on_quality_warnings=args.fail_on_quality_warnings,
        )
    except ValidationError as exc:
        logger.error("full-run failed validation/quality gates: %s", exc)
        return 1

    logger.info(
        (
            "full-run completed run_id=%s quality_issues=%s aligned=%s "
            "features=%s summary=%s"
        ),
        result.raw_result.run_id,
        result.quality_issue_count,
        result.aligned_json_path,
        result.processed_result.feature_json_path,
        result.summary_json_path,
    )
    return 0


def run_align_run(args: argparse.Namespace) -> int:
    """Build aligned minute records from raw ingestion artifacts."""
    logger = get_logger()
    path = build_aligned_from_raw_run(
        raw_run_log_path=args.raw_run_log,
        output_json_path=args.output_json,
        duplicate_policy=args.duplicate_policy,
    )
    logger.info("align-run wrote aligned records to %s", path)
    return 0


def _load_dotenv_fallback() -> None:
    """Load .env into process environment when python-dotenv is unavailable."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
        return
    except ModuleNotFoundError:
        pass

    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "placeholder":
        return run_placeholder(args)
    if args.command == "uniswap-preview":
        return run_uniswap_preview(args)
    if args.command == "coinbase-preview":
        return run_coinbase_preview(args)
    if args.command == "gas-preview":
        return run_gas_preview(args)
    if args.command == "raw-ingest":
        return run_raw_ingest(args)
    if args.command == "features-preview":
        return run_features_preview(args)
    if args.command == "export-preview":
        return run_export_preview(args)
    if args.command == "validate-preview":
        return run_validate_preview(args)
    if args.command == "align-run":
        return run_align_run(args)
    if args.command == "process-run":
        return run_process_run(args)
    if args.command == "full-run":
        return run_full_run(args)

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
