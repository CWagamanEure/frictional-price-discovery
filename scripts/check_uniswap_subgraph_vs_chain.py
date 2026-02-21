#!/usr/bin/env python3
"""Compare Uniswap raw subgraph swap rows vs on-chain Swap logs."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error, request

import pyarrow.parquet as pq

SWAP_TOPIC0 = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)


def load_dotenv_fallback() -> None:
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
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare subgraph swap-row counts against on-chain Swap logs."
    )
    parser.add_argument(
        "--run-log",
        default=None,
        help="Path to raw_ingestion_run_*.json. Defaults to latest in data/raw.",
    )
    parser.add_argument(
        "--fee-tier-bps",
        type=int,
        default=30,
        choices=[5, 30],
        help="Uniswap fee tier to compare (5 or 30).",
    )
    parser.add_argument(
        "--pool-id",
        default=None,
        help="Pool address (0x...). Defaults from env by fee tier.",
    )
    parser.add_argument(
        "--rpc-url",
        default=None,
        help="Ethereum RPC URL. Defaults from ETHEREUM_RPC_URL or ALCHEMY_API_KEY.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=2000,
        help="Block chunk size for eth_getLogs.",
    )
    parser.add_argument(
        "--request-sleep-seconds",
        type=float,
        default=0.2,
        help="Sleep between successful eth_getLogs requests.",
    )
    return parser.parse_args()


def _latest_run_log() -> Path:
    candidates = sorted(Path("data/raw").glob("raw_ingestion_run_*.json"))
    if not candidates:
        raise FileNotFoundError("No raw ingestion run logs found in data/raw.")
    return candidates[-1]


def _resolve_rpc_url(explicit: str | None) -> str:
    if explicit:
        return explicit

    env_url = os.getenv("ETHEREUM_RPC_URL")
    if env_url:
        return env_url

    alchemy_key = os.getenv("ALCHEMY_API_KEY")
    if alchemy_key:
        return f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"

    raise ValueError("Set --rpc-url or ETHEREUM_RPC_URL or ALCHEMY_API_KEY.")


def _resolve_pool_id(explicit: str | None, fee_tier_bps: int) -> str:
    if explicit:
        return explicit.lower()
    env_key = (
        "UNISWAP_POOL_ID_30_BPS"
        if fee_tier_bps == 30
        else "UNISWAP_POOL_ID_5_BPS"
    )
    pool_id = os.getenv(env_key)
    if not pool_id:
        raise ValueError(f"Set --pool-id or {env_key}.")
    normalized = pool_id.strip().strip('"').strip("'").lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", normalized):
        raise ValueError(f"Invalid pool id format: {pool_id}")
    return normalized


def _find_artifact(run_log: dict[str, Any], base_key: str) -> Path:
    files = run_log.get("files", {})
    if not isinstance(files, dict):
        raise ValueError("run log missing files mapping")

    key_candidates = [
        f"{base_key}_parquet",
        f"{base_key}_json",
        base_key,
    ]
    for key in key_candidates:
        path = files.get(key)
        if isinstance(path, str) and path:
            return Path(path)

    raise ValueError(f"No artifact path found for {base_key}.")


def _read_record_count(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.read_table(path).num_rows
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload in {path}")
    return len(payload)


def _read_block_window(path: Path) -> tuple[int, int]:
    if path.suffix == ".parquet":
        rows = pq.read_table(path, columns=["block_number"]).column("block_number")
        blocks = [int(x) for x in rows.to_pylist()]
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"Expected list payload in {path}")
        blocks = [
            int(item["block_number"]) for item in payload if "block_number" in item
        ]

    if not blocks:
        raise ValueError("No block_number values found in ethereum_rpc artifact.")
    return min(blocks), max(blocks)


def _rpc_call(rpc_url: str, method: str, params: list[Any]) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    req = request.Request(
        rpc_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    attempts = 8
    for attempt in range(1, attempts + 1):
        try:
            with request.urlopen(req, timeout=60) as response:
                body = response.read().decode("utf-8")
            parsed = json.loads(body)
            if parsed.get("error"):
                raise RuntimeError(f"RPC error: {parsed['error']}")
            return parsed["result"]
        except error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass

            retryable_http = exc.code == 429 or exc.code >= 500
            if retryable_http and attempt < attempts:
                time.sleep(min(10.0, 0.75 * (2 ** (attempt - 1))))
                continue

            if body:
                raise RuntimeError(f"HTTP {exc.code} for {method}: {body}") from exc
            raise RuntimeError(f"HTTP {exc.code} for {method}") from exc
        except (error.URLError, TimeoutError):
            if attempt < attempts:
                time.sleep(min(10.0, 0.75 * (2 ** (attempt - 1))))
                continue
            raise RuntimeError(f"Transport failure for {method}")
    raise RuntimeError("RPC call retry loop exhausted")


def _count_swap_logs(
    *,
    rpc_url: str,
    pool_id: str,
    from_block: int,
    to_block: int,
    chunk_size: int,
    request_sleep_seconds: float,
) -> int:
    total = 0
    current = from_block
    current_chunk = max(1, chunk_size)
    total_blocks = (to_block - from_block) + 1
    started = time.monotonic()
    calls = 0
    while current <= to_block:
        end_block = min(current + current_chunk - 1, to_block)
        try:
            logs = _rpc_call(
                rpc_url,
                "eth_getLogs",
                [
                    {
                        "fromBlock": hex(current),
                        "toBlock": hex(end_block),
                        "address": pool_id,
                        "topics": [SWAP_TOPIC0],
                    }
                ],
            )
            calls += 1
            total += len(logs)
            completed_blocks = (end_block - from_block) + 1
            pct = (completed_blocks / total_blocks) * 100.0
            elapsed = time.monotonic() - started
            print(
                (
                    "[progress] %.1f%% blocks=%s/%s calls=%s "
                    "window=%s..%s logs_so_far=%s elapsed=%.1fs"
                )
                % (
                    pct,
                    completed_blocks,
                    total_blocks,
                    calls,
                    current,
                    end_block,
                    total,
                    elapsed,
                ),
                flush=True,
            )
            current = end_block + 1
            if request_sleep_seconds > 0:
                time.sleep(request_sleep_seconds)
        except RuntimeError as exc:
            message = str(exc).lower()
            if (
                current_chunk > 1
                and (
                    "429" in message
                    or "too many" in message
                    or "limit" in message
                    or "range" in message
                )
            ):
                print(
                    (
                        "[throttle] reducing chunk size from %s to %s after error: %s"
                    )
                    % (current_chunk, max(1, current_chunk // 2), exc),
                    flush=True,
                )
                current_chunk = max(1, current_chunk // 2)
                time.sleep(1.0)
                continue
            raise
    return total


def main() -> int:
    load_dotenv_fallback()
    args = parse_args()

    try:
        run_log_path = Path(args.run_log) if args.run_log else _latest_run_log()
        run_log = json.loads(run_log_path.read_text(encoding="utf-8"))
        if not isinstance(run_log, dict):
            raise ValueError("run log is not a JSON object")

        rpc_url = _resolve_rpc_url(args.rpc_url)
        pool_id = _resolve_pool_id(args.pool_id, args.fee_tier_bps)

        uni_key = f"uniswap_{args.fee_tier_bps}bps"
        uni_path = _find_artifact(run_log, uni_key)
        eth_path = _find_artifact(run_log, "ethereum_rpc")

        subgraph_count = _read_record_count(uni_path)
        from_block, to_block = _read_block_window(eth_path)

        chunk_size = max(1, int(args.chunk_size))
        onchain_count = _count_swap_logs(
            rpc_url=rpc_url,
            pool_id=pool_id,
            from_block=from_block,
            to_block=to_block,
            chunk_size=chunk_size,
            request_sleep_seconds=max(0.0, args.request_sleep_seconds),
        )

        print(f"run_log: {run_log_path}")
        print(f"fee_tier_bps: {args.fee_tier_bps}")
        print(f"pool_id: {pool_id}")
        print(f"subgraph_artifact: {uni_path}")
        print(f"ethereum_artifact: {eth_path}")
        print(f"block_window: {from_block}..{to_block}")
        print(f"subgraph_swap_rows: {subgraph_count}")
        print(f"onchain_swap_logs: {onchain_count}")

        if onchain_count == 0:
            print("coverage_ratio_subgraph_vs_chain: n/a (on-chain count is zero)")
            return 0

        ratio = subgraph_count / onchain_count
        print(f"coverage_ratio_subgraph_vs_chain: {ratio:.4f}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
