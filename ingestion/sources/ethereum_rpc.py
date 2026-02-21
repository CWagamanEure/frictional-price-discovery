"""Ethereum RPC ingestion for block basefee and gas fields."""

from __future__ import annotations

import json
import logging
import time
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib import error, request

from ingestion.models import GasBasefeeObservation, MinuteGasObservation
from ingestion.utils_time import floor_to_utc_minute, to_utc


class EthereumRPCError(RuntimeError):
    """Raised when Ethereum RPC responses are invalid."""


class EthereumRPCClientProtocol:
    """Protocol-like base for Ethereum RPC clients."""

    def get_latest_block_number(self) -> int:
        """Return latest block number."""
        raise NotImplementedError

    def get_block_by_number(self, block_number: int) -> Mapping[str, Any] | None:
        """Return block JSON object for a block number or None if missing."""
        raise NotImplementedError

    def get_fee_history(self, block_count: int, newest_block: int) -> Mapping[str, Any]:
        """Return eth_feeHistory payload for a contiguous block window."""
        raise NotImplementedError


@dataclass
class UrllibEthereumRPCClient(EthereumRPCClientProtocol):
    """Ethereum JSON-RPC client with bounded retries."""

    rpc_url: str
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 0.5

    def _rpc_call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        req = request.Request(
            self.rpc_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        attempts = max(1, self.max_retries)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                parsed = json.loads(body)
                if parsed.get("error"):
                    raise EthereumRPCError(f"RPC error: {parsed['error']}")
                if "result" not in parsed:
                    raise EthereumRPCError("RPC response missing result")
                return parsed["result"]
            except error.HTTPError as exc:
                if not _is_retryable_http_error(exc) or attempt >= attempts:
                    raise
                last_error = exc
            except (error.URLError, TimeoutError) as exc:
                if attempt >= attempts:
                    raise
                last_error = exc

            time.sleep(self.retry_backoff_seconds * attempt)

        if last_error is not None:
            raise last_error
        raise RuntimeError("unreachable retry state")

    def get_latest_block_number(self) -> int:
        result = self._rpc_call("eth_blockNumber", [])
        return int(result, 16)

    def get_block_by_number(self, block_number: int) -> Mapping[str, Any] | None:
        result = self._rpc_call("eth_getBlockByNumber", [_to_hex(block_number), False])
        if result is None:
            return None
        if not isinstance(result, Mapping):
            raise EthereumRPCError("unexpected block payload")
        return result

    def get_fee_history(self, block_count: int, newest_block: int) -> Mapping[str, Any]:
        result = self._rpc_call(
            "eth_feeHistory",
            [_to_hex(block_count), _to_hex(newest_block), []],
        )
        if not isinstance(result, Mapping):
            raise EthereumRPCError("unexpected feeHistory payload")
        return result


def _is_retryable_http_error(exc: error.HTTPError) -> bool:
    return exc.code == 429 or exc.code >= 500


def _to_hex(value: int) -> str:
    return hex(value)


def _hex_to_int(value: str | None) -> int:
    if value is None:
        return 0
    return int(value, 16)


def parse_block_basefee(block: Mapping[str, Any]) -> GasBasefeeObservation:
    """Parse block payload into a normalized gas/basefee observation."""
    try:
        block_number = _hex_to_int(str(block["number"]))
        timestamp = _hex_to_int(str(block["timestamp"]))
    except KeyError as exc:
        raise EthereumRPCError("block missing required number/timestamp") from exc

    return GasBasefeeObservation(
        block_number=block_number,
        timestamp_utc=datetime.fromtimestamp(timestamp, tz=UTC),
        base_fee_per_gas_wei=_hex_to_int(block.get("baseFeePerGas")),
        gas_used=_hex_to_int(block.get("gasUsed")),
        gas_limit=_hex_to_int(block.get("gasLimit")),
    )


def _find_first_block_at_or_after(
    client: EthereumRPCClientProtocol,
    *,
    target_timestamp_unix: int,
    latest_block_number: int,
) -> int:
    low = 0
    high = latest_block_number + 1

    while low < high:
        mid = (low + high) // 2
        block = client.get_block_by_number(mid)
        if block is None:
            warnings.warn(
                f"missing block {mid} while searching bounds; probing forward",
                RuntimeWarning,
            )
            probe = mid + 1
            while probe < high:
                block = client.get_block_by_number(probe)
                if block is not None:
                    mid = probe
                    break
                warnings.warn(
                    f"missing block {probe} while searching bounds; probing forward",
                    RuntimeWarning,
                )
                probe += 1
            if block is None:
                high = mid
                continue

        mid_ts = _hex_to_int(block.get("timestamp"))
        if mid_ts < target_timestamp_unix:
            low = mid + 1
        else:
            high = mid

    return low


def fetch_basefee_observations(
    client: EthereumRPCClientProtocol,
    *,
    start_time_utc: datetime,
    end_time_utc: datetime,
    rpc_mode: str = "auto",
    feehistory_blocks_per_request: int = 1024,
    progress_every_blocks: int = 1000,
) -> list[GasBasefeeObservation]:
    """Fetch block-level basefee/gas observations within a UTC window."""
    logger = logging.getLogger("ingestion")
    start_time_utc = to_utc(start_time_utc)
    end_time_utc = to_utc(end_time_utc)
    if end_time_utc <= start_time_utc:
        raise ValueError("end_time_utc must be later than start_time_utc")

    latest_block_number = client.get_latest_block_number()
    latest_block = client.get_block_by_number(latest_block_number)
    if latest_block is None:
        raise EthereumRPCError("latest block missing")

    latest_timestamp = _hex_to_int(latest_block.get("timestamp"))
    start_ts = int(start_time_utc.timestamp())
    end_ts = int(end_time_utc.timestamp())

    if start_ts > latest_timestamp:
        return []

    start_block = _find_first_block_at_or_after(
        client,
        target_timestamp_unix=start_ts,
        latest_block_number=latest_block_number,
    )
    first_after_end_block = _find_first_block_at_or_after(
        client,
        target_timestamp_unix=end_ts + 1,
        latest_block_number=latest_block_number,
    )
    end_block = min(first_after_end_block - 1, latest_block_number)

    if start_block > end_block:
        return []

    if rpc_mode not in {"auto", "blocks", "feehistory"}:
        raise ValueError("rpc_mode must be one of: auto, blocks, feehistory")

    if rpc_mode in {"auto", "feehistory"}:
        try:
            return _fetch_basefee_observations_feehistory(
                client,
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc,
                start_block=start_block,
                end_block=end_block,
                blocks_per_request=feehistory_blocks_per_request,
                progress_every_blocks=progress_every_blocks,
            )
        except (EthereumRPCError, ValueError) as exc:
            if rpc_mode == "feehistory":
                raise
            logger.warning(
                "eth_feeHistory unavailable, falling back to block polling: %s",
                exc,
            )

    return _fetch_basefee_observations_blocks(
        client,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        start_block=start_block,
        end_block=end_block,
        progress_every_blocks=progress_every_blocks,
    )


def _fetch_basefee_observations_blocks(
    client: EthereumRPCClientProtocol,
    *,
    start_time_utc: datetime,
    end_time_utc: datetime,
    start_block: int,
    end_block: int,
    progress_every_blocks: int,
) -> list[GasBasefeeObservation]:
    logger = logging.getLogger("ingestion")
    started = time.monotonic()
    if logger.isEnabledFor(logging.INFO):
        total_blocks = end_block - start_block + 1
        logger.info(
            "ethereum_rpc fetch starting blocks %s..%s (%s total)",
            start_block,
            end_block,
            total_blocks,
        )

    rows: list[GasBasefeeObservation] = []
    for index, block_number in enumerate(range(start_block, end_block + 1), start=1):
        block = client.get_block_by_number(block_number)
        if block is None:
            warnings.warn(f"missing block {block_number}; skipping", RuntimeWarning)
            continue

        row = parse_block_basefee(block)
        if row.timestamp_utc < start_time_utc or row.timestamp_utc > end_time_utc:
            continue
        rows.append(row)

        if (
            logger.isEnabledFor(logging.INFO)
            and progress_every_blocks > 0
            and (index % progress_every_blocks == 0 or index == total_blocks)
        ):
            elapsed = time.monotonic() - started
            pct = (index / total_blocks) * 100.0
            rate = index / elapsed if elapsed > 0 else 0.0
            logger.info(
                (
                    "ethereum_rpc progress %.1f%% (%s/%s blocks, "
                    "%.1f blocks/s, elapsed %.1fs)"
                ),
                pct,
                index,
                total_blocks,
                rate,
                elapsed,
            )

    return sorted(rows, key=lambda row: row.block_number)


def _fetch_basefee_observations_feehistory(
    client: EthereumRPCClientProtocol,
    *,
    start_time_utc: datetime,
    end_time_utc: datetime,
    start_block: int,
    end_block: int,
    blocks_per_request: int,
    progress_every_blocks: int,
) -> list[GasBasefeeObservation]:
    logger = logging.getLogger("ingestion")
    if blocks_per_request < 1 or blocks_per_request > 1024:
        raise ValueError("feehistory_blocks_per_request must be between 1 and 1024")

    total_blocks = end_block - start_block + 1
    started = time.monotonic()
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            (
                "ethereum_rpc feehistory fetch starting blocks %s..%s "
                "(%s total, chunk=%s)"
            ),
            start_block,
            end_block,
            total_blocks,
            blocks_per_request,
        )

    rows: list[GasBasefeeObservation] = []
    processed = 0
    chunk_start = start_block
    while chunk_start <= end_block:
        chunk_end = min(chunk_start + blocks_per_request - 1, end_block)
        chunk_count = chunk_end - chunk_start + 1
        history = client.get_fee_history(chunk_count, chunk_end)
        oldest_block, base_fees_wei, gas_used_ratios = _parse_fee_history(history)
        if oldest_block != chunk_start:
            raise EthereumRPCError(
                "feeHistory oldestBlock mismatch: "
                f"expected {chunk_start}, got {oldest_block}"
            )
        if len(gas_used_ratios) != chunk_count:
            raise EthereumRPCError("feeHistory gasUsedRatio length mismatch")
        if len(base_fees_wei) < chunk_count:
            raise EthereumRPCError("feeHistory baseFeePerGas length mismatch")

        start_block_payload = client.get_block_by_number(chunk_start)
        end_block_payload = (
            start_block_payload
            if chunk_count == 1
            else client.get_block_by_number(chunk_end)
        )
        if start_block_payload is None or end_block_payload is None:
            raise EthereumRPCError("feeHistory timestamp anchors missing block payload")

        start_ts = _hex_to_int(start_block_payload.get("timestamp"))
        end_ts = _hex_to_int(end_block_payload.get("timestamp"))
        gas_limit = _hex_to_int(end_block_payload.get("gasLimit"))
        if gas_limit <= 0:
            gas_limit = _hex_to_int(start_block_payload.get("gasLimit"))
        if gas_limit <= 0:
            gas_limit = 1

        for offset in range(chunk_count):
            block_number = chunk_start + offset
            if chunk_count == 1:
                ts_unix = start_ts
            else:
                fraction = offset / (chunk_count - 1)
                ts_unix = round(start_ts + ((end_ts - start_ts) * fraction))

            timestamp_utc = datetime.fromtimestamp(ts_unix, tz=UTC)
            if timestamp_utc < start_time_utc or timestamp_utc > end_time_utc:
                continue

            ratio = float(gas_used_ratios[offset])
            ratio = min(max(ratio, 0.0), 1.5)
            gas_used = int(ratio * gas_limit)
            rows.append(
                GasBasefeeObservation(
                    block_number=block_number,
                    timestamp_utc=timestamp_utc,
                    base_fee_per_gas_wei=base_fees_wei[offset],
                    gas_used=gas_used,
                    gas_limit=gas_limit,
                )
            )

        processed += chunk_count
        if (
            logger.isEnabledFor(logging.INFO)
            and progress_every_blocks > 0
            and (processed % progress_every_blocks == 0 or processed >= total_blocks)
        ):
            elapsed = time.monotonic() - started
            pct = (processed / total_blocks) * 100.0
            rate = processed / elapsed if elapsed > 0 else 0.0
            logger.info(
                (
                    "ethereum_rpc feehistory progress %.1f%% (%s/%s blocks, "
                    "%.1f blocks/s, elapsed %.1fs)"
                ),
                pct,
                processed,
                total_blocks,
                rate,
                elapsed,
            )

        chunk_start = chunk_end + 1

    return sorted(rows, key=lambda row: row.block_number)


def _parse_fee_history(
    payload: Mapping[str, Any],
) -> tuple[int, list[int], list[float]]:
    try:
        oldest_block = _hex_to_int(str(payload["oldestBlock"]))
    except KeyError as exc:
        raise EthereumRPCError("feeHistory missing oldestBlock") from exc

    raw_base_fees = payload.get("baseFeePerGas")
    raw_gas_used_ratios = payload.get("gasUsedRatio")
    if not isinstance(raw_base_fees, list) or not isinstance(raw_gas_used_ratios, list):
        raise EthereumRPCError("feeHistory missing baseFeePerGas/gasUsedRatio arrays")

    try:
        base_fees = [_hex_to_int(str(item)) for item in raw_base_fees]
        gas_used_ratios = [float(item) for item in raw_gas_used_ratios]
    except (TypeError, ValueError) as exc:
        raise EthereumRPCError("feeHistory payload contains invalid values") from exc

    return oldest_block, base_fees, gas_used_ratios


def aggregate_basefee_to_minutes(
    observations: list[GasBasefeeObservation],
) -> list[MinuteGasObservation]:
    """Aggregate blocks to minute UTC using latest block in each minute."""
    if not observations:
        return []

    buckets: dict[datetime, list[GasBasefeeObservation]] = {}
    for row in observations:
        minute = floor_to_utc_minute(row.timestamp_utc)
        buckets.setdefault(minute, []).append(row)

    minute_rows: list[MinuteGasObservation] = []
    for minute in sorted(buckets):
        minute_blocks = sorted(
            buckets[minute],
            key=lambda row: (row.timestamp_utc, row.block_number),
        )
        chosen = minute_blocks[-1]
        minute_rows.append(
            MinuteGasObservation(
                minute_utc=minute,
                base_fee_per_gas_wei=chosen.base_fee_per_gas_wei,
                gas_used=chosen.gas_used,
                gas_limit=chosen.gas_limit,
                block_number=chosen.block_number,
                block_count=len(minute_blocks),
            )
        )

    return minute_rows


def observations_to_records(
    observations: list[GasBasefeeObservation],
) -> list[dict[str, object]]:
    """Return JSON-like records from block observations."""
    return [row.to_record() for row in observations]


def minute_observations_to_records(
    observations: list[MinuteGasObservation],
) -> list[dict[str, object]]:
    """Return JSON-like records from minute observations."""
    return [row.to_record() for row in observations]
