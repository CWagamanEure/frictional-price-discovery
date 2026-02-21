"""Uniswap v3 ingestion via The Graph."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib import error, request

from ingestion.models import UniswapMinuteObservation

GRAPH_GATEWAY_BASE_URL = "https://gateway.thegraph.com/api"
MAINNET_NETWORK_LABEL = "ethereum-mainnet"

POOL_MINUTE_QUERY = """
query PoolMinutePage(
  $pool: String!,
  $start: Int!,
  $end: Int!,
  $first: Int!,
  $skip: Int!
) {
  poolMinuteDatas(
    first: $first
    skip: $skip
    orderBy: periodStartUnix
    orderDirection: asc
    where: {pool: $pool, periodStartUnix_gte: $start, periodStartUnix_lte: $end}
  ) {
    periodStartUnix
    token0Price
    token1Price
    volumeUSD
    tvlUSD
  }
}
"""

POOL_SWAPS_QUERY = """
query PoolSwapsPage(
  $pool: String!,
  $start: Int!,
  $end: Int!,
  $first: Int!,
  $skip: Int!
) {
  swaps(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
    where: {pool: $pool, timestamp_gte: $start, timestamp_lte: $end}
  ) {
    id
    timestamp
    amount0
    amount1
    amountUSD
    sqrtPriceX96
  }
}
"""


class GraphAPIError(RuntimeError):
    """Raised when The Graph responds with GraphQL-level errors."""


class GraphClientProtocol:
    """Protocol-like base for Graph POST clients."""

    def post_json(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        """Submit a GraphQL payload and return decoded JSON."""
        raise NotImplementedError


@dataclass
class UrllibGraphClient(GraphClientProtocol):
    """Minimal GraphQL client using urllib with bounded retries."""

    endpoint: str
    api_key: str | None = None
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 0.5

    def post_json(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        payload_dict = {"query": query, "variables": dict(variables)}
        payload = json.dumps(payload_dict).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "research-project-ingestion/0.1 (+https://local)",
        }
        if self.api_key and f"/{self.api_key}/" not in self.endpoint:
            headers["Authorization"] = f"Bearer {self.api_key}"

        req = request.Request(
            self.endpoint,
            data=payload,
            headers=headers,
            method="POST",
        )

        attempts = max(1, self.max_retries)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                parsed = json.loads(body)
                ensure_graph_response_ok(parsed)
                return parsed
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


def _is_retryable_http_error(exc: error.HTTPError) -> bool:
    return exc.code == 429 or exc.code >= 500


def resolve_graph_endpoint(
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    subgraph_id: str | None = None,
) -> str:
    """Resolve Graph endpoint for Uniswap v3 mainnet ingestion."""
    if endpoint:
        return endpoint

    if api_key and subgraph_id:
        return f"{GRAPH_GATEWAY_BASE_URL}/{api_key}/subgraphs/id/{subgraph_id}"

    raise ValueError(
        "Graph endpoint is required. Provide --graph-endpoint or both "
        "--graph-api-key and --graph-subgraph-id for mainnet."
    )


def ensure_graph_response_ok(payload: Mapping[str, Any]) -> None:
    """Raise a clear error if GraphQL-level errors are present."""
    errors = payload.get("errors")
    if not errors:
        return

    if isinstance(errors, list):
        messages = [str(item.get("message", item)) for item in errors]
    else:
        messages = [str(errors)]

    joined = "; ".join(messages)
    raise GraphAPIError(f"Graph returned errors: {joined}")


def parse_pool_minute_page(
    payload: Mapping[str, Any],
    *,
    pool_id: str,
    fee_tier_bps: int,
) -> list[UniswapMinuteObservation]:
    """Parse one Graph response page into normalized observations."""
    ensure_graph_response_ok(payload)

    try:
        raw_rows = payload["data"]["poolMinuteDatas"]
    except (KeyError, TypeError) as exc:
        raise ValueError("unexpected Graph payload shape") from exc

    observations: list[UniswapMinuteObservation] = []
    for row in raw_rows:
        ts = datetime.fromtimestamp(int(row["periodStartUnix"]), tz=UTC)
        observations.append(
            UniswapMinuteObservation(
                timestamp_utc=ts,
                pool_id=pool_id.lower(),
                fee_tier_bps=fee_tier_bps,
                token0_price=float(row["token0Price"]),
                token1_price=float(row["token1Price"]),
                volume_usd=float(row["volumeUSD"]),
                tvl_usd=float(row["tvlUSD"]),
            )
        )

    return observations


def fetch_pool_minutes(
    client: GraphClientProtocol,
    *,
    pool_id: str,
    fee_tier_bps: int,
    start_time_utc: datetime,
    end_time_utc: datetime,
    page_size: int = 1000,
) -> list[UniswapMinuteObservation]:
    """Fetch all minute observations for one pool over a UTC window."""
    if end_time_utc <= start_time_utc:
        raise ValueError("end_time_utc must be later than start_time_utc")

    start_unix = int(start_time_utc.astimezone(UTC).timestamp())
    end_unix = int(end_time_utc.astimezone(UTC).timestamp())

    all_rows: list[UniswapMinuteObservation] = []
    skip = 0

    while True:
        payload = client.post_json(
            POOL_MINUTE_QUERY,
            {
                "pool": pool_id.lower(),
                "start": start_unix,
                "end": end_unix,
                "first": page_size,
                "skip": skip,
            },
        )
        page_rows = parse_pool_minute_page(
            payload,
            pool_id=pool_id,
            fee_tier_bps=fee_tier_bps,
        )
        if not page_rows:
            break

        all_rows.extend(page_rows)
        if len(page_rows) < page_size:
            break
        skip += page_size

    return sorted(all_rows, key=lambda row: row.timestamp_utc)


def fetch_two_fee_tiers(
    client: GraphClientProtocol,
    *,
    pools_by_fee_tier_bps: Mapping[int, str],
    start_time_utc: datetime,
    end_time_utc: datetime,
    page_size: int = 1000,
) -> list[UniswapMinuteObservation]:
    """Fetch minute observations for multiple configured fee tiers."""
    merged: list[UniswapMinuteObservation] = []
    for fee_tier_bps in sorted(pools_by_fee_tier_bps):
        pool_id = pools_by_fee_tier_bps[fee_tier_bps]
        merged.extend(
            fetch_pool_minutes(
                client,
                pool_id=pool_id,
                fee_tier_bps=fee_tier_bps,
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc,
                page_size=page_size,
            )
        )

    return sorted(
        merged,
        key=lambda row: (row.timestamp_utc, row.fee_tier_bps, row.pool_id),
    )


def fetch_pool_swaps_raw(
    client: GraphClientProtocol,
    *,
    pool_id: str,
    start_time_utc: datetime,
    end_time_utc: datetime,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch raw swaps for one pool over a UTC window."""
    if end_time_utc <= start_time_utc:
        raise ValueError("end_time_utc must be later than start_time_utc")

    start_unix = int(start_time_utc.astimezone(UTC).timestamp())
    end_unix = int(end_time_utc.astimezone(UTC).timestamp())
    skip = 0
    rows: list[dict[str, Any]] = []

    while True:
        payload = client.post_json(
            POOL_SWAPS_QUERY,
            {
                "pool": pool_id.lower(),
                "start": start_unix,
                "end": end_unix,
                "first": page_size,
                "skip": skip,
            },
        )
        ensure_graph_response_ok(payload)
        page_rows = payload.get("data", {}).get("swaps", [])
        if not isinstance(page_rows, list):
            raise ValueError("unexpected swaps payload shape")
        if not page_rows:
            break
        rows.extend(page_rows)
        if len(page_rows) < page_size:
            break
        skip += page_size

    return rows


def observations_to_records(
    observations: list[UniswapMinuteObservation],
) -> list[dict[str, object]]:
    """Return JSON-like records from normalized observations."""
    return [row.to_record() for row in observations]


def observations_to_dataframe(
    observations: list[UniswapMinuteObservation],
) -> Any:
    """Convert observations into a pandas DataFrame when pandas is available."""
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("pandas is required for DataFrame conversion") from exc

    return pd.DataFrame(observations_to_records(observations))
