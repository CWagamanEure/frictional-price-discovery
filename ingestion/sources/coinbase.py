"""Coinbase market data ingestion."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib import error, parse, request

from ingestion.models import CoinbaseCandleObservation

COINBASE_ENDPOINT = "https://api.exchange.coinbase.com"
MAX_CANDLES_PER_REQUEST = 300


class CoinbaseAPIError(RuntimeError):
    """Raised when Coinbase responds with an invalid payload."""


class CoinbaseClientProtocol:
    """Protocol-like base for Coinbase HTTP clients."""

    def get_json(self, path: str, query_params: Mapping[str, str]) -> Any:
        """Submit an HTTP GET and return decoded JSON."""
        raise NotImplementedError


@dataclass
class UrllibCoinbaseClient(CoinbaseClientProtocol):
    """Coinbase REST client with bounded retries."""

    base_url: str = COINBASE_ENDPOINT
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 0.5
    user_agent: str = "research-project-ingestion/0.1 (+https://local)"

    def get_json(self, path: str, query_params: Mapping[str, str]) -> Any:
        query = parse.urlencode(dict(query_params))
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}?{query}"
        req = request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
        )

        attempts = max(1, self.max_retries)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                return json.loads(body)
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


def parse_candle_rows(
    payload: Any,
    *,
    product_id: str,
    interval_seconds: int,
) -> list[CoinbaseCandleObservation]:
    """Parse Coinbase candles payload into normalized observations."""
    if not isinstance(payload, list):
        raise CoinbaseAPIError("unexpected Coinbase payload type")

    observations: list[CoinbaseCandleObservation] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < 6:
            raise CoinbaseAPIError("unexpected Coinbase candle row shape")

        unix_ts, low, high, open_, close, volume = row[:6]
        observations.append(
            CoinbaseCandleObservation(
                timestamp_utc=datetime.fromtimestamp(int(unix_ts), tz=UTC),
                product_id=product_id,
                interval_seconds=interval_seconds,
                open_price=float(open_),
                high_price=float(high),
                low_price=float(low),
                close_price=float(close),
                volume=float(volume),
            )
        )

    return sorted(observations, key=lambda row: row.timestamp_utc)


def fetch_coinbase_candles(
    client: CoinbaseClientProtocol,
    *,
    product_id: str,
    interval_seconds: int,
    start_time_utc: datetime,
    end_time_utc: datetime,
) -> list[CoinbaseCandleObservation]:
    """Fetch and normalize Coinbase candles over a UTC window."""
    if end_time_utc <= start_time_utc:
        raise ValueError("end_time_utc must be later than start_time_utc")

    start = start_time_utc.astimezone(UTC)
    end = end_time_utc.astimezone(UTC)
    chunk_span = timedelta(seconds=interval_seconds * (MAX_CANDLES_PER_REQUEST - 1))

    merged: dict[datetime, CoinbaseCandleObservation] = {}
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + chunk_span, end)
        payload = client.get_json(
            path=f"products/{product_id}/candles",
            query_params={
                "start": cursor.isoformat().replace("+00:00", "Z"),
                "end": chunk_end.isoformat().replace("+00:00", "Z"),
                "granularity": str(interval_seconds),
            },
        )
        rows = parse_candle_rows(
            payload,
            product_id=product_id,
            interval_seconds=interval_seconds,
        )
        for row in rows:
            merged[row.timestamp_utc] = row

        cursor = chunk_end + timedelta(seconds=interval_seconds)

    return sorted(merged.values(), key=lambda row: row.timestamp_utc)


def observations_to_records(
    observations: list[CoinbaseCandleObservation],
) -> list[dict[str, object]]:
    """Return JSON-like records from normalized observations."""
    return [row.to_record() for row in observations]
