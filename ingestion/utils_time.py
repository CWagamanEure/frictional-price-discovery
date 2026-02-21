"""Time utility helpers."""

from datetime import UTC, datetime


def floor_to_utc_minute(ts: datetime) -> datetime:
    """Floor a timestamp to its UTC minute boundary."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    utc_ts = ts.astimezone(UTC)
    return utc_ts.replace(second=0, microsecond=0)


def to_utc(ts: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)
