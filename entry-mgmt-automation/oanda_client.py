"""
Oanda API client for fetching candlestick data (Phase A1).

Uses .env in this directory: OANDA_ACCESS_TOKEN, OANDA_ACCOUNT_ID, OANDA_ENVIRONMENT (practice | live).
Callers (e.g. A2) pass from/to and granularity from config; this module does not read config.yaml.
"""

from __future__ import annotations

import os
import re
import time as _time
from datetime import datetime, timezone, timedelta
from typing import Callable, Union

import pandas as pd
import requests
from dotenv import load_dotenv

# Load .env from the same directory as this module (entry-mgmt-automation)
_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"))

# Oanda CandlestickGranularity values; scanner typically uses M15, H1, D, W
VALID_GRANULARITIES = frozenset({
    "S5", "S10", "S15", "S30",
    "M1", "M2", "M4", "M5", "M10", "M15", "M30",
    "H1", "H2", "H3", "H4", "H6", "H8", "H12",
    "D", "W", "M",
})

# Oanda rejects when from-to implies more than this many candles; chunk requests.
MAX_CANDLES_PER_REQUEST = 5000

# One period for pagination: next request starts after last candle time
_GRANULARITY_DELTA = {
    "S5": timedelta(seconds=5),
    "S10": timedelta(seconds=10),
    "S15": timedelta(seconds=15),
    "S30": timedelta(seconds=30),
    "M1": timedelta(minutes=1),
    "M2": timedelta(minutes=2),
    "M4": timedelta(minutes=4),
    "M5": timedelta(minutes=5),
    "M10": timedelta(minutes=10),
    "M15": timedelta(minutes=15),
    "M30": timedelta(minutes=30),
    "H1": timedelta(hours=1),
    "H2": timedelta(hours=2),
    "H3": timedelta(hours=3),
    "H4": timedelta(hours=4),
    "H6": timedelta(hours=6),
    "H8": timedelta(hours=8),
    "H12": timedelta(hours=12),
    "D": timedelta(days=1),
    "W": timedelta(weeks=1),
    "M": timedelta(days=31),  # approximate
}

_BASE_URLS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}

# Empty DataFrame schema for empty range
_EMPTY_DF = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_rfc3339(t: Union[datetime, str]) -> str:
    """Normalize to RFC3339 string for Oanda API (UTC)."""
    if isinstance(t, str):
        dt = _parse_iso_time(t)
    else:
        dt = t
    dt = _ensure_utc(dt)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def _parse_iso_time(s: str) -> datetime:
    """Parse Oanda RFC3339 time (may include nanoseconds); return UTC datetime."""
    s = s.replace("Z", "+00:00")
    # Python fromisoformat() accepts at most 6 fractional digits; Oanda may use 8 or 9.
    s = re.sub(r"\.(\d{6})\d*", r".\1", s)
    return datetime.fromisoformat(s)


def _parse_candle(c: dict) -> dict:
    """Extract time and mid OHLC from one Oanda candle; raise if mid missing."""
    mid = c.get("mid")
    if not mid:
        raise ValueError("Candle has no 'mid' (request with price=M)")
    return {
        "time": _parse_iso_time(c["time"]),
        "open": float(mid["o"]),
        "high": float(mid["h"]),
        "low": float(mid["l"]),
        "close": float(mid["c"]),
        "volume": c.get("volume", 0),
    }


class OandaClientError(Exception):
    """Raised on API or client errors."""
    pass


class OandaClient:
    """
    Oanda v20 client for fetching candles. Auth from .env in entry-mgmt-automation.
    """

    def __init__(self) -> None:
        token = os.getenv("OANDA_ACCESS_TOKEN")
        account_id = os.getenv("OANDA_ACCOUNT_ID")
        env_name = (os.getenv("OANDA_ENVIRONMENT") or "").strip().lower()

        if not token:
            raise OandaClientError("OANDA_ACCESS_TOKEN is not set in .env")
        if not account_id:
            raise OandaClientError("OANDA_ACCOUNT_ID is not set in .env")
        if env_name not in _BASE_URLS:
            raise OandaClientError(
                f"OANDA_ENVIRONMENT must be one of {list(_BASE_URLS)} (got {env_name!r})"
            )

        self._base_url = _BASE_URLS[env_name].rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._account_id = account_id

    def fetch_candles(
        self,
        instrument: str,
        granularity: str,
        from_time: Union[datetime, str],
        to_time: Union[datetime, str],
        *,
        pagination_delay_seconds: float = 0.1,
        on_chunk: Callable[[str, str, int, int], None] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch OHLC candles for the given instrument and range.

        Returns a DataFrame with columns: time (UTC), open, high, low, close, volume.
        Sorted by time. Empty range returns empty DataFrame with same columns.
        Times are inclusive [from_time, to_time] per Oanda semantics.

        Args:
            instrument: e.g. "EUR_USD"
            granularity: one of VALID_GRANULARITIES (e.g. M15, H1, D, W)
            from_time: start (datetime or ISO 8601 string, UTC)
            to_time: end (datetime or ISO 8601 string, UTC)
            pagination_delay_seconds: delay between paginated requests (default 0.1)
            on_chunk: if set, called after each HTTP page with
                (instrument, granularity, page_index_1based, cumulative_row_count).
        """
        if granularity not in VALID_GRANULARITIES:
            raise OandaClientError(
                f"Invalid granularity {granularity!r}; must be one of {sorted(VALID_GRANULARITIES)}"
            )

        from_ts = _ensure_utc(
            _parse_iso_time(from_time) if isinstance(from_time, str) else from_time
        )
        to_ts = _ensure_utc(
            _parse_iso_time(to_time) if isinstance(to_time, str) else to_time
        )
        from_str = _to_rfc3339(from_ts)
        to_str = _to_rfc3339(to_ts)
        delta = _GRANULARITY_DELTA.get(granularity)
        if not delta:
            raise OandaClientError(
                f"Unknown granularity {granularity!r}; cannot chunk requests"
            )

        all_rows: list[dict] = []
        current_from = from_ts
        page_idx = 0

        while current_from < to_ts:
            page_idx += 1
            # Request at most MAX_CANDLES_PER_REQUEST bars so Oanda does not reject
            chunk_end = min(
                current_from + MAX_CANDLES_PER_REQUEST * delta,
                to_ts,
            )
            chunk_from_str = _to_rfc3339(current_from)
            chunk_to_str = _to_rfc3339(chunk_end)

            url = (
                f"{self._base_url}/v3/instruments/{instrument}/candles"
                f"?from={requests.utils.quote(chunk_from_str)}"
                f"&to={requests.utils.quote(chunk_to_str)}"
                f"&granularity={granularity}"
                "&price=M"
            )
            max_retries = 5
            backoff = 1.0
            r = None
            for attempt in range(max_retries):
                r = requests.get(url, headers=self._headers, timeout=60)
                if r.status_code == 429 and attempt < max_retries - 1:
                    retry_after = r.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                    _time.sleep(wait)
                    backoff = min(backoff * 2, 60.0)
                    continue
                break
            if r is None or not r.ok:
                raise OandaClientError(
                    f"Oanda API error {r.status_code}: {r.text[:500]}"
                )
            data = r.json()
            candles = data.get("candles") or []

            for c in candles:
                if "mid" in c:
                    all_rows.append(_parse_candle(c))

            if on_chunk is not None:
                on_chunk(instrument, granularity, page_idx, len(all_rows))

            if not candles:
                break
            last_time = candles[-1]["time"]
            last_dt = _parse_iso_time(last_time)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            current_from = last_dt + delta
            if current_from >= to_ts:
                break
            _time.sleep(pagination_delay_seconds)

        if not all_rows:
            return _EMPTY_DF.copy()

        df = pd.DataFrame(all_rows)
        df = df.sort_values("time").reset_index(drop=True)
        return df


if __name__ == "__main__":
    # Use config.yaml: scan range, timeframes, symbols, and MA buffer. Requires .env with valid token.
    import yaml
    from mtf_loader import resolve_scan_windows

    config_path = os.path.join(_DIR, "config.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    timeframes = config["timeframes"]
    symbols = config.get("symbols") or ["EUR_USD"]
    windows = resolve_scan_windows(config)
    # Fetch range = [from - buffer, to] so largest MA is warmed up at first scan bar (PLAN Phase A).
    entry_detection = config.get("entry_detection") or {}
    slow = entry_detection.get("slowEMAPeriod") or entry_detection.get("ema20Period") or 20
    medium = entry_detection.get("mediumEMAPeriod") or entry_detection.get("ema50Period") or 50
    fast = entry_detection.get("fastEMAPeriod") or entry_detection.get("ema100Period") or 100
    buffer_bars = max(slow, medium, fast)
    entry_tf = timeframes["entry"]
    bar_delta = _GRANULARITY_DELTA.get(entry_tf)
    if bar_delta:
        fetch_from = from_ts - buffer_bars * bar_delta
    else:
        fetch_from = from_ts
    client = OandaClient()
    print(f"Config: windows={len(windows)}, entry TF={entry_tf}, symbols={symbols}")
    for w_idx, (from_ts, to_ts) in enumerate(windows):
        if bar_delta:
            fetch_from = from_ts - buffer_bars * bar_delta
        else:
            fetch_from = from_ts
        print(
            f"  Window {w_idx+1}: {from_ts.isoformat()} → {to_ts.isoformat()} "
            f"(fetch from {fetch_from.isoformat()})"
        )
        for instrument in symbols:
            df = client.fetch_candles(instrument, entry_tf, fetch_from, to_ts)
            print(f"    {instrument} {entry_tf}: {len(df)} candles (columns: {list(df.columns)})")
            if len(df) > 0:
                print(df.head(2).to_string())
                print("    ...")
                print(df.tail(2).to_string())
