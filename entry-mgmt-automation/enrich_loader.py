"""
Load and normalise index/enrich CSV (Date, Score): parse dates to UTC, fill gaps, shift by one day.

Used to attach enrich_score to aligned_candles so we use only previous-day score (no lookahead).
Symbol -> file by naming convention: USD_JPY -> enrich/ef_usd_jpy.csv (lowercase, underscores).
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))


def _symbol_to_enrich_path(symbol: str, enrich_dir: str | None = None) -> str:
    """Resolve symbol to enrich CSV path by naming convention: USD_JPY -> enrich/ef_usd_jpy.csv."""
    if enrich_dir is None:
        enrich_dir = os.path.join(_DIR, "enrich")
    # e.g. USD_JPY -> usd_jpy
    base = symbol.replace("_", "_").lower()
    return os.path.join(enrich_dir, f"ef_{base}.csv")


def load_enrich_score_series(
    csv_path: str | None = None,
    symbol: str | None = None,
    config: dict[str, Any] | None = None,
) -> pd.Series:
    """
    Load CSV, parse dates to UTC calendar date, fill gaps (weekends/missing), then build
    a series with 1-day shift: for bar date D, value = score from CSV date D-1 (no lookahead).

    Either pass csv_path directly, or symbol (and optional config for enrich.dir) to resolve path.

    Returns:
        Series with index = date (pd.Timestamp date at UTC midnight), value = score (float).
        Lookup: series.get(bar_date) gives the score to use for that bar date (previous day's score).
    """
    if csv_path is None and symbol is None:
        raise ValueError("Provide csv_path or symbol")
    if csv_path is None and symbol is not None:
        enrich_dir = None
        if config and isinstance(config.get("enrich"), dict):
            enrich_dir = config["enrich"].get("dir")
            if enrich_dir and not os.path.isabs(enrich_dir):
                enrich_dir = os.path.join(_DIR, enrich_dir)
        csv_path = _symbol_to_enrich_path(symbol, enrich_dir)

    if not os.path.isfile(csv_path):
        return pd.Series(dtype=float)

    df = pd.read_csv(csv_path)
    # Expect columns: Date (e.g. "16 Feb 2025"), Score
    if "Date" not in df.columns or "Score" not in df.columns:
        return pd.Series(dtype=float)

    # Parse to calendar date (UTC convention for matching entry dates)
    df["date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=False).dt.date
    df = df[["date", "Score"]].dropna(subset=["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    series = df.set_index("date")["Score"].astype(float)

    # Full date range and forward-fill gaps (weekends, missing)
    if series.empty:
        return pd.Series(dtype=float)
    min_d, max_d = series.index.min(), series.index.max()
    dr = pd.date_range(start=pd.Timestamp(min_d), end=pd.Timestamp(max_d), freq="D", tz="UTC")
    full_range = [d.date() for d in dr]
    series = series.reindex(full_range).ffill()

    # 1-day shift: for bar date D we want score from D-1
    # So new index = old index + 1 day, value = old value => shifted[D] = original[D-1]
    shifted_index = [d + timedelta(days=1) for d in series.index]
    shifted = pd.Series(series.values.tolist(), index=shifted_index)

    return shifted


def get_score_for_date(series: pd.Series, bar_date: date) -> float | None:
    """Return score for bar date (previous day's score), or None if not available."""
    if series is None or series.empty:
        return None
    val = series.get(bar_date)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return float(val)


if __name__ == "__main__":
    # Quick sanity check when run as script
    s = load_enrich_score_series(symbol="USD_JPY")
    print(f"Loaded {len(s)} dates (bar_date -> prev-day score)")
    if not s.empty:
        sample = list(s.items())[:3]
        print("Sample:", sample)
        print("e.g. get_score_for_date(2025-02-17):", get_score_for_date(s, date(2025, 2, 17)))
    else:
        print("No data (missing or empty enrich/ef_usd_jpy.csv?)")
