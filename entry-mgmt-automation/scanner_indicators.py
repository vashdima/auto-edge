"""
Chart, context, and validation indicators for B1.1.

Computes ema_slow, ema_medium, ema_fast, atr (chart); ctx_ema_slow, ctx_ema_fast, ctx_atr (context);
val_ema_slow, val_ema_medium, val_ema_fast, val_atr (validation). Config: slowEMAPeriod, mediumEMAPeriod, fastEMAPeriod, atrPeriod.
Column names are fixed so changing period values does not require DB schema change.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _ema(series: pd.Series, period: int) -> pd.Series:
    """EMA with alpha = 2/(period+1), matching Pine ta.ema(source, length)."""
    return series.ewm(span=period, adjust=False).mean()


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """ATR as RMA (Wilder) of True Range, matching Pine ta.atr(length)."""
    prev_close = close.shift(1)
    tr = (high - low).combine(
        (high - prev_close).abs().combine((low - prev_close).abs(), max),
        max,
    )
    return tr.ewm(alpha=1 / period, adjust=False).mean()


# Default periods: slow = long (smooth), fast = short (breakout), matching convention
_DEFAULT_SLOW = 100
_DEFAULT_MEDIUM = 50
_DEFAULT_FAST = 20
_DEFAULT_ATR = 14


def _periods_from_config(config: dict[str, Any]) -> tuple[int, int, int, int]:
    """Extract (ema_slow_period, ema_medium_period, ema_fast_period, atr_period) from config."""
    ed = config.get("entry_detection") or {}
    rm = config.get("risk_management") or {}
    slow = ed.get("slowEMAPeriod") or ed.get("ema20Period") or _DEFAULT_SLOW
    medium = ed.get("mediumEMAPeriod") or ed.get("ema50Period") or _DEFAULT_MEDIUM
    fast = ed.get("fastEMAPeriod") or ed.get("ema100Period") or _DEFAULT_FAST
    atr = rm.get("atrPeriod") or _DEFAULT_ATR
    return (int(slow), int(medium), int(fast), int(atr))


def add_chart_indicators(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    *,
    ema_slow_period: int | None = None,
    ema_medium_period: int | None = None,
    ema_fast_period: int | None = None,
    atr_period: int | None = None,
) -> pd.DataFrame:
    """
    Add chart (entry TF) indicators: ema_slow, ema_medium, ema_fast, atr.

    Rows must be sorted by time. First bars may have NaN until warm-up.
    """
    out = df.copy()
    if config is not None:
        slow, medium, fast, atr_p = _periods_from_config(config)
    else:
        slow = ema_slow_period or _DEFAULT_SLOW
        medium = ema_medium_period or _DEFAULT_MEDIUM
        fast = ema_fast_period or _DEFAULT_FAST
        atr_p = atr_period or _DEFAULT_ATR

    out["ema_slow"] = _ema(out["close"], slow)
    out["ema_medium"] = _ema(out["close"], medium)
    out["ema_fast"] = _ema(out["close"], fast)
    out["atr"] = _atr(out["high"], out["low"], out["close"], atr_p)
    return out


def add_context_indicators(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    *,
    ema_slow_period: int | None = None,
    ema_medium_period: int | None = None,
    ema_fast_period: int | None = None,
    atr_period: int | None = None,
) -> pd.DataFrame:
    """
    Add context (HTF) indicators: ctx_ema_slow, ctx_ema_medium, ctx_ema_fast, ctx_atr.

    Builds context bar series (one close per ctx_time), computes EMAs, maps back to each chart row.
    """
    out = df.copy()
    if config is not None:
        slow, medium, fast, atr_p = _periods_from_config(config)
    else:
        slow = ema_slow_period or _DEFAULT_SLOW
        medium = ema_medium_period or _DEFAULT_MEDIUM
        fast = ema_fast_period or _DEFAULT_FAST
        atr_p = atr_period or _DEFAULT_ATR

    # One row per ctx_time: use finalized OHLC from each context bar.
    ctx_bars = out.groupby("ctx_time", as_index=False).agg(
        {"ctx_high": "last", "ctx_low": "last", "ctx_close": "last"}
    )
    ctx_bars = ctx_bars.sort_values("ctx_time").reset_index(drop=True)
    ctx_bars["ctx_ema_slow"] = _ema(ctx_bars["ctx_close"], slow)
    ctx_bars["ctx_ema_medium"] = _ema(ctx_bars["ctx_close"], medium)
    ctx_bars["ctx_ema_fast"] = _ema(ctx_bars["ctx_close"], fast)
    ctx_bars["ctx_atr"] = _atr(
        ctx_bars["ctx_high"], ctx_bars["ctx_low"], ctx_bars["ctx_close"], atr_p
    )

    out = out.merge(
        ctx_bars[["ctx_time", "ctx_ema_slow", "ctx_ema_medium", "ctx_ema_fast", "ctx_atr"]],
        on="ctx_time",
        how="left",
    )
    return out


def add_validation_indicators(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    *,
    ema_slow_period: int | None = None,
    ema_medium_period: int | None = None,
    ema_fast_period: int | None = None,
    atr_period: int | None = None,
) -> pd.DataFrame:
    """
    Add validation TF indicators: val_ema_slow, val_ema_medium, val_ema_fast, val_atr.

    Validation logic (Pine valEMA20) uses the fast-period EMA; all three EMAs are written for chart display.
    val_atr uses the same ATR period as chart/context (risk_management.atrPeriod).
    Builds validation bar series (finalized OHLC per val_time), computes EMAs and ATR, maps back to each chart row.
    """
    out = df.copy()
    if config is not None:
        slow, medium, fast, atr_p = _periods_from_config(config)
    else:
        slow = ema_slow_period or _DEFAULT_SLOW
        medium = ema_medium_period or _DEFAULT_MEDIUM
        fast = ema_fast_period or _DEFAULT_FAST
        atr_p = atr_period or _DEFAULT_ATR

    val_bars = out.groupby("val_time", as_index=False).agg(
        {"val_high": "last", "val_low": "last", "val_close": "last"}
    )
    val_bars = val_bars.sort_values("val_time").reset_index(drop=True)
    val_bars["val_ema_slow"] = _ema(val_bars["val_close"], slow)
    val_bars["val_ema_medium"] = _ema(val_bars["val_close"], medium)
    val_bars["val_ema_fast"] = _ema(val_bars["val_close"], fast)
    val_bars["val_atr"] = _atr(
        val_bars["val_high"], val_bars["val_low"], val_bars["val_close"], atr_p
    )

    out = out.merge(
        val_bars[
            [
                "val_time",
                "val_ema_slow",
                "val_ema_medium",
                "val_ema_fast",
                "val_atr",
            ]
        ],
        on="val_time",
        how="left",
    )
    return out


# Indicator column names for DB and consumers
CHART_INDICATOR_COLUMNS = ["ema_slow", "ema_medium", "ema_fast", "atr"]
CONTEXT_INDICATOR_COLUMNS = ["ctx_ema_slow", "ctx_ema_medium", "ctx_ema_fast", "ctx_atr"]
VALIDATION_INDICATOR_COLUMNS = [
    "val_ema_slow",
    "val_ema_medium",
    "val_ema_fast",
    "val_atr",
]
ALL_INDICATOR_COLUMNS = (
    CHART_INDICATOR_COLUMNS + CONTEXT_INDICATOR_COLUMNS + VALIDATION_INDICATOR_COLUMNS
)


def add_all_indicators(df: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Add chart, context, and validation indicators. Returns DataFrame with all indicator columns.
    """
    out = add_chart_indicators(df, config)
    out = add_context_indicators(out, config)
    out = add_validation_indicators(out, config)
    return out


if __name__ == "__main__":
    """Run B1.1: load aligned data from DB, add indicators and enrich_score, write back, print summary."""
    import os
    import sqlite3

    from enrich_loader import get_score_for_date, load_enrich_score_series
    from mtf_loader import (
        ENRICH_SCORE_COLUMN,
        _init_aligned_candles_db,
        get_db_path,
        load_aligned_full_buffer,
        load_config,
        write_enrich_scores_to_db,
        write_indicators_to_db,
    )

    config = load_config()
    db_path = get_db_path(config)
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}. Run mtf_loader.py first to fetch and store aligned data.")
        raise SystemExit(1)

    conn = sqlite3.connect(db_path)
    _init_aligned_candles_db(conn)

    data = load_aligned_full_buffer()
    chart_tf = config["timeframes"]["entry"]
    print(f"B1.1: chart_tf={chart_tf}, db={db_path}")
    for symbol, df in data.items():
        if df.empty:
            print(f"  {symbol}: no rows, skip")
            continue
        # Drop indicator and enrich columns so we recompute
        drop_cols = [c for c in ALL_INDICATOR_COLUMNS if c in df.columns]
        if ENRICH_SCORE_COLUMN in df.columns:
            drop_cols.append(ENRICH_SCORE_COLUMN)
        base = df.drop(columns=drop_cols, errors="ignore")
        with_ind = add_all_indicators(base, config)
        write_indicators_to_db(conn, symbol, chart_tf, with_ind)

        # Attach enrich_score from index CSV (naming convention: USD_JPY -> enrich/ef_usd_jpy.csv)
        score_series = load_enrich_score_series(symbol=symbol, config=config)
        if not score_series.empty:
            def bar_date_utc(t):
                if t is None or pd.isna(t):
                    return None
                ts = pd.Timestamp(t)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                return ts.date()
            with_ind[ENRICH_SCORE_COLUMN] = with_ind["time"].apply(
                lambda t: get_score_for_date(score_series, bar_date_utc(t))
            )
            write_enrich_scores_to_db(conn, symbol, chart_tf, with_ind)

        n = len(with_ind)
        print(f"  {symbol}: {n} rows, indicators written")
        cols = ["time", "close", "ema_slow", "atr", "ctx_ema_slow", "val_ema_slow"]
        if ENRICH_SCORE_COLUMN in with_ind.columns:
            cols.append(ENRICH_SCORE_COLUMN)
        print(with_ind[cols].head(2).to_string())
        print("  ...")
        print(with_ind[cols].tail(2).to_string())
    conn.close()
    print("Done.")
