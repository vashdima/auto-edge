"""
Chart entry logic for B1.2 (breakout, pause, distance, EMAs up, chart_setup_detected).

B1.2a: Breakout state — breakout_above_ema, bars_since_breakout, within_breakout_window.
B1.2b: Pause pattern — pause_pattern = (high < high_prev). Logic matches entry_mgmt.pine.
B1.2c: Distance filter — distance_from_ema, distance_in_atr, price_not_too_far_from_ema (uses atr from DB).
B1.2d: EMAs up — ema_slow_sloping_up, ema_medium_sloping_up, ema_fast_sloping_up, all_emas_up (uses ema_* from DB).
B1.2e: Combine — chart_setup_detected (chart-only: breakout window + pause + EMAs up + distance; no context/validation).
B1.3: Context (HTF) — context_bullish = ctx_ema_slow > ctx_ema_fast and ctx_close > ctx_ema_slow (uses ctx_* from DB).
B1.4: Validation — val_ema_slow_prev, val_is_locked (stateful), val_ema_sloping_up, val_price_above_ema, validation_ok (uses val_* from DB).
B1.5: Combine — entry_setup_detected = chart_setup_detected and context_bullish and validation_ok (Pine: entrySetupDetected).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _max_candles_from_config(config: dict[str, Any] | None) -> int:
    """Extract maxCandlesAfterBreak from config; default 5 (Pine default)."""
    if config is None:
        return 5
    ed = config.get("entry_detection") or {}
    return int(ed.get("maxCandlesAfterBreak", 5))


def _max_distance_from_config(config: dict[str, Any] | None) -> float:
    """Extract maxDistanceFromEMA from config; default 2.0 (Pine default)."""
    if config is None:
        return 2.0
    ed = config.get("entry_detection") or {}
    return float(ed.get("maxDistanceFromEMA", 2.0))


def _context_re_enabled(config: dict[str, Any] | None) -> bool:
    if config is None:
        return False
    ed = config.get("entry_detection") or {}
    return bool(ed.get("contextReEnabled", False))


def _context_re_multiplier(config: dict[str, Any] | None) -> float:
    if config is None:
        return 1.4
    ed = config.get("entry_detection") or {}
    return float(ed.get("contextReAtrMultiplier", 1.4))


def add_breakout_state(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    *,
    max_candles_after_break: int | None = None,
) -> pd.DataFrame:
    """
    Add breakout state columns: breakout_above_ema, bars_since_breakout, within_breakout_window, within_pause_window.

    Breakout = first bar where close crosses above ema_fast (20). Reset when close < ema_fast.
    bars_since_breakout counts from breakout bar (0, 1, 2, ...);
    within_breakout_window is True when bars_since_breakout <= maxCandlesAfterBreak.
    within_pause_window is True only when 1 <= bars_since_breakout <= max (setup allowed from next bar after breakout).
    Rows must be sorted by time. Requires columns: close, ema_fast.
    """
    out = df.copy()
    max_candles = (
        max_candles_after_break
        if max_candles_after_break is not None
        else _max_candles_from_config(config)
    )

    prev_close = out["close"].shift(1)
    prev_ema = out["ema_fast"].shift(1)
    breakout_above_ema = (prev_close <= prev_ema) & (out["close"] > out["ema_fast"])
    # First row: prev_close/prev_ema are NaN -> (NaN <= x) is False, so False
    breakout_above_ema = breakout_above_ema.fillna(False).astype(bool)

    n = len(out)
    bars_since = np.full(n, np.nan, dtype=float)
    within_window = np.zeros(n, dtype=bool)

    breakout_start_idx: int | None = None

    for i in range(n):
        close_i = out["close"].iloc[i]
        ema_i = out["ema_fast"].iloc[i]
        if pd.isna(close_i) or pd.isna(ema_i):
            bars_since[i] = np.nan
            within_window[i] = False
            continue
        if close_i < ema_i:
            breakout_start_idx = None
            bars_since[i] = np.nan
            within_window[i] = False
        elif breakout_above_ema.iloc[i]:
            breakout_start_idx = i
            bars_since[i] = 0
            within_window[i] = 0 <= max_candles
        elif breakout_start_idx is not None:
            b = i - breakout_start_idx
            bars_since[i] = float(b)
            within_window[i] = b <= max_candles
        else:
            bars_since[i] = np.nan
            within_window[i] = False

    out["breakout_above_ema"] = breakout_above_ema
    out["bars_since_breakout"] = bars_since
    out["within_breakout_window"] = within_window
    # Pause window for setup: only bars *after* the breakout (1..max), not the breakout bar (0)
    out["within_pause_window"] = (
        out["within_breakout_window"]
        & out["bars_since_breakout"].notna()
        & (out["bars_since_breakout"] >= 1)
    ).astype(bool)
    return out


def add_pause_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add pause_pattern column: True when current high < previous high (Pine: high < high[1]).

    First row has no previous bar, so pause_pattern is False. Requires column: high.
    """
    out = df.copy()
    high_prev = out["high"].shift(1)
    out["pause_pattern"] = (out["high"] < high_prev).fillna(False).astype(bool)
    return out


def add_distance_filter(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    *,
    max_distance_from_ema: float | None = None,
) -> pd.DataFrame:
    """
    Add distance filter columns: distance_from_ema, distance_in_atr, price_not_too_far_from_ema.

    Pine: distanceFromEMA = close - ema20; distanceInATR = atr != 0 ? (distanceFromEMA/atr) : na;
    priceNotTooFarFromEMA = na(distanceInATR) ? false : distanceInATR <= maxDistanceFromEMA.
    Requires columns: close, ema_fast, atr. Uses config entry_detection.maxDistanceFromEMA (default 2.0).
    """
    out = df.copy()
    max_dist = (
        max_distance_from_ema
        if max_distance_from_ema is not None
        else _max_distance_from_config(config)
    )

    out["distance_from_ema"] = out["close"] - out["ema_fast"]
    # Where atr is 0 or NaN, distance_in_atr is NaN (Pine: na)
    out["distance_in_atr"] = out["distance_from_ema"] / out["atr"].replace(0, np.nan)
    # Where distance_in_atr is NaN, treat as not within range (Pine: false)
    out["price_not_too_far_from_ema"] = (
        out["distance_in_atr"].notna() & (out["distance_in_atr"] <= max_dist)
    ).astype(bool)
    return out


def add_emas_up(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add EMAs-up columns: ema_slow_sloping_up, ema_medium_sloping_up, ema_fast_sloping_up, all_emas_up.

    Pine: ema20SlopingUp = ema20 > ema20[1]; allEmasUp = all three sloping up.
    First row has no previous bar, so sloping_up is False. Requires columns: ema_slow, ema_medium, ema_fast.
    """
    out = df.copy()
    out["ema_slow_sloping_up"] = (out["ema_slow"] > out["ema_slow"].shift(1)).fillna(False).astype(bool)
    out["ema_medium_sloping_up"] = (out["ema_medium"] > out["ema_medium"].shift(1)).fillna(False).astype(bool)
    out["ema_fast_sloping_up"] = (out["ema_fast"] > out["ema_fast"].shift(1)).fillna(False).astype(bool)
    out["all_emas_up"] = (
        out["ema_slow_sloping_up"] & out["ema_medium_sloping_up"] & out["ema_fast_sloping_up"]
    )
    return out


def add_chart_setup_detected(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add chart_setup_detected: True when all chart-only conditions are met (no context/validation yet).

    Uses within_pause_window (not within_breakout_window): setup only on bars *after* the breakout
    (bars_since_breakout >= 1), so the breakout bar itself is not a valid setup bar.
    Requires columns: within_pause_window, pause_pattern, all_emas_up, price_not_too_far_from_ema.
    """
    out = df.copy()
    out["chart_setup_detected"] = (
        out["within_pause_window"]
        & out["pause_pattern"]
        & out["all_emas_up"]
        & out["price_not_too_far_from_ema"]
    ).astype(bool)
    return out


def add_chart_entry_conditions(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Run all B1.2 chart entry steps and add chart_setup_detected.

    Order: breakout state → pause → distance filter → EMAs up → chart_setup_detected.
    Requires columns from DB (after B1.1): close, high, ema_slow, ema_medium, ema_fast, atr.
    """
    out = add_breakout_state(df, config)
    out = add_pause_pattern(out)
    out = add_distance_filter(out, config)
    out = add_emas_up(out)
    out = add_chart_setup_detected(out)
    return out


def add_context_bullish(df: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Add context_bullish: True when HTF trend is bullish (Pine: htfTrendBullish).

    context_bullish = (ctx_ema_fast > ctx_ema_slow) and (ctx_close > ctx_ema_slow).
    Fast above slow = uptrend; price above trend (slow) EMA.
    If entry_detection.contextReEnabled is true, also require a stateful context RE regime:
      - trigger: ctx_re_ratio >= contextReAtrMultiplier
      - reset: ctx_close < ctx_ema_fast
      - ctx_re_state_active persists between trigger and reset.
    Requires columns from DB (B1.1): ctx_ema_slow, ctx_ema_fast, ctx_close, ctx_atr.
    Where any input is NaN, context_bullish is False.
    """
    out = df.copy()
    for col in ("ctx_high", "ctx_low", "ctx_atr"):
        if col not in out.columns:
            out[col] = np.nan
    out["context_bullish_base"] = (
        (out["ctx_ema_fast"] > out["ctx_ema_slow"]) & (out["ctx_close"] > out["ctx_ema_slow"])
    ).fillna(False).astype(bool)
    re_mult = _context_re_multiplier(config)
    # Build finalized context bars (last row per ctx_time), then run RE logic on those bars.
    # Using first chart row per ctx_time can miss true range expansion and cause false 0-trade runs.
    ctx_bars = (
        out.groupby("ctx_time", as_index=False)
        .agg(
            {
                "ctx_high": "last",
                "ctx_low": "last",
                "ctx_close": "last",
                "ctx_atr": "last",
                "ctx_ema_fast": "last",
            }
        )
        .sort_values("ctx_time")
        .reset_index(drop=True)
    )
    ctx_bars["ctx_re_ratio"] = (
        (ctx_bars["ctx_high"] - ctx_bars["ctx_low"]) / ctx_bars["ctx_atr"].replace(0, np.nan)
    )
    ctx_bars["ctx_re_trigger"] = (ctx_bars["ctx_re_ratio"] >= re_mult).fillna(False).astype(bool)
    ctx_bars["ctx_re_reset"] = (ctx_bars["ctx_close"] < ctx_bars["ctx_ema_fast"]).fillna(False).astype(bool)
    state: list[bool] = []
    active = False
    for _, row in ctx_bars.iterrows():
        if bool(row["ctx_re_trigger"]):
            active = True
        if bool(row["ctx_re_reset"]):
            active = False
        state.append(active)
    ctx_bars["ctx_re_state_active"] = state
    out = out.merge(
        ctx_bars[
            ["ctx_time", "ctx_re_ratio", "ctx_re_trigger", "ctx_re_reset", "ctx_re_state_active"]
        ],
        on="ctx_time",
        how="left",
    )
    out["ctx_re_ratio"] = out["ctx_re_ratio"].astype(float)
    out["ctx_re_trigger"] = out["ctx_re_trigger"].fillna(False).astype(bool)
    out["ctx_re_reset"] = out["ctx_re_reset"].fillna(False).astype(bool)
    out["ctx_re_state_active"] = out["ctx_re_state_active"].fillna(False).astype(bool)

    if _context_re_enabled(config):
        out["context_bullish"] = out["context_bullish_base"] & out["ctx_re_state_active"]
    else:
        out["context_bullish"] = out["context_bullish_base"]
    out["context_bullish"] = out["context_bullish"].astype(bool)
    return out


def add_validation_ok(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add validation columns: val_ema_fast_prev, val_is_locked, val_ema_sloping_up, val_price_above_ema, validation_ok.

    Validation uses the fast (20) EMA (Pine: valEMA20). valIsLocked when valHigh < val_ema_fast, unlock when valLow > val_ema_fast;
    val_ema_sloping_up = val_ema_fast > val_ema_fast_prev; val_price_above_ema = any OHLC > val_ema_fast.
    Requires columns from DB: val_time, val_ema_fast, val_open, val_high, val_low, val_close.
    """
    out = df.copy()

    # val_ema_fast_prev: previous validation bar's fast EMA (per val_time, then merge back)
    val_bars = (
        out[["val_time", "val_ema_fast"]]
        .drop_duplicates("val_time")
        .sort_values("val_time")
        .reset_index(drop=True)
    )
    val_bars["val_ema_fast_prev"] = val_bars["val_ema_fast"].shift(1)
    out = out.merge(
        val_bars[["val_time", "val_ema_fast_prev"]],
        on="val_time",
        how="left",
    )

    # val_is_locked: stateful over validation bars (full candle below -> lock, full candle above -> unlock)
    # Use last row per val_time for that bar's final high/low (full candle)
    val_bars_hl = (
        out.groupby("val_time", as_index=False)
        .agg({"val_ema_fast": "first", "val_high": "last", "val_low": "last"})
    )
    val_bars_hl = val_bars_hl.sort_values("val_time").reset_index(drop=True)
    locked = np.zeros(len(val_bars_hl), dtype=bool)
    for i in range(len(val_bars_hl)):
        row = val_bars_hl.iloc[i]
        ema, high, low = row["val_ema_fast"], row["val_high"], row["val_low"]
        if pd.notna(high) and pd.notna(ema) and high < ema:
            locked[i] = True
        if pd.notna(low) and pd.notna(ema) and low > ema:
            locked[i] = False
    val_bars_hl["val_is_locked"] = locked
    out = out.merge(
        val_bars_hl[["val_time", "val_is_locked"]],
        on="val_time",
        how="left",
    )
    out["val_is_locked"] = out["val_is_locked"].fillna(True).astype(bool)  # NaN -> treat as locked (safe)

    # val_ema_sloping_up, val_price_above_ema, validation_ok
    out["val_ema_sloping_up"] = (
        (out["val_ema_fast"] > out["val_ema_fast_prev"]).fillna(False).astype(bool)
    )
    out["val_price_above_ema"] = (
        (out["val_open"] > out["val_ema_fast"])
        | (out["val_close"] > out["val_ema_fast"])
        | (out["val_high"] > out["val_ema_fast"])
        | (out["val_low"] > out["val_ema_fast"])
    ).fillna(False).astype(bool)
    out["validation_ok"] = (
        (~out["val_is_locked"]) & out["val_ema_sloping_up"] & out["val_price_above_ema"]
    ).astype(bool)
    return out


def add_entry_setup_detected(df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
    """
    Add entry_setup_detected: True when chart_setup_detected and context_bullish and validation_ok (Pine: entrySetupDetected).
    If config has both enrichScoreMin and enrichScoreMax under entry_detection, also require enrich_score in [min, max] (inclusive).
    Rows with missing/NaN enrich_score are treated as not in range when the filter is active.

    Requires columns: chart_setup_detected; optionally context_bullish, validation_ok (if present, ANDed); enrich_score (if score range is configured).
    """
    out = df.copy()
    out["entry_setup_detected"] = out["chart_setup_detected"].astype(bool)
    if "context_bullish" in out.columns:
        out["entry_setup_detected"] = out["entry_setup_detected"] & out["context_bullish"]
    if "validation_ok" in out.columns:
        out["entry_setup_detected"] = out["entry_setup_detected"] & out["validation_ok"]
    # Optional enrich score range filter (both min and max must be set)
    ed = (config or {}).get("entry_detection") or {}
    min_score = ed.get("enrichScoreMin")
    max_score = ed.get("enrichScoreMax")
    if (
        min_score is not None
        and max_score is not None
        and "enrich_score" in out.columns
    ):
        score_in_range = (
            out["enrich_score"].notna()
            & (out["enrich_score"] >= min_score)
            & (out["enrich_score"] <= max_score)
        )
        out["entry_setup_detected"] = out["entry_setup_detected"] & score_in_range
    out["entry_setup_detected"] = out["entry_setup_detected"].astype(bool)
    return out


if __name__ == "__main__":
    """Run B1.2 + B1.3: load from DB, add chart + context conditions, print summary."""
    import os

    from mtf_loader import get_db_path, load_aligned_for_scan, load_config

    config = load_config()
    db_path = get_db_path(config)
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}. Run mtf_loader.py first, then scanner_indicators.py (B1.1).")
        raise SystemExit(1)

    data = load_aligned_for_scan()
    chart_tf = config["timeframes"]["entry"]
    print(f"B1.2 + B1.3 + B1.4 + B1.5: chart_tf={chart_tf}, db={db_path}")
    for symbol, df in data.items():
        if df.empty:
            print(f"  {symbol}: no rows, skip")
            continue
        required = ["close", "high", "ema_slow", "ema_medium", "ema_fast", "atr"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"  {symbol}: missing columns {missing}. Run scanner_indicators.py (B1.1) first.")
            continue
        with_conditions = add_chart_entry_conditions(df, config)
        # B1.3: context (requires ctx_* from DB)
        ctx_required = ["ctx_ema_slow", "ctx_ema_fast", "ctx_close"]
        if all(c in with_conditions.columns for c in ctx_required):
            with_conditions = add_context_bullish(with_conditions, config)
            n_ctx = with_conditions["context_bullish"].sum()
        else:
            n_ctx = None
        # B1.4: validation (requires val_* from DB)
        val_required = ["val_time", "val_ema_fast", "val_open", "val_high", "val_low", "val_close"]
        if all(c in with_conditions.columns for c in val_required):
            with_conditions = add_validation_ok(with_conditions)
            n_val = with_conditions["validation_ok"].sum()
        else:
            n_val = None
        # B1.5: combine
        with_conditions = add_entry_setup_detected(with_conditions, config)
        n_entry = with_conditions["entry_setup_detected"].sum()
        n_chart = with_conditions["chart_setup_detected"].sum()
        msg = f"  {symbol}: {len(with_conditions)} rows, chart_setup_detected={n_chart}"
        if n_ctx is not None:
            msg += f", context_bullish={n_ctx}"
        else:
            msg += " (no ctx columns, skip context_bullish)"
        if n_val is not None:
            msg += f", validation_ok={n_val}"
        else:
            msg += " (no val columns, skip validation_ok)"
        msg += f", entry_setup_detected={n_entry}"
        print(msg)
        cols = ["time", "close", "chart_setup_detected"]
        if "context_bullish" in with_conditions.columns:
            cols.append("context_bullish")
        if "validation_ok" in with_conditions.columns:
            cols.append("validation_ok")
        cols.append("entry_setup_detected")
        setup_rows = with_conditions.loc[with_conditions["chart_setup_detected"], cols]
        if len(setup_rows) > 0:
            print("  Sample setup bars (chart_setup_detected=True):")
            print(setup_rows.head(5).to_string())
            if len(setup_rows) > 5:
                print("  ...")
        else:
            print("  (no chart_setup_detected bars)")
        full_setup_rows = with_conditions.loc[with_conditions["entry_setup_detected"], cols]
        n_full = len(full_setup_rows)
        if n_full > 0:
            print(f"  Sample bars (entry_setup_detected=True) [{n_full} total]:")
            print(full_setup_rows.head(5).to_string())
            if n_full > 5:
                print("  ...")
        else:
            print("  (no bars with entry_setup_detected)")
    print("Done.")
