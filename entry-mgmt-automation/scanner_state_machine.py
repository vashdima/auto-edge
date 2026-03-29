"""
B2.2: In-memory candidate trades from entry_setup_detected bars.
B2.3: Run each candidate to completion (fill + exit rules) on aligned OHLC.

Builds list of candidate trades (setup_time, entry_price, sl, tp, sl_size) using
Pine's "highest high in pause" and risk config. B2.3 simulates fill and SL/BE/TP exit; B2.4 persists.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _to_iso8601_utc(t: pd.Timestamp) -> str:
    """Format timestamp as ISO8601 UTC string."""
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _risk_params_from_config(config: dict[str, Any]) -> tuple[float, float]:
    """Return (atr_multiplier, rr_take_profit) from config.risk_management."""
    rm = config.get("risk_management") or {}
    atr_mult = float(rm.get("atrMultiplier", 1.5))
    rr_tp = float(rm.get("rrTakeProfit", 4.0))
    return (atr_mult, rr_tp)


def _exit_config(config: dict[str, Any]) -> tuple[float, int]:
    """Return (rr_break_even, max_candles_after_pause) from config."""
    rm = config.get("risk_management") or {}
    rr_be = float(rm.get("rrBreakEven", 1.0))
    ed = config.get("entry_detection") or {}
    max_pause = int(ed.get("maxCandlesAfterPause", 20))
    return (rr_be, max_pause)


def run_trade_to_completion(
    candidate: dict[str, Any],
    df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Resolve entry bar (Pine fill rule) then simulate bar-by-bar exit (SL/BE/TP).
    Returns a completed trade dict (candidate + entry_time, exit_reason, rr) or None if no fill or no exit.
    """
    required = ["time", "high", "low", "close", "ema_fast"]
    if any(c not in df.columns for c in required):
        return None
    setup_idx = candidate["setup_bar_index"]
    entry_price = candidate["entry_price"]
    sl = candidate["sl"]
    tp = candidate["tp"]
    rr_be, max_pause = _exit_config(config)
    n = len(df)
    if setup_idx < 0 or setup_idx >= n:
        return None

    # --- Resolve entry bar: first bar after setup where high >= entry_price (cancel if close < ema_fast first)
    end_fill = min(setup_idx + max_pause + 1, n)
    entry_bar_index: int | None = None
    for i in range(setup_idx + 1, end_fill):
        row = df.iloc[i]
        close = row["close"]
        ema_fast = row["ema_fast"]
        high = row["high"]
        if pd.isna(close) or pd.isna(ema_fast):
            continue
        if close < ema_fast:
            return None  # cancel before fill
        if not pd.isna(high) and high >= entry_price:
            entry_bar_index = i
            break
    if entry_bar_index is None:
        return None

    entry_time = _to_iso8601_utc(df.iloc[entry_bar_index]["time"])
    risk_dist = entry_price - sl

    # --- Exit simulation from entry_bar_index + 1
    break_even_triggered = False
    was_in_profit_after_be = False
    for i in range(entry_bar_index + 1, n):
        row = df.iloc[i]
        high = row["high"]
        low = row["low"]
        if pd.isna(high):
            high = float(row["close"]) if not pd.isna(row["close"]) else 0.0
        if pd.isna(low):
            low = float(row["close"]) if not pd.isna(row["close"]) else 0.0

        be_was_just_triggered = False
        if not break_even_triggered:
            be_distance = entry_price - sl
            be_target = entry_price + (be_distance * rr_be)
            if high >= be_target:
                break_even_triggered = True
                be_was_just_triggered = True
                was_in_profit_after_be = False
        if break_even_triggered and not be_was_just_triggered and high > entry_price:
            was_in_profit_after_be = True

        if high >= tp:
            rr = (tp - entry_price) / risk_dist if risk_dist else 0.0
            exit_time = _to_iso8601_utc(df.iloc[i]["time"])
            return {**candidate, "entry_time": entry_time, "exit_time": exit_time, "exit_reason": "TP", "rr": rr}
        if low <= sl and not break_even_triggered:
            exit_time = _to_iso8601_utc(df.iloc[i]["time"])
            return {**candidate, "entry_time": entry_time, "exit_time": exit_time, "exit_reason": "SL", "rr": -1.0}
        if (
            low <= entry_price
            and break_even_triggered
            and was_in_profit_after_be
            and not be_was_just_triggered
        ):
            exit_time = _to_iso8601_utc(df.iloc[i]["time"])
            return {**candidate, "entry_time": entry_time, "exit_time": exit_time, "exit_reason": "BE", "rr": 0.0}

    return None  # ran out of bars without exit


def candidate_trades_from_df(
    df: pd.DataFrame,
    config: dict[str, Any],
    symbol: str,
    chart_tf: str,
    context_tf: str,
    validation_tf: str,
) -> list[dict[str, Any]]:
    """
    Build one candidate per run (first bar where entry_setup_detected is True).

    Matches Pine: entry/SL/TP set once when setup is detected; we wait for fill or cancel.
    Entry price = max(high) in current breakout window (Pine: highestHighInPause), fallback previous high.
    Setup time = time of bar where that max high occurred. SL/TP from first setup bar's ATR only.
    """
    required = [
        "time", "high", "close", "ema_fast", "atr",
        "bars_since_breakout", "within_breakout_window", "entry_setup_detected",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return []
    atr_mult, rr_tp = _risk_params_from_config(config)

    candidates: list[dict[str, Any]] = []
    run_max_high: float | None = None
    run_max_time: pd.Timestamp | None = None
    emitted_this_run = False

    for i in range(len(df)):
        row = df.iloc[i]
        close = row["close"]
        high = row["high"]
        ema_fast = row["ema_fast"]
        bars_since = row["bars_since_breakout"]
        within = row["within_breakout_window"]
        entry_ok = row["entry_setup_detected"]

        if pd.isna(close) or pd.isna(ema_fast):
            run_max_high = None
            run_max_time = None
            emitted_this_run = False
            continue
        if close < ema_fast:
            run_max_high = None
            run_max_time = None
            emitted_this_run = False
            continue

        if not pd.isna(bars_since) and bars_since == 0:
            run_max_high = float(high) if not pd.isna(high) else None
            run_max_time = row["time"]
        elif within and run_max_high is not None and not pd.isna(high) and high > run_max_high:
            run_max_high = float(high)
            run_max_time = row["time"]

        # Pine: emit one candidate per run (first bar where setup detected); levels set once
        if not entry_ok or emitted_this_run:
            continue

        entry_price = run_max_high
        setup_time = run_max_time
        if entry_price is None or setup_time is None:
            if i > 0:
                prev = df.iloc[i - 1]
                entry_price = float(prev["high"]) if not pd.isna(prev["high"]) else None
                setup_time = prev["time"]
            if entry_price is None or setup_time is None:
                continue

        atr_val = row["atr"]
        if pd.isna(atr_val) or atr_val <= 0:
            continue
        risk_distance = atr_val * atr_mult
        sl = entry_price - risk_distance
        tp = entry_price + (risk_distance * rr_tp)
        sl_size = risk_distance

        emitted_this_run = True

        setup_time_str = _to_iso8601_utc(setup_time)
        ctx_bull = bool(row["context_bullish"]) if "context_bullish" in row and pd.notna(row.get("context_bullish")) else False
        val_ok = bool(row["validation_ok"]) if "validation_ok" in row and pd.notna(row.get("validation_ok")) else False

        candidates.append({
            "symbol": symbol,
            "chart_tf": chart_tf,
            "context_tf": context_tf,
            "validation_tf": validation_tf,
            "setup_time": setup_time_str,
            "entry_price": entry_price,
            "sl": sl,
            "tp": tp,
            "sl_size": sl_size,
            "context_bullish": ctx_bull,
            "validation_ok": val_ok,
            "setup_bar_index": i,
        })

    return candidates
