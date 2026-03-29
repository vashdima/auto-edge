"""
C1: Build entry map per trade (JSON for frontend).

For a chosen run (run_id or run_key), queries raw_trades and aligned_candles,
produces a list of entry map dicts (one per trade) in the JSON shape the frontend uses.
No separate entries table; C2 will call this and return the JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import defaultdict
from datetime import timedelta, timezone
from typing import Any

import pandas as pd

from mtf_loader import (
    ENRICH_SCORE_COLUMN,
    GRANULARITY_DELTA,
    get_db_path,
    init_b2_tables,
    load_aligned_bars_before,
    load_aligned_from_db,
    load_config,
)

_DIR = os.path.dirname(os.path.abspath(__file__))


def _bars_before_from_config(config: dict[str, Any]) -> int:
    """Slowest EMA period = bars_before for every timeframe (chart, context, validation)."""
    ed = config.get("entry_detection") or {}
    slow = int(ed.get("slowEMAPeriod", 20))
    medium = int(ed.get("mediumEMAPeriod", 50))
    fast = int(ed.get("fastEMAPeriod", 100))
    return max(slow, medium, fast)


def _chart_bars_to_cover_htf_bars(
    bars_before: int,
    chart_tf: str,
    context_tf: str,
    validation_tf: str,
) -> int:
    """
    Return number of chart bars to load so we have at least bars_before context bars and bars_before validation bars.

    Chart buffer needs bars_before chart bars. Context/validation need bars_before HTF bars; each HTF bar
    spans (htf_delta / chart_delta) chart bars. So we need max(bars_before, bars_before * context_ratio, ...).
    """
    chart_delta = GRANULARITY_DELTA.get(chart_tf) or timedelta(hours=1)
    ctx_delta = GRANULARITY_DELTA.get(context_tf) or timedelta(days=1)
    val_delta = GRANULARITY_DELTA.get(validation_tf) or timedelta(days=1)
    chart_sec = max(1, chart_delta.total_seconds())
    ctx_ratio = max(1.0, ctx_delta.total_seconds() / chart_sec)
    val_ratio = max(1.0, val_delta.total_seconds() / chart_sec)
    bars_for_ctx = int(bars_before * ctx_ratio)
    bars_for_val = int(bars_before * val_ratio)
    return max(bars_before, bars_for_ctx, bars_for_val)


def _resolve_run_id(conn: sqlite3.Connection, run_id: int | None, run_key: str | None) -> int | None:
    """Return run_id from run_key if run_key given; else return run_id. None if not found."""
    if run_id is not None:
        cur = conn.execute("SELECT run_id FROM scan_runs WHERE run_id = ?", (run_id,))
        return run_id if cur.fetchone() else None
    if run_key:
        cur = conn.execute("SELECT run_id FROM scan_runs WHERE run_key = ?", (run_key,))
        row = cur.fetchone()
        return row[0] if row else None
    return None


def _load_raw_trades(conn: sqlite3.Connection, run_id: int) -> list[dict[str, Any]]:
    """Load all raw_trades for run_id ordered by entry_time. Rows as dicts with column names."""
    cur = conn.execute(
        """SELECT id, run_id, symbol, chart_tf, context_tf, validation_tf,
                  setup_time, entry_time, entry_price, sl, tp, sl_size,
                  exit_reason, rr, context_bullish, validation_ok, exit_time
           FROM raw_trades WHERE run_id = ? ORDER BY entry_time""",
        (run_id,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _to_iso_utc(ts: pd.Timestamp) -> str:
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _chart_bar_to_dict(row: pd.Series) -> dict[str, Any]:
    """One chart bar for JSON: time (ISO), open, high, low, close, optional ema_slow, ema_medium, ema_fast, atr."""
    out: dict[str, Any] = {
        "time": _to_iso_utc(row["time"]) if hasattr(row["time"], "strftime") else str(row["time"]),
        "open": float(row["open"]) if pd.notna(row["open"]) else None,
        "high": float(row["high"]) if pd.notna(row["high"]) else None,
        "low": float(row["low"]) if pd.notna(row["low"]) else None,
        "close": float(row["close"]) if pd.notna(row["close"]) else None,
    }
    for key in ("ema_slow", "ema_medium", "ema_fast", "atr"):
        if key in row.index and pd.notna(row.get(key)):
            out[key] = float(row[key])
    return out


def _ctx_bar_to_dict(row: pd.Series) -> dict[str, Any]:
    """One context bar: time (ctx_time), open/high/low/close from ctx_*."""
    out: dict[str, Any] = {
        "time": _to_iso_utc(row["ctx_time"]) if hasattr(row["ctx_time"], "strftime") else str(row["ctx_time"]),
        "open": float(row["ctx_open"]) if pd.notna(row.get("ctx_open")) else None,
        "high": float(row["ctx_high"]) if pd.notna(row.get("ctx_high")) else None,
        "low": float(row["ctx_low"]) if pd.notna(row.get("ctx_low")) else None,
        "close": float(row["ctx_close"]) if pd.notna(row.get("ctx_close")) else None,
    }
    if "ctx_ema_slow" in row.index and pd.notna(row.get("ctx_ema_slow")):
        out["ema_slow"] = float(row["ctx_ema_slow"])
    if "ctx_ema_medium" in row.index and pd.notna(row.get("ctx_ema_medium")):
        out["ema_medium"] = float(row["ctx_ema_medium"])
    if "ctx_ema_fast" in row.index and pd.notna(row.get("ctx_ema_fast")):
        out["ema_fast"] = float(row["ctx_ema_fast"])
    return out


def _val_bar_to_dict(row: pd.Series) -> dict[str, Any]:
    """One validation bar: time (val_time), open/high/low/close from val_*."""
    out: dict[str, Any] = {
        "time": _to_iso_utc(row["val_time"]) if hasattr(row["val_time"], "strftime") else str(row["val_time"]),
        "open": float(row["val_open"]) if pd.notna(row.get("val_open")) else None,
        "high": float(row["val_high"]) if pd.notna(row.get("val_high")) else None,
        "low": float(row["val_low"]) if pd.notna(row.get("val_low")) else None,
        "close": float(row["val_close"]) if pd.notna(row.get("val_close")) else None,
    }
    if "val_ema_slow" in row.index and pd.notna(row.get("val_ema_slow")):
        out["ema_slow"] = float(row["val_ema_slow"])
    if "val_ema_medium" in row.index and pd.notna(row.get("val_ema_medium")):
        out["ema_medium"] = float(row["val_ema_medium"])
    if "val_ema_fast" in row.index and pd.notna(row.get("val_ema_fast")):
        out["ema_fast"] = float(row["val_ema_fast"])
    return out


def build_entry_maps_for_run(
    run_id: int | None = None,
    run_key: str | None = None,
    db_path: str | None = None,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Build list of entry map dicts (one per trade) for the given run.

    Resolves run_id from run_key if run_key is provided. Loads raw_trades and aligned_candles,
    builds chart (and optional context/validation) buffers per trade, returns JSON-serialisable list.
    """
    if run_id is None and not run_key:
        return []
    config = config or load_config()
    path = db_path or get_db_path(config)
    if not os.path.exists(path):
        return []

    bars_before = _bars_before_from_config(config)
    timeframes = config.get("timeframes") or {}
    context_tf = timeframes.get("context", "W")
    validation_tf = timeframes.get("validation", "D")

    with sqlite3.connect(path) as conn:
        init_b2_tables(conn)  # ensure exit_time column exists
        rid = _resolve_run_id(conn, run_id, run_key)
        if rid is None:
            return []
        trades = _load_raw_trades(conn, rid)

    if not trades:
        return []

    # Group trades by (symbol, chart_tf) for batched candle load
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in trades:
        by_key[(t["symbol"], t["chart_tf"])].append(t)

    # Load aligned_candles per (symbol, chart_tf): enough chart bars so we get bars_before context/validation bars too
    cache: dict[tuple[str, str], pd.DataFrame] = {}
    for (symbol, chart_tf), group in by_key.items():
        entry_times = [t["entry_time"] for t in group]
        exit_times = [t.get("exit_time") or t["entry_time"] for t in group]
        min_entry_ts = min(
            pd.Timestamp(et).tz_localize(timezone.utc) if pd.Timestamp(et).tzinfo is None else pd.Timestamp(et)
            for et in entry_times
        )
        max_exit_ts = max(
            pd.Timestamp(et).tz_localize(timezone.utc) if pd.Timestamp(et).tzinfo is None else pd.Timestamp(et)
            for et in exit_times
        )
        bars_before_load = _chart_bars_to_cover_htf_bars(bars_before, chart_tf, context_tf, validation_tf)
        before_df = load_aligned_bars_before([symbol], chart_tf, min_entry_ts, bars_before_load, db_path=path).get(
            symbol, pd.DataFrame()
        )
        after_df = load_aligned_from_db([symbol], chart_tf, min_entry_ts, max_exit_ts, db_path=path).get(
            symbol, pd.DataFrame()
        )
        combined = pd.concat([before_df, after_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
        cache[(symbol, chart_tf)] = combined

    result: list[dict[str, Any]] = []
    for t in trades:
        enrich_score_val: float | None = None
        symbol = t["symbol"]
        chart_tf = t["chart_tf"]
        entry_time = t["entry_time"]
        exit_time = t.get("exit_time") or entry_time
        entry_ts = pd.Timestamp(entry_time)
        if entry_ts.tzinfo is None:
            entry_ts = entry_ts.tz_localize(timezone.utc)
        else:
            entry_ts = entry_ts.tz_convert(timezone.utc)
        exit_ts = pd.Timestamp(exit_time)
        if exit_ts.tzinfo is None:
            exit_ts = exit_ts.tz_localize(timezone.utc)
        else:
            exit_ts = exit_ts.tz_convert(timezone.utc)

        df = cache.get((symbol, chart_tf))
        if df is None or df.empty:
            chart_buf: list[dict[str, Any]] = []
            ctx_buf: list[dict[str, Any]] = []
            val_buf: list[dict[str, Any]] = []
        else:
            if "time" not in df.columns or not pd.api.types.is_datetime64_any_dtype(df["time"]):
                df = df.copy()
                df["time"] = pd.to_datetime(df["time"], utc=True)
            pre = df[df["time"] < entry_ts].tail(bars_before)
            post = df[(df["time"] >= entry_ts) & (df["time"] <= exit_ts)]
            slice_df = (
                pd.concat([pre, post], ignore_index=True).sort_values("time").reset_index(drop=True)
            )
            chart_buf = [_chart_bar_to_dict(row) for _, row in slice_df.iterrows()]
            if ENRICH_SCORE_COLUMN in df.columns and not post.empty:
                val = post[ENRICH_SCORE_COLUMN].iloc[0]
                if val is not None and not pd.isna(val):
                    enrich_score_val = float(val)

            # Context buffer: last bars_before context bars before entry, then entry to exit (use full df so we have enough HTF bars)
            if "ctx_time" in df.columns:
                df_ctx = df.copy()
                df_ctx["ctx_time"] = pd.to_datetime(df_ctx["ctx_time"], utc=True)
                ctx_dedup = df_ctx.drop_duplicates(subset=["ctx_time"], keep="last").sort_values("ctx_time")
                pre_ctx = ctx_dedup[ctx_dedup["ctx_time"] < entry_ts].tail(bars_before)
                post_ctx = ctx_dedup[
                    (ctx_dedup["ctx_time"] >= entry_ts) & (ctx_dedup["ctx_time"] <= exit_ts)
                ]
                ctx_slice = (
                    pd.concat([pre_ctx, post_ctx], ignore_index=True)
                    .sort_values("ctx_time")
                    .reset_index(drop=True)
                )
                ctx_buf = [_ctx_bar_to_dict(row) for _, row in ctx_slice.iterrows()]
            else:
                ctx_buf = []

            # Validation buffer: last bars_before validation bars before entry, then entry to exit (use full df)
            if "val_time" in df.columns:
                df_val = df.copy()
                df_val["val_time"] = pd.to_datetime(df_val["val_time"], utc=True)
                val_dedup = df_val.drop_duplicates(subset=["val_time"], keep="last").sort_values("val_time")
                pre_val = val_dedup[val_dedup["val_time"] < entry_ts].tail(bars_before)
                post_val = val_dedup[
                    (val_dedup["val_time"] >= entry_ts) & (val_dedup["val_time"] <= exit_ts)
                ]
                val_slice = (
                    pd.concat([pre_val, post_val], ignore_index=True)
                    .sort_values("val_time")
                    .reset_index(drop=True)
                )
                val_buf = [_val_bar_to_dict(row) for _, row in val_slice.iterrows()]
            else:
                val_buf = []

        entry_day = entry_time[:10] if isinstance(entry_time, str) and len(entry_time) >= 10 else entry_ts.strftime("%Y-%m-%d")

        entry_map = {
            "trade_id": int(t["id"]),
            "symbol": symbol,
            "chartTF": chart_tf,
            "contextTF": t["context_tf"],
            "validationTF": t["validation_tf"],
            "run_id": t["run_id"],
            "contextBullish": bool(t.get("context_bullish")),
            "validationOk": bool(t.get("validation_ok")),
            "state": "COMPLETED",
            "setupTime": t["setup_time"],
            "entryTime": entry_time,
            "exitTime": t.get("exit_time") or entry_time,
            "entryDay": entry_day,
            "entryPrice": float(t["entry_price"]),
            "sl": float(t["sl"]),
            "tp": float(t["tp"]),
            "slSize": float(t["sl_size"]) if t.get("sl_size") is not None else None,
            "beActive": False,
            "rr": float(t["rr"]) if t.get("rr") is not None else None,
            "exitReason": t["exit_reason"],
            "chartBuffer": chart_buf,
            "contextBuffer": ctx_buf,
            "validationBuffer": val_buf,
            "enrichScore": enrich_score_val,
        }
        result.append(entry_map)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build entry maps for a run (C1); output JSON.")
    parser.add_argument("--run-key", type=str, help="Run key (e.g. usd_jpy_h1_2024_v1)")
    parser.add_argument("--run-id", type=int, help="Run ID")
    parser.add_argument("--out", type=str, help="Write JSON to file (default: stdout)")
    parser.add_argument("--config", type=str, default=None, help="Config YAML path")
    args = parser.parse_args()
    if not args.run_key and args.run_id is None:
        parser.error("Provide --run-key or --run-id")
    config = load_config(args.config) if args.config else load_config()
    db_path = get_db_path(config, args.config)
    maps = build_entry_maps_for_run(
        run_id=args.run_id,
        run_key=args.run_key or None,
        db_path=db_path,
        config=config,
    )
    json_str = json.dumps(maps, indent=2)
    if args.out:
        with open(args.out, "w") as f:
            f.write(json_str)
    else:
        print(json_str)


if __name__ == "__main__":
    main()
