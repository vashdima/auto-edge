"""
C1: Build entry map per trade (JSON for frontend).

For a chosen run (run_id or run_key), queries raw_trades and aligned_candles,
produces a list of entry map dicts (one per trade) in the JSON shape the frontend uses.
No separate entries table; C2 will call this and return the JSON.

Summary mode (no aligned load) and trade-buffers-on-demand are supported for faster UI loads.
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
from pip_math import calc_sl_pips, get_pip_size

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


def _entry_ts_exit_ts(t: dict[str, Any]) -> tuple[pd.Timestamp, pd.Timestamp, str, str]:
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
    return entry_ts, exit_ts, entry_time, exit_time


def _normalize_aligned_frame(combined: pd.DataFrame) -> pd.DataFrame:
    """Ensure time / ctx_time / val_time are UTC datetime; copy once."""
    out = combined.copy()
    if "time" in out.columns:
        out["time"] = pd.to_datetime(out["time"], utc=True)
    if "ctx_time" in out.columns:
        out["ctx_time"] = pd.to_datetime(out["ctx_time"], utc=True)
    if "val_time" in out.columns:
        out["val_time"] = pd.to_datetime(out["val_time"], utc=True)
    return out


def _chart_slice_to_dicts(slice_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Build chart bar dicts using itertuples (faster than iterrows)."""
    if slice_df.empty:
        return []
    base = ["time", "open", "high", "low", "close"]
    if not all(c in slice_df.columns for c in base):
        return []
    opt = [c for c in ("ema_slow", "ema_medium", "ema_fast", "atr") if c in slice_df.columns]
    cols = base + opt
    sub = slice_df[cols]
    out: list[dict[str, Any]] = []
    n_base = 5
    for tup in sub.itertuples(index=False, name=None):
        tm = tup[0]
        t_iso = _to_iso_utc(tm) if isinstance(tm, pd.Timestamp) else str(tm)
        row_dict: dict[str, Any] = {
            "time": t_iso,
            "open": float(tup[1]) if pd.notna(tup[1]) else None,
            "high": float(tup[2]) if pd.notna(tup[2]) else None,
            "low": float(tup[3]) if pd.notna(tup[3]) else None,
            "close": float(tup[4]) if pd.notna(tup[4]) else None,
        }
        j = n_base
        for cname in opt:
            v = tup[j]
            j += 1
            if pd.notna(v):
                row_dict[cname] = float(v)
        out.append(row_dict)
    return out


def _ctx_slice_to_dicts(slice_df: pd.DataFrame) -> list[dict[str, Any]]:
    if slice_df.empty:
        return []
    base = ["ctx_time", "ctx_open", "ctx_high", "ctx_low", "ctx_close"]
    if not all(c in slice_df.columns for c in base):
        return []
    opt = [c for c in ("ctx_ema_slow", "ctx_ema_medium", "ctx_ema_fast") if c in slice_df.columns]
    cols = base + opt
    sub = slice_df[cols]
    out: list[dict[str, Any]] = []
    n_base = 5
    for tup in sub.itertuples(index=False, name=None):
        tm = tup[0]
        t_iso = _to_iso_utc(tm) if isinstance(tm, pd.Timestamp) else str(tm)
        row_dict: dict[str, Any] = {
            "time": t_iso,
            "open": float(tup[1]) if pd.notna(tup[1]) else None,
            "high": float(tup[2]) if pd.notna(tup[2]) else None,
            "low": float(tup[3]) if pd.notna(tup[3]) else None,
            "close": float(tup[4]) if pd.notna(tup[4]) else None,
        }
        j = n_base
        for cname in opt:
            v = tup[j]
            j += 1
            if pd.notna(v):
                if cname == "ctx_ema_slow":
                    row_dict["ema_slow"] = float(v)
                elif cname == "ctx_ema_medium":
                    row_dict["ema_medium"] = float(v)
                elif cname == "ctx_ema_fast":
                    row_dict["ema_fast"] = float(v)
        out.append(row_dict)
    return out


def _val_slice_to_dicts(slice_df: pd.DataFrame) -> list[dict[str, Any]]:
    if slice_df.empty:
        return []
    base = ["val_time", "val_open", "val_high", "val_low", "val_close"]
    if not all(c in slice_df.columns for c in base):
        return []
    opt = [c for c in ("val_ema_slow", "val_ema_medium", "val_ema_fast") if c in slice_df.columns]
    cols = base + opt
    sub = slice_df[cols]
    out: list[dict[str, Any]] = []
    n_base = 5
    for tup in sub.itertuples(index=False, name=None):
        tm = tup[0]
        t_iso = _to_iso_utc(tm) if isinstance(tm, pd.Timestamp) else str(tm)
        row_dict: dict[str, Any] = {
            "time": t_iso,
            "open": float(tup[1]) if pd.notna(tup[1]) else None,
            "high": float(tup[2]) if pd.notna(tup[2]) else None,
            "low": float(tup[3]) if pd.notna(tup[3]) else None,
            "close": float(tup[4]) if pd.notna(tup[4]) else None,
        }
        j = n_base
        for cname in opt:
            v = tup[j]
            j += 1
            if pd.notna(v):
                if cname == "val_ema_slow":
                    row_dict["ema_slow"] = float(v)
                elif cname == "val_ema_medium":
                    row_dict["ema_medium"] = float(v)
                elif cname == "val_ema_fast":
                    row_dict["ema_fast"] = float(v)
        out.append(row_dict)
    return out


def _buffers_for_trade(
    t: dict[str, Any],
    df: pd.DataFrame | None,
    bars_before: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], float | None]:
    """Slice aligned df for one trade; return chart, context, validation buffers and enrich score."""
    if df is None or df.empty:
        return [], [], [], None
    entry_ts, exit_ts, entry_time, exit_time = _entry_ts_exit_ts(t)
    enrich_score_val: float | None = None
    pre = df[df["time"] < entry_ts].tail(bars_before)
    post = df[(df["time"] >= entry_ts) & (df["time"] <= exit_ts)]
    slice_df = pd.concat([pre, post], ignore_index=True).sort_values("time").reset_index(drop=True)
    chart_buf = _chart_slice_to_dicts(slice_df)
    if ENRICH_SCORE_COLUMN in df.columns and not post.empty:
        val = post[ENRICH_SCORE_COLUMN].iloc[0]
        if val is not None and not pd.isna(val):
            enrich_score_val = float(val)

    if "ctx_time" in df.columns:
        ctx_dedup = df.drop_duplicates(subset=["ctx_time"], keep="last").sort_values("ctx_time")
        pre_ctx = ctx_dedup[ctx_dedup["ctx_time"] < entry_ts].tail(bars_before)
        post_ctx = ctx_dedup[(ctx_dedup["ctx_time"] >= entry_ts) & (ctx_dedup["ctx_time"] <= exit_ts)]
        ctx_slice = (
            pd.concat([pre_ctx, post_ctx], ignore_index=True).sort_values("ctx_time").reset_index(drop=True)
        )
        ctx_buf = _ctx_slice_to_dicts(ctx_slice)
    else:
        ctx_buf = []

    if "val_time" in df.columns:
        val_dedup = df.drop_duplicates(subset=["val_time"], keep="last").sort_values("val_time")
        pre_val = val_dedup[val_dedup["val_time"] < entry_ts].tail(bars_before)
        post_val = val_dedup[(val_dedup["val_time"] >= entry_ts) & (val_dedup["val_time"] <= exit_ts)]
        val_slice = (
            pd.concat([pre_val, post_val], ignore_index=True).sort_values("val_time").reset_index(drop=True)
        )
        val_buf = _val_slice_to_dicts(val_slice)
    else:
        val_buf = []

    return chart_buf, ctx_buf, val_buf, enrich_score_val


def _entry_map_dict(
    t: dict[str, Any],
    chart_buf: list[dict[str, Any]],
    ctx_buf: list[dict[str, Any]],
    val_buf: list[dict[str, Any]],
    enrich_score_val: float | None,
) -> dict[str, Any]:
    entry_ts, _, entry_time, _ = _entry_ts_exit_ts(t)
    entry_day = (
        entry_time[:10]
        if isinstance(entry_time, str) and len(entry_time) >= 10
        else entry_ts.strftime("%Y-%m-%d")
    )
    return {
        "trade_id": int(t["id"]),
        "symbol": t["symbol"],
        "chartTF": t["chart_tf"],
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
        "slPips": calc_sl_pips(
            entry_price=float(t["entry_price"]),
            sl_price=float(t["sl"]),
            pip_size=get_pip_size(t["symbol"], entry_price=float(t["entry_price"])),
        ),
        "beActive": False,
        "rr": float(t["rr"]) if t.get("rr") is not None else None,
        "exitReason": t["exit_reason"],
        "chartBuffer": chart_buf,
        "contextBuffer": ctx_buf,
        "validationBuffer": val_buf,
        "enrichScore": enrich_score_val,
    }


def _build_aligned_cache(
    path: str,
    by_key: dict[tuple[str, str], list[dict]],
    bars_before: int,
    context_tf: str,
    validation_tf: str,
) -> dict[tuple[str, str], pd.DataFrame]:
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
        cache[(symbol, chart_tf)] = _normalize_aligned_frame(combined) if not combined.empty else combined
    return cache


def build_entry_summaries_for_run(
    run_id: int | None = None,
    run_key: str | None = None,
    db_path: str | None = None,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Same metadata as full entry maps but empty chart/context/validation buffers and no enrich score.
    Does not query aligned_candles (fast for large runs).
    """
    if run_id is None and not run_key:
        return []
    config = config or load_config()
    path = db_path or get_db_path(config)
    if not os.path.exists(path):
        return []

    with sqlite3.connect(path) as conn:
        init_b2_tables(conn)
        rid = _resolve_run_id(conn, run_id, run_key)
        if rid is None:
            return []
        trades = _load_raw_trades(conn, rid)

    if not trades:
        return []

    return [_entry_map_dict(t, [], [], [], None) for t in trades]


def build_trade_buffers_for_run(
    run_id: int | None = None,
    run_key: str | None = None,
    trade_ids: list[int] | None = None,
    db_path: str | None = None,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Build chart/context/validation buffers (and enrichScore) for specific trade ids in a run.
    Returns list of {trade_id, chartBuffer, contextBuffer, validationBuffer, enrichScore}.
    """
    if run_id is None and not run_key:
        return []
    if not trade_ids:
        return []
    id_set = set(trade_ids)
    config = config or load_config()
    path = db_path or get_db_path(config)
    if not os.path.exists(path):
        return []

    bars_before = _bars_before_from_config(config)
    timeframes = config.get("timeframes") or {}
    context_tf = timeframes.get("context", "W")
    validation_tf = timeframes.get("validation", "D")

    with sqlite3.connect(path) as conn:
        init_b2_tables(conn)
        rid = _resolve_run_id(conn, run_id, run_key)
        if rid is None:
            return []
        trades = _load_raw_trades(conn, rid)

    trades = [t for t in trades if int(t["id"]) in id_set]
    if not trades:
        return []

    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in trades:
        by_key[(t["symbol"], t["chart_tf"])].append(t)

    cache = _build_aligned_cache(path, by_key, bars_before, context_tf, validation_tf)

    out: list[dict[str, Any]] = []
    for t in trades:
        symbol = t["symbol"]
        chart_tf = t["chart_tf"]
        df = cache.get((symbol, chart_tf))
        chart_buf, ctx_buf, val_buf, enrich = _buffers_for_trade(t, df, bars_before)
        out.append(
            {
                "trade_id": int(t["id"]),
                "chartBuffer": chart_buf,
                "contextBuffer": ctx_buf,
                "validationBuffer": val_buf,
                "enrichScore": enrich,
            }
        )
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
        init_b2_tables(conn)
        rid = _resolve_run_id(conn, run_id, run_key)
        if rid is None:
            return []
        trades = _load_raw_trades(conn, rid)

    if not trades:
        return []

    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in trades:
        by_key[(t["symbol"], t["chart_tf"])].append(t)

    cache = _build_aligned_cache(path, by_key, bars_before, context_tf, validation_tf)

    result: list[dict[str, Any]] = []
    for t in trades:
        symbol = t["symbol"]
        chart_tf = t["chart_tf"]
        df = cache.get((symbol, chart_tf))
        chart_buf, ctx_buf, val_buf, enrich = _buffers_for_trade(t, df, bars_before)
        result.append(_entry_map_dict(t, chart_buf, ctx_buf, val_buf, enrich))

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
