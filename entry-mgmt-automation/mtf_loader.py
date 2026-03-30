"""
Multi-timeframe loader (Phase A2).

Loads config.yaml, fetches chart / context / validation candles via A1 with per-TF buffers,
aligns so each chart bar has current context and validation bar with running OHLC (no lookahead),
returns dict[symbol, aligned DataFrame] for Phase B.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import numpy as np
import pandas as pd
import yaml

from oanda_client import OandaClient, _parse_iso_time
from scanner_indicators import ALL_INDICATOR_COLUMNS
from pip_math import ensure_pip_metadata_for_symbols

ENRICH_SCORE_COLUMN = "enrich_score"

# Default SQLite path (relative to entry-mgmt-automation); override via config database.path
DEFAULT_DB_PATH = "data/trendfinder.db"

# Same directory as this module (entry-mgmt-automation)
_DIR = os.path.dirname(os.path.abspath(__file__))

# Period per granularity for "current bar" assignment (ctx_time <= T < ctx_time + period)
GRANULARITY_DELTA = {
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
    "M": timedelta(days=31),
}

# Empty aligned DataFrame column schema (base OHLC + context + validation)
_ALIGNED_COLUMNS = [
    "time", "open", "high", "low", "close", "volume",
    "ctx_time", "ctx_open", "ctx_high", "ctx_low", "ctx_close", "ctx_volume",
    "val_time", "val_open", "val_high", "val_low", "val_close", "val_volume",
]

# Base columns we SELECT from aligned_candles (with symbol, chart_tf for filtering)
_SCAN_SELECT_BASE = [
    "symbol", "chart_tf", "time", "open", "high", "low", "close", "volume",
    "ctx_time", "ctx_open", "ctx_high", "ctx_low", "ctx_close", "ctx_volume",
    "val_time", "val_open", "val_high", "val_low", "val_close", "val_volume",
]


def _aligned_candles_columns_with_indicators(conn: sqlite3.Connection) -> tuple[list[str], list[str]]:
    """
    Return (select_columns, result_columns) for load_aligned_from_db.
    select_columns: for SELECT (includes symbol, chart_tf); result_columns: for DataFrame (no symbol, chart_tf).
    Includes indicator columns (ema_slow, atr, etc.) and enrich_score when present in the table.
    """
    cur = conn.execute("PRAGMA table_info(aligned_candles)")
    existing = {row[1] for row in cur.fetchall()}
    indicator_present = [c for c in ALL_INDICATOR_COLUMNS if c in existing]
    enrich_present = [ENRICH_SCORE_COLUMN] if ENRICH_SCORE_COLUMN in existing else []
    select_cols = _SCAN_SELECT_BASE + indicator_present + enrich_present
    result_cols = _ALIGNED_COLUMNS + indicator_present + enrich_present
    return (select_cols, result_cols)


def resolve_config_path(config_path: str | None = None) -> str:
    """Same path resolution as load_config (default _DIR/config.yaml)."""
    if config_path is None:
        return os.path.join(_DIR, "config.yaml")
    return config_path


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Load config YAML from path or default _DIR/config.yaml."""
    path = resolve_config_path(config_path)
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_scan_windows(config: dict[str, Any]) -> list[tuple[datetime, datetime]]:
    """
    Resolve scan windows from config.

    Supports:
      - Legacy single window: scan.from + scan.to
      - Multi-window: scan.windows = [{from, to}, ...]
    Returns parsed UTC datetimes.
    """
    scan = config.get("scan") or {}
    windows = scan.get("windows")
    resolved: list[tuple[datetime, datetime]] = []

    if windows:
        if not isinstance(windows, list):
            raise ValueError("scan.windows must be a list of {from, to} objects")
        for i, w in enumerate(windows):
            if not isinstance(w, dict):
                raise ValueError(f"scan.windows[{i}] must be an object with from/to")
            from_raw = w.get("from")
            to_raw = w.get("to")
            if not from_raw or not to_raw:
                raise ValueError(f"scan.windows[{i}] must include both from and to")
            from_ts = _parse_iso_time(from_raw)
            to_ts = _parse_iso_time(to_raw)
            if from_ts > to_ts:
                raise ValueError(f"scan.windows[{i}] has from > to")
            resolved.append((from_ts, to_ts))
    else:
        from_raw = scan.get("from")
        to_raw = scan.get("to")
        if not from_raw or not to_raw:
            raise ValueError("scan.from and scan.to are required when scan.windows is not set")
        from_ts = _parse_iso_time(from_raw)
        to_ts = _parse_iso_time(to_raw)
        if from_ts > to_ts:
            raise ValueError("scan.from must be <= scan.to")
        resolved.append((from_ts, to_ts))

    if not resolved:
        raise ValueError("No scan windows resolved from config")
    return resolved


def get_db_path(config: dict[str, Any], config_path: str | None = None) -> str:
    """Resolve SQLite DB path from config; default data/trendfinder.db under _DIR."""
    raw = (config.get("database") or {}).get("path") or DEFAULT_DB_PATH
    if os.path.isabs(raw):
        return raw
    base = _DIR if config_path is None else os.path.dirname(os.path.abspath(config_path))
    return os.path.join(base, raw)


def _init_aligned_candles_db(conn: sqlite3.Connection) -> None:
    """Create aligned_candles table and index if not exist; enable WAL."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS aligned_candles (
            symbol TEXT NOT NULL,
            chart_tf TEXT NOT NULL,
            time TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            ctx_time TEXT, ctx_open REAL, ctx_high REAL, ctx_low REAL, ctx_close REAL, ctx_volume REAL,
            val_time TEXT, val_open REAL, val_high REAL, val_low REAL, val_close REAL, val_volume REAL,
            PRIMARY KEY (symbol, chart_tf, time)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_aligned_candles_symbol_chart_tf_time "
        "ON aligned_candles (symbol, chart_tf, time)"
    )
    _ensure_indicator_columns(conn)
    _ensure_enrich_column(conn)
    conn.commit()


def _init_scan_runs_table(conn: sqlite3.Connection) -> None:
    """Create scan_runs table (B2). One row per run for run selector; Phase C lists runs without querying raw_trades."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_from TEXT NOT NULL,
            scan_to TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()


def _init_raw_trades_table(conn: sqlite3.Connection) -> None:
    """Create raw_trades table (B2). Only completed trades; Phase C filters by run_id."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            chart_tf TEXT NOT NULL,
            context_tf TEXT NOT NULL,
            validation_tf TEXT NOT NULL,
            setup_time TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            entry_price REAL NOT NULL,
            sl REAL NOT NULL,
            tp REAL NOT NULL,
            sl_size REAL,
            exit_reason TEXT NOT NULL,
            rr REAL,
            context_bullish INTEGER NOT NULL,
            validation_ok INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_trades_run_id ON raw_trades (run_id)")
    conn.commit()


def ensure_raw_trades_exit_time(conn: sqlite3.Connection) -> None:
    """Add exit_time column to raw_trades if missing (C1: bar time when trade completed)."""
    cur = conn.execute("PRAGMA table_info(raw_trades)")
    columns = {row[1] for row in cur.fetchall()}
    if "exit_time" not in columns:
        conn.execute("ALTER TABLE raw_trades ADD COLUMN exit_time TEXT")
    conn.commit()


def ensure_scan_runs_run_key(conn: sqlite3.Connection) -> None:
    """Add run_key column and unique index to scan_runs if missing (B2.4 migration)."""
    cur = conn.execute("PRAGMA table_info(scan_runs)")
    columns = {row[1] for row in cur.fetchall()}
    if "run_key" not in columns:
        conn.execute("ALTER TABLE scan_runs ADD COLUMN run_key TEXT")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_scan_runs_run_key ON scan_runs(run_key)"
    )
    conn.commit()


def ensure_scan_runs_config_yaml(conn: sqlite3.Connection) -> None:
    """Add config_yaml column (per-run snapshot of config file) if missing."""
    cur = conn.execute("PRAGMA table_info(scan_runs)")
    columns = {row[1] for row in cur.fetchall()}
    if "config_yaml" not in columns:
        conn.execute("ALTER TABLE scan_runs ADD COLUMN config_yaml TEXT")
    conn.commit()


def get_or_create_run_id(
    conn: sqlite3.Connection,
    run_key: str,
    scan_from: str,
    scan_to: str,
    created_at: str,
    overwrite: bool = True,
    config_yaml: str | None = None,
) -> int:
    """
    Return run_id for the given run_key. If run_key exists: reuse run_id, and if overwrite
    delete existing raw_trades and update scan_runs metadata. If not exists: insert new row.
    Uses a transaction for atomicity.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute("SELECT run_id FROM scan_runs WHERE run_key = ?", (run_key,))
        row = cur.fetchone()
        if row is not None:
            run_id = row[0]
            if not overwrite:
                conn.rollback()
                raise ValueError(
                    f"run_key '{run_key}' already exists; pick a new key or set run.overwrite=true"
                )
            conn.execute("DELETE FROM raw_trades WHERE run_id = ?", (run_id,))
            conn.execute(
                "UPDATE scan_runs SET scan_from = ?, scan_to = ?, created_at = ?, config_yaml = ? WHERE run_id = ?",
                (scan_from, scan_to, created_at, config_yaml, run_id),
            )
            conn.commit()
            return run_id
        cur = conn.execute(
            "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at, config_yaml) VALUES (?, ?, ?, ?, ?)",
            (run_key, scan_from, scan_to, created_at, config_yaml),
        )
        run_id = cur.lastrowid
        conn.commit()
        return run_id
    except Exception:
        conn.rollback()
        raise


def init_b2_tables(conn: sqlite3.Connection) -> None:
    """Create B2 tables (scan_runs, raw_trades) if not exist. Call from scanner_entry_mgmt before first B2 write."""
    _init_scan_runs_table(conn)
    _init_raw_trades_table(conn)
    ensure_raw_trades_exit_time(conn)
    ensure_scan_runs_run_key(conn)
    ensure_scan_runs_config_yaml(conn)


def _ensure_indicator_columns(conn: sqlite3.Connection) -> None:
    """Add B1.1 indicator columns to aligned_candles if not present."""
    cur = conn.execute("PRAGMA table_info(aligned_candles)")
    existing = {row[1] for row in cur.fetchall()}
    for col in ALL_INDICATOR_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE aligned_candles ADD COLUMN {col} REAL")


def _ensure_enrich_column(conn: sqlite3.Connection) -> None:
    """Add enrich_score column to aligned_candles if not present (index enrichment)."""
    cur = conn.execute("PRAGMA table_info(aligned_candles)")
    existing = {row[1] for row in cur.fetchall()}
    if ENRICH_SCORE_COLUMN not in existing:
        conn.execute(f"ALTER TABLE aligned_candles ADD COLUMN {ENRICH_SCORE_COLUMN} REAL")


def _to_iso8601_utc(dt: datetime | pd.Timestamp) -> str | None:
    """Format datetime as ISO8601 UTC string for SQLite; return None for NaT/None."""
    if dt is None or pd.isna(dt):
        return None
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _write_aligned_to_db(conn: sqlite3.Connection, symbol: str, chart_tf: str, aligned: pd.DataFrame) -> None:
    """Delete existing rows for (symbol, chart_tf) in aligned time range, then insert aligned rows."""
    if aligned.empty:
        return
    t_min = _to_iso8601_utc(aligned["time"].min())
    t_max = _to_iso8601_utc(aligned["time"].max())
    conn.execute(
        "DELETE FROM aligned_candles WHERE symbol = ? AND chart_tf = ? AND time >= ? AND time <= ?",
        (symbol, chart_tf, t_min, t_max),
    )
    insert_cols = [
        "time", "open", "high", "low", "close", "volume",
        "ctx_time", "ctx_open", "ctx_high", "ctx_low", "ctx_close", "ctx_volume",
        "val_time", "val_open", "val_high", "val_low", "val_close", "val_volume",
    ]
    rows = []
    for tup in aligned[insert_cols].itertuples(index=False, name=None):
        rows.append((
            symbol,
            chart_tf,
            _to_iso8601_utc(tup[0]),
            float(tup[1]), float(tup[2]), float(tup[3]), float(tup[4]), float(tup[5]),
            _to_iso8601_utc(tup[6]), float(tup[7]), float(tup[8]), float(tup[9]), float(tup[10]), float(tup[11]),
            _to_iso8601_utc(tup[12]), float(tup[13]), float(tup[14]), float(tup[15]), float(tup[16]), float(tup[17]),
        ))
    conn.executemany(
        """INSERT INTO aligned_candles (
            symbol, chart_tf, time, open, high, low, close, volume,
            ctx_time, ctx_open, ctx_high, ctx_low, ctx_close, ctx_volume,
            val_time, val_open, val_high, val_low, val_close, val_volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()


def write_indicators_to_db(
    conn: sqlite3.Connection,
    symbol: str,
    chart_tf: str,
    df: pd.DataFrame,
) -> None:
    """
    Update aligned_candles with indicator columns (B1.1). df must have 'time' and all ALL_INDICATOR_COLUMNS.
    Rows are matched by (symbol, chart_tf, time). Call after add_all_indicators() at start of Phase B.
    """
    if df.empty or not all(c in df.columns for c in ALL_INDICATOR_COLUMNS):
        return
    placeholders = ", ".join(f"{c} = ?" for c in ALL_INDICATOR_COLUMNS)
    sql = f"UPDATE aligned_candles SET {placeholders} WHERE symbol = ? AND chart_tf = ? AND time = ?"
    rows = []
    for _, row in df.iterrows():
        t_str = _to_iso8601_utc(row["time"])
        vals = [float(row[c]) if pd.notna(row[c]) else None for c in ALL_INDICATOR_COLUMNS]
        rows.append((*vals, symbol, chart_tf, t_str))
    conn.executemany(sql, rows)
    conn.commit()


def write_enrich_scores_to_db(
    conn: sqlite3.Connection,
    symbol: str,
    chart_tf: str,
    df: pd.DataFrame,
) -> None:
    """
    Update aligned_candles enrich_score for each row. df must have 'time' and 'enrich_score'.
    Rows are matched by (symbol, chart_tf, time). Call from B1.1 after attaching enrich_score to DataFrame.
    """
    if df.empty or ENRICH_SCORE_COLUMN not in df.columns:
        return
    sql = f"UPDATE aligned_candles SET {ENRICH_SCORE_COLUMN} = ? WHERE symbol = ? AND chart_tf = ? AND time = ?"
    rows = []
    for _, row in df.iterrows():
        t_str = _to_iso8601_utc(row["time"])
        val = row[ENRICH_SCORE_COLUMN]
        val_float = float(val) if val is not None and pd.notna(val) else None
        rows.append((val_float, symbol, chart_tf, t_str))
    conn.executemany(sql, rows)
    conn.commit()


def load_aligned_from_db(
    symbols: list[str],
    chart_tf: str,
    from_time: datetime | str,
    to_time: datetime | str,
    db_path: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load aligned candle data from SQLite for the given symbols, chart_tf, and time range (B1.0).

    Returns dict[symbol, DataFrame] with same columns as load_aligned(): time, open, high, low, close, volume,
    ctx_time, ctx_open, ..., val_time, val_open, .... All time columns are parsed to timezone-aware UTC datetime.

    Args:
        symbols: List of instrument symbols (e.g. ["EUR_USD", "USD_JPY"]).
        chart_tf: Chart timeframe (e.g. "M15", "H1").
        from_time: Start of range (inclusive), datetime or ISO8601 string.
        to_time: End of range (inclusive), datetime or ISO8601 string.
        db_path: Path to SQLite DB; if None, uses DEFAULT_DB_PATH relative to _DIR.
    """
    if not symbols:
        return {}
    from_ts = _parse_iso_time(from_time) if isinstance(from_time, str) else pd.Timestamp(from_time).tz_convert(timezone.utc)
    to_ts = _parse_iso_time(to_time) if isinstance(to_time, str) else pd.Timestamp(to_time).tz_convert(timezone.utc)
    from_str = _to_iso8601_utc(from_ts)
    to_str = _to_iso8601_utc(to_ts)

    path = db_path or os.path.join(_DIR, DEFAULT_DB_PATH)
    if not os.path.exists(path):
        return {s: pd.DataFrame(columns=_ALIGNED_COLUMNS) for s in symbols}

    conn = sqlite3.connect(path)
    select_cols, result_cols = _aligned_candles_columns_with_indicators(conn)
    placeholders = ",".join("?" * len(symbols))
    select_list = ", ".join(select_cols)
    query = (
        f"SELECT {select_list} "
        f"FROM aligned_candles WHERE symbol IN ({placeholders}) AND chart_tf = ? AND time >= ? AND time <= ? ORDER BY symbol, time"
    )
    params = (*symbols, chart_tf, from_str, to_str)
    df_all = pd.read_sql_query(query, conn, params=params)
    conn.close()

    result: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        sub = df_all.loc[df_all["symbol"] == sym].drop(columns=["symbol", "chart_tf"])
        if sub.empty:
            result[sym] = pd.DataFrame(columns=result_cols)
            continue
        for col in ("time", "ctx_time", "val_time"):
            sub[col] = pd.to_datetime(sub[col], utc=True)
        sub = sub[result_cols]
        result[sym] = sub.reset_index(drop=True)
    return result


def load_aligned_bars_before(
    symbols: list[str],
    chart_tf: str,
    before_time: datetime | str | pd.Timestamp,
    n_bars: int,
    db_path: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load the last n_bars aligned rows with time < before_time, per symbol (bar-count, not time window).

    Returns dict[symbol, DataFrame] with same columns as load_aligned_from_db, in ascending time order.
    Use time < before_time so there is no overlap with a range that starts at before_time.
    """
    if not symbols or n_bars <= 0:
        return {s: pd.DataFrame(columns=_ALIGNED_COLUMNS) for s in symbols}
    before_ts = (
        _parse_iso_time(before_time)
        if isinstance(before_time, str)
        else pd.Timestamp(before_time).tz_convert(timezone.utc)
    )
    before_str = _to_iso8601_utc(before_ts)

    path = db_path or os.path.join(_DIR, DEFAULT_DB_PATH)
    if not os.path.exists(path):
        return {s: pd.DataFrame(columns=_ALIGNED_COLUMNS) for s in symbols}

    conn = sqlite3.connect(path)
    select_cols, result_cols = _aligned_candles_columns_with_indicators(conn)
    select_list = ", ".join(select_cols)
    result: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        query = (
            f"SELECT {select_list} "
            f"FROM aligned_candles WHERE symbol = ? AND chart_tf = ? AND time < ? ORDER BY time DESC LIMIT ?"
        )
        df_sub = pd.read_sql_query(query, conn, params=(sym, chart_tf, before_str, n_bars))
        if df_sub.empty:
            result[sym] = pd.DataFrame(columns=result_cols)
            continue
        sub = df_sub.drop(columns=["symbol", "chart_tf"])
        sub = sub.iloc[::-1].reset_index(drop=True)  # ascending time
        for col in ("time", "ctx_time", "val_time"):
            sub[col] = pd.to_datetime(sub[col], utc=True)
        sub = sub[result_cols]
        result[sym] = sub.reset_index(drop=True)
    conn.close()
    return result


def load_aligned_for_scan(
    config_path: str | None = None,
    from_time: datetime | None = None,
    to_time: datetime | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load aligned data from SQLite for the scan window defined in config (B1.0 wiring).

    Reads config for scan.from, scan.to, timeframes.entry (chart_tf), symbols, and database.path.
    Returns dict[symbol, DataFrame] with same shape as load_aligned(), but only rows in [from, to].
    """
    config = load_config(config_path)
    chart_tf = config["timeframes"]["entry"]
    symbols = config.get("symbols") or ["EUR_USD"]
    db_path = get_db_path(config, config_path)
    if from_time is not None and to_time is not None:
        return load_aligned_from_db(symbols, chart_tf, from_time, to_time, db_path=db_path)

    windows = resolve_scan_windows(config)
    per_window = [
        load_aligned_from_db(symbols, chart_tf, w_from, w_to, db_path=db_path)
        for (w_from, w_to) in windows
    ]
    result: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        frames = [d[sym] for d in per_window if sym in d and not d[sym].empty]
        if not frames:
            result[sym] = pd.DataFrame(columns=_ALIGNED_COLUMNS)
            continue
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
        result[sym] = merged
    return result


def load_aligned_full_buffer(config_path: str | None = None) -> dict[str, pd.DataFrame]:
    """
    Load aligned data from SQLite for the full range (buffer + scan window) defined in config.

    Uses the same range mtf_loader writes: from fetch_from_chart to scan.to, so that B1.1 can
    compute and store indicators for every aligned row, giving context/validation EMAs from the
    first bar for entry_maps and charts.
    """
    config = load_config(config_path)
    chart_tf = config["timeframes"]["entry"]
    symbols = config.get("symbols") or ["EUR_USD"]
    ensure_pip_metadata_for_symbols(symbols)
    db_path = get_db_path(config, config_path)
    windows = resolve_scan_windows(config)
    per_window: list[dict[str, pd.DataFrame]] = []
    for (w_from, w_to) in windows:
        fetch_from_chart, _, _, to_ts = _buffer_fetch_times(config, from_ts=w_from, to_ts=w_to)
        per_window.append(load_aligned_from_db(symbols, chart_tf, fetch_from_chart, to_ts, db_path=db_path))
    result: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        frames = [d[sym] for d in per_window if sym in d and not d[sym].empty]
        if not frames:
            result[sym] = pd.DataFrame(columns=_ALIGNED_COLUMNS)
            continue
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
        result[sym] = merged
    return result


def _buffer_fetch_times(
    config: dict[str, Any],
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
) -> tuple[datetime, datetime, datetime, datetime]:
    """Compute fetch_from for chart, context, validation and to_ts. All UTC.

    Chart fetch start is the minimum of the three buffer boundaries so that aligned
    data contains enough history for chart, context, and validation EMAs (one row per
    chart bar, so chart must extend back far enough to span buffer_bars on each TF).
    """
    if from_ts is None or to_ts is None:
        windows = resolve_scan_windows(config)
        from_ts = min(w[0] for w in windows)
        to_ts = max(w[1] for w in windows)
    timeframes = config["timeframes"]
    entry_detection = config.get("entry_detection") or {}
    slow = entry_detection.get("slowEMAPeriod") or entry_detection.get("ema20Period") or 20
    medium = entry_detection.get("mediumEMAPeriod") or entry_detection.get("ema50Period") or 50
    fast = entry_detection.get("fastEMAPeriod") or entry_detection.get("ema100Period") or 100
    buffer_bars = max(slow, medium, fast)

    chart_tf = timeframes["entry"]
    context_tf = timeframes["context"]
    validation_tf = timeframes["validation"]

    delta_c = GRANULARITY_DELTA.get(chart_tf) or timedelta(hours=1)
    delta_ctx = GRANULARITY_DELTA.get(context_tf) or timedelta(days=1)
    delta_val = GRANULARITY_DELTA.get(validation_tf) or timedelta(weeks=1)

    fetch_from_context = from_ts - buffer_bars * delta_ctx
    fetch_from_validation = from_ts - buffer_bars * delta_val
    # Chart must extend back so aligned rows span buffer_bars for context and validation too
    fetch_from_chart = min(
        from_ts - buffer_bars * delta_c,
        fetch_from_context,
        fetch_from_validation,
    )

    return fetch_from_chart, fetch_from_context, fetch_from_validation, to_ts


def _align_current_bar_running_ohlc(
    chart_df: pd.DataFrame,
    context_df: pd.DataFrame,
    validation_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Align chart with context and validation: current bar (we're in) + running OHLC.
    - ctx_time / val_time = bar start containing this chart time (merge_asof backward).
    - ctx_open / val_open from context/validation DataFrame.
    - ctx_high, ctx_low = running max/min of chart high/low within that bar. ctx_close = confirmed context bar close (no lookahead: previous ctx bar close until current ctx bar closes; on the last chart bar of the ctx period, current ctx bar close). Same for val_close.
    """
    if chart_df.empty:
        return pd.DataFrame(columns=_ALIGNED_COLUMNS)

    chart = chart_df.sort_values("time").reset_index(drop=True)
    context_sorted = context_df.sort_values("time").reset_index(drop=True)
    validation_sorted = validation_df.sort_values("time").reset_index(drop=True)

    # Assign current context bar start (bar we're in)
    ctx_merge = pd.merge_asof(
        chart[["time"]],
        context_sorted[["time"]].rename(columns={"time": "ctx_time"}),
        left_on="time",
        right_on="ctx_time",
        direction="backward",
    )
    chart = chart.copy()
    chart["ctx_time"] = pd.to_datetime(ctx_merge["ctx_time"].values, utc=True)

    # Assign current validation bar start
    val_merge = pd.merge_asof(
        chart[["time"]],
        validation_sorted[["time"]].rename(columns={"time": "val_time"}),
        left_on="time",
        right_on="val_time",
        direction="backward",
    )
    chart["val_time"] = pd.to_datetime(val_merge["val_time"].values, utc=True)

    # ctx_open from context_df (normalize to UTC so merge on ctx_time matches)
    ctx_opens = context_sorted[["time", "open"]].rename(columns={"time": "ctx_time", "open": "ctx_open"})
    ctx_opens["ctx_time"] = pd.to_datetime(ctx_opens["ctx_time"], utc=True)
    chart = chart.merge(ctx_opens, on="ctx_time", how="left")
    # Running ctx_high, ctx_low within each context bar
    chart["ctx_high"] = chart.groupby("ctx_time", group_keys=False)["high"].cummax()
    chart["ctx_low"] = chart.groupby("ctx_time", group_keys=False)["low"].cummin()
    chart["ctx_volume"] = chart.groupby("ctx_time", group_keys=False)["volume"].cumsum()
    # ctx_close = confirmed context bar close (Pine lookahead_off): previous ctx bar close until current ctx bar closes
    _ctx_bars = chart.groupby("ctx_time", as_index=False).agg({"close": "last"}).sort_values("ctx_time")
    _ctx_bars["_ctx_bar_close"] = _ctx_bars["close"]
    _ctx_bars["_ctx_prev_close"] = _ctx_bars["_ctx_bar_close"].shift(1)
    chart = chart.merge(_ctx_bars[["ctx_time", "_ctx_bar_close", "_ctx_prev_close"]], on="ctx_time", how="left")
    _is_last_ctx = chart.groupby("ctx_time", group_keys=False)["time"].transform("max") == chart["time"]
    chart["ctx_close"] = np.where(_is_last_ctx, chart["_ctx_bar_close"], chart["_ctx_prev_close"])
    chart = chart.drop(columns=["_ctx_bar_close", "_ctx_prev_close"])

    # val_open from validation_df (normalize to UTC so merge on val_time matches)
    val_opens = validation_sorted[["time", "open"]].rename(columns={"time": "val_time", "open": "val_open"})
    val_opens["val_time"] = pd.to_datetime(val_opens["val_time"], utc=True)
    chart = chart.merge(val_opens, on="val_time", how="left")
    chart["val_high"] = chart.groupby("val_time", group_keys=False)["high"].cummax()
    chart["val_low"] = chart.groupby("val_time", group_keys=False)["low"].cummin()
    chart["val_volume"] = chart.groupby("val_time", group_keys=False)["volume"].cumsum()
    # val_close = confirmed validation bar close (same no-lookahead logic)
    _val_bars = chart.groupby("val_time", as_index=False).agg({"close": "last"}).sort_values("val_time")
    _val_bars["_val_bar_close"] = _val_bars["close"]
    _val_bars["_val_prev_close"] = _val_bars["_val_bar_close"].shift(1)
    chart = chart.merge(_val_bars[["val_time", "_val_bar_close", "_val_prev_close"]], on="val_time", how="left")
    _is_last_val = chart.groupby("val_time", group_keys=False)["time"].transform("max") == chart["time"]
    chart["val_close"] = np.where(_is_last_val, chart["_val_bar_close"], chart["_val_prev_close"])
    chart = chart.drop(columns=["_val_bar_close", "_val_prev_close"])

    # Column order: chart then ctx_* then val_*
    return chart[
        [
            "time", "open", "high", "low", "close", "volume",
            "ctx_time", "ctx_open", "ctx_high", "ctx_low", "ctx_close", "ctx_volume",
            "val_time", "val_open", "val_high", "val_low", "val_close", "val_volume",
        ]
    ].copy()


def _resolve_mtf_fetch_max_symbol_workers(config: dict[str, Any]) -> int:
    """Bounded parallel symbol count for load_aligned (1–8). Default 1 (sequential symbols)."""
    mtf = config.get("mtf_fetch") or {}
    raw = mtf.get("max_symbol_workers", 1)
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = 1
    return max(1, min(n, 8))


def _fetch_three_timeframes_parallel(
    instrument: str,
    chart_tf: str,
    context_tf: str,
    validation_tf: str,
    fetch_from_chart: datetime,
    fetch_from_context: datetime,
    fetch_from_validation: datetime,
    to_ts: datetime,
    *,
    client_factory: Callable[[], Any] | None = None,
    candle_chunk_progress: Callable[[str, str, int, int], None] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fetch chart, context, validation candles concurrently (one OandaClient per thread).
    Returns (chart_df, context_df, validation_df) in timeframe order.
    """
    specs: list[tuple[str, datetime]] = [
        (chart_tf, fetch_from_chart),
        (context_tf, fetch_from_context),
        (validation_tf, fetch_from_validation),
    ]

    def _make_client() -> Any:
        if client_factory is not None:
            return client_factory()
        return OandaClient()

    def _fetch_one(spec: tuple[str, datetime]) -> pd.DataFrame:
        gran, from_t = spec
        client = _make_client()
        if candle_chunk_progress is not None:
            return client.fetch_candles(
                instrument, gran, from_t, to_ts, on_chunk=candle_chunk_progress
            )
        return client.fetch_candles(instrument, gran, from_t, to_ts)

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_fetch_one, s) for s in specs]
        dfs = [f.result() for f in futures]
    return dfs[0], dfs[1], dfs[2]


def _fetch_and_align_one_symbol(
    instrument: str,
    chart_tf: str,
    context_tf: str,
    validation_tf: str,
    fetch_from_chart: datetime,
    fetch_from_context: datetime,
    fetch_from_validation: datetime,
    to_ts: datetime,
    *,
    client_factory: Callable[[], Any] | None = None,
    candle_chunk_progress: Callable[[str, str, int, int], None] | None = None,
) -> pd.DataFrame:
    chart_df, context_df, validation_df = _fetch_three_timeframes_parallel(
        instrument,
        chart_tf,
        context_tf,
        validation_tf,
        fetch_from_chart,
        fetch_from_context,
        fetch_from_validation,
        to_ts,
        client_factory=client_factory,
        candle_chunk_progress=candle_chunk_progress,
    )
    if chart_df.empty:
        return pd.DataFrame(columns=_ALIGNED_COLUMNS)
    return _align_current_bar_running_ohlc(chart_df, context_df, validation_df)


def load_aligned(
    config_path: str | None = None,
    *,
    progress: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Load config, fetch chart/context/validation for each symbol with buffer,
    align (current bar + running OHLC), write to SQLite aligned_candles, return dict[symbol, aligned_df].

    Aligned DataFrame columns: time, open, high, low, close, volume (chart),
    ctx_time, ctx_open, ctx_high, ctx_low, ctx_close, ctx_volume,
    val_time, val_open, val_high, val_low, val_close, val_volume.
    All times UTC. Sorted by time. Buffer rows are included; Phase B filters scan window (from <= time <= to).

    If progress is True, prints a short header, per-symbol status, and Oanda pagination
    lines (per HTTP page per timeframe) to stderr (flushed).
    """
    config = load_config(config_path)
    timeframes = config["timeframes"]
    chart_tf = timeframes["entry"]
    context_tf = timeframes["context"]
    validation_tf = timeframes["validation"]
    symbols = config.get("symbols") or ["EUR_USD"]
    ensure_pip_metadata_for_symbols(symbols)
    windows = resolve_scan_windows(config)
    global_from = min(w[0] for w in windows)
    global_to = max(w[1] for w in windows)
    fetch_from_chart, fetch_from_context, fetch_from_validation, to_ts = _buffer_fetch_times(
        config, from_ts=global_from, to_ts=global_to
    )

    db_path = get_db_path(config, config_path)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    _init_aligned_candles_db(conn)

    max_symbol_workers = _resolve_mtf_fetch_max_symbol_workers(config)
    result: dict[str, pd.DataFrame] = {s: pd.DataFrame(columns=_ALIGNED_COLUMNS) for s in symbols}
    n_sym = len(symbols)

    if progress and n_sym:
        print(
            f"mtf load: {n_sym} symbol(s), workers={max_symbol_workers}, "
            f"TFs entry={chart_tf} context={context_tf} validation={validation_tf}, "
            f"chart range {fetch_from_chart.isoformat()} → {to_ts.isoformat()}",
            file=sys.stderr,
            flush=True,
        )

    chunk_lock = threading.Lock()
    candle_chunk_progress: Callable[[str, str, int, int], None] | None = None
    if progress:

        def _candle_chunk_progress(inst: str, gran: str, page: int, rows: int) -> None:
            with chunk_lock:
                print(
                    f"mtf load {inst} {gran}: page {page}, {rows} candles",
                    file=sys.stderr,
                    flush=True,
                )

        candle_chunk_progress = _candle_chunk_progress

    try:
        if max_symbol_workers == 1:
            for i, instrument in enumerate(symbols):
                if progress:
                    print(
                        f"mtf load [{i + 1}/{n_sym}] {instrument}: fetching…",
                        file=sys.stderr,
                        flush=True,
                    )
                t0 = time.perf_counter()
                aligned = _fetch_and_align_one_symbol(
                    instrument,
                    chart_tf,
                    context_tf,
                    validation_tf,
                    fetch_from_chart,
                    fetch_from_context,
                    fetch_from_validation,
                    to_ts,
                    candle_chunk_progress=candle_chunk_progress,
                )
                result[instrument] = aligned
                if not aligned.empty:
                    _write_aligned_to_db(conn, instrument, chart_tf, aligned)
                if progress:
                    dt = time.perf_counter() - t0
                    print(
                        f"mtf load [{i + 1}/{n_sym}] {instrument}: done ({len(aligned)} rows, {dt:.1f}s)",
                        file=sys.stderr,
                        flush=True,
                    )
        else:
            if progress:
                for i, instrument in enumerate(symbols):
                    print(
                        f"mtf load [{i + 1}/{n_sym}] {instrument}: fetching…",
                        file=sys.stderr,
                        flush=True,
                    )
            with ThreadPoolExecutor(max_workers=max_symbol_workers) as pool:
                future_to_symbol = {
                    pool.submit(
                        _fetch_and_align_one_symbol,
                        instrument,
                        chart_tf,
                        context_tf,
                        validation_tf,
                        fetch_from_chart,
                        fetch_from_context,
                        fetch_from_validation,
                        to_ts,
                        candle_chunk_progress=candle_chunk_progress,
                    ): instrument
                    for instrument in symbols
                }
                by_symbol: dict[str, pd.DataFrame] = {}
                done_lock = threading.Lock()
                done_count = 0
                for fut in as_completed(future_to_symbol):
                    inst = future_to_symbol[fut]
                    aligned = fut.result()
                    by_symbol[inst] = aligned
                    if progress:
                        with done_lock:
                            done_count += 1
                            k = done_count
                        print(
                            f"mtf load [{k}/{n_sym}] {inst}: done ({len(aligned)} rows)",
                            file=sys.stderr,
                            flush=True,
                        )
            for instrument in symbols:
                aligned = by_symbol[instrument]
                result[instrument] = aligned
                if not aligned.empty:
                    _write_aligned_to_db(conn, instrument, chart_tf, aligned)
    finally:
        conn.close()

    return result


if __name__ == "__main__":
    aligned = load_aligned(progress=True)
    print("Aligned data (buffer rows included; Phase B filters scan window):")
    for symbol, df in aligned.items():
        print(f"  {symbol}: {len(df)} rows, columns: {list(df.columns)}")
        if len(df) > 0:
            print(df.head(2).to_string())
            print("  ...")
            print(df.tail(2).to_string())

    # B1.0: load from DB for scan window (output to check)
    print("\n--- B1.0: Load from DB (scan window only) ---")
    try:
        from_scan = load_aligned_for_scan()
        config = load_config()
        windows = resolve_scan_windows(config)
        chart_tf = config["timeframes"]["entry"]
        print(f"Config: windows={len(windows)}, chart_tf={chart_tf}")
        for i, (w_from, w_to) in enumerate(windows):
            print(f"  window[{i+1}] {w_from} → {w_to}")
        for symbol, df in from_scan.items():
            n = len(df)
            t_min = df["time"].min() if n > 0 else None
            t_max = df["time"].max() if n > 0 else None
            print(f"  {symbol}: {n} rows, time range: {t_min} → {t_max}")
            if n > 0:
                print(df.head(2).to_string())
                print("  ...")
                print(df.tail(2).to_string())
    except Exception as e:
        print(f"  (B1.0 skip: {e})")
