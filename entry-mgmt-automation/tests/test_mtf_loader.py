"""
Unit and integration tests for mtf_loader (A2 alignment).
Run from entry-mgmt-automation: pytest tests/ or python -m pytest tests/
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from oanda_client import _parse_iso_time
from mtf_loader import (
    ENRICH_SCORE_COLUMN,
    GRANULARITY_DELTA,
    _align_current_bar_running_ohlc,
    _ALIGNED_COLUMNS,
    _buffer_fetch_times,
    _init_aligned_candles_db,
    _write_aligned_to_db,
    get_or_create_run_id,
    init_b2_tables,
    load_aligned_bars_before,
    load_aligned_for_scan,
    load_aligned_from_db,
    load_aligned_full_buffer,
    load_aligned,
    load_config,
    resolve_scan_windows,
    write_indicators_to_db,
)
from scanner_indicators import add_all_indicators, ALL_INDICATOR_COLUMNS

# When DB has indicator columns (after _init_aligned_candles_db), load returns base + indicators
_SCAN_RESULT_COLUMNS = _ALIGNED_COLUMNS + list(ALL_INDICATOR_COLUMNS) + [ENRICH_SCORE_COLUMN]


def test_align_current_bar_running_ohlc_synthetic():
    """Unit test: alignment assigns current ctx/val bar and running OHLC (no lookahead)."""
    # Chart: 4 bars, 1 hour apart (e.g. H1)
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    # Context: 1 bar per day (D). Bar at 2024-01-01 00:00 covers the day.
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0],
        "high": [105.0],
        "low": [98.0],
        "close": [104.0],
        "volume": [1000],
    })
    # Validation: 1 bar per week (W). Bar at 2023-12-29 or 2024-01-01 depending on alignment; use same day for simplicity.
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0],
        "high": [106.0],
        "low": [97.0],
        "close": [105.0],
        "volume": [5000],
    })

    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)

    assert list(aligned.columns) == _ALIGNED_COLUMNS
    assert len(aligned) == 4

    # All chart bars fall in same context and validation bar (2024-01-01)
    assert aligned["ctx_time"].nunique() == 1
    assert aligned["val_time"].nunique() == 1
    assert aligned["ctx_open"].iloc[0] == 99.0
    assert aligned["val_open"].iloc[0] == 98.0

    # Running high/low: ctx_high should increase (cummax of chart high)
    assert aligned["ctx_high"].iloc[0] == 100.5
    assert aligned["ctx_high"].iloc[1] == 101.8
    assert aligned["ctx_high"].iloc[2] == 102.2
    assert aligned["ctx_high"].iloc[3] == 103.5
    assert aligned["ctx_low"].iloc[0] == 99.5
    assert aligned["ctx_low"].iloc[3] == 99.5  # running min over 99.5, 100.2, 101.5, 102.5

    # ctx_close / val_close = confirmed bar close (no lookahead): previous bar close until current bar closes; only last row in period gets current close
    assert pd.isna(aligned["ctx_close"].iloc[0]) and pd.isna(aligned["ctx_close"].iloc[1]) and pd.isna(aligned["ctx_close"].iloc[2])
    assert aligned["ctx_close"].iloc[3] == 103.0
    assert pd.isna(aligned["val_close"].iloc[0]) and pd.isna(aligned["val_close"].iloc[1]) and pd.isna(aligned["val_close"].iloc[2])
    assert aligned["val_close"].iloc[3] == 103.0


def test_write_aligned_to_db():
    """Unit test: _write_aligned_to_db persists aligned rows; _init_aligned_candles_db creates table and index."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    assert len(aligned) == 4 and list(aligned.columns) == _ALIGNED_COLUMNS

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        conn2 = sqlite3.connect(db_path)
        cur = conn2.execute("SELECT COUNT(*) FROM aligned_candles")
        assert cur.fetchone()[0] == 4
        cur = conn2.execute(
            "SELECT symbol, chart_tf, time, open, close FROM aligned_candles ORDER BY time LIMIT 1"
        )
        row = cur.fetchone()
        assert row[0] == "EUR_USD" and row[1] == "M15"
        assert "2024-01-01" in row[2] and row[3] == 100.0 and row[4] == 101.0
        conn2.close()
    finally:
        os.unlink(db_path)


# --- B1.0: Load from DB ---


def test_load_aligned_from_db_returns_same_shape_and_time_range():
    """B1.0: load_aligned_from_db returns correct columns, row count, and time range."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        result = load_aligned_from_db(
            ["EUR_USD"], "M15",
            datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            db_path=db_path,
        )
        assert list(result.keys()) == ["EUR_USD"]
        df = result["EUR_USD"]
        assert list(df.columns) == _SCAN_RESULT_COLUMNS
        assert len(df) == 4
        assert df["time"].min().hour == 8 and df["time"].max().hour == 11
        assert df["open"].iloc[0] == 100.0 and df["close"].iloc[-1] == 103.0
    finally:
        os.unlink(db_path)


def test_load_aligned_from_db_filters_time_range():
    """B1.0: load_aligned_from_db respects from_time and to_time."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        result = load_aligned_from_db(
            ["EUR_USD"], "M15",
            datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            db_path=db_path,
        )
        df = result["EUR_USD"]
        assert len(df) == 2
        assert df["time"].iloc[0].hour == 9 and df["time"].iloc[1].hour == 10
    finally:
        os.unlink(db_path)


def test_load_aligned_from_db_missing_db_returns_empty():
    """B1.0: load_aligned_from_db returns empty DataFrames when DB file does not exist."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        path = f.name  # deleted, so path does not exist
    result = load_aligned_from_db(
        ["EUR_USD", "USD_JPY"], "H1",
        datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        db_path=path,
    )
    assert list(result.keys()) == ["EUR_USD", "USD_JPY"]
    assert len(result["EUR_USD"]) == 0 and len(result["USD_JPY"]) == 0
    assert list(result["EUR_USD"].columns) == _ALIGNED_COLUMNS


def test_load_aligned_bars_before_returns_last_n_bars_ascending():
    """load_aligned_bars_before returns at most n_bars rows with time < before_time, in ascending order."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11), t0.replace(hour=12)],
        "open": [100.0, 101.0, 102.0, 103.0, 104.0],
        "high": [100.5, 101.5, 102.5, 103.5, 104.5],
        "low": [99.5, 100.5, 101.5, 102.5, 103.5],
        "close": [101.0, 101.5, 102.0, 103.0, 104.0],
        "volume": [10, 20, 30, 40, 50],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "USD_JPY", "H1", aligned)
        conn.close()

        before_time = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)  # bars at 8,9,10 are before
        result = load_aligned_bars_before(["USD_JPY"], "H1", before_time, n_bars=3, db_path=db_path)
        assert list(result.keys()) == ["USD_JPY"]
        df = result["USD_JPY"]
        assert len(df) == 3
        assert (df["time"] < pd.Timestamp(before_time)).all()
        assert df["time"].is_monotonic_increasing
        assert df["time"].iloc[0].hour == 8 and df["time"].iloc[-1].hour == 10

        result2 = load_aligned_bars_before(["USD_JPY"], "H1", before_time, n_bars=10, db_path=db_path)
        assert len(result2["USD_JPY"]) == 3  # only 3 bars before 11:00

        result3 = load_aligned_bars_before(["USD_JPY"], "H1", before_time, n_bars=2, db_path=db_path)
        assert len(result3["USD_JPY"]) == 2
        # Last 2 bars before 11:00 are 9:00 and 10:00 (ascending), so last row is 10:00
        assert result3["USD_JPY"]["time"].iloc[-1].hour == 10
    finally:
        os.unlink(db_path)


def test_load_aligned_for_scan_wired_to_config():
    """B1.0: load_aligned_for_scan uses config for scan range, symbols, chart_tf, db_path."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9)],
        "open": [100.0, 101.0], "high": [100.5, 101.5], "low": [99.5, 100.5],
        "close": [101.0, 101.5], "volume": [10, 20],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "b10_test_config.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  from: "2024-01-01T08:00:00Z"
  to:   "2024-01-01T09:00:00Z"
timeframes:
  entry: "M15"
  context: "D"
  validation: "W"
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
""".strip()
        )
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        result = load_aligned_for_scan(config_path=config_path)
        assert list(result.keys()) == ["EUR_USD"]
        df = result["EUR_USD"]
        assert list(df.columns) == _SCAN_RESULT_COLUMNS
        # Scan window 08:00–09:00 inclusive → 2 rows (08:00 and 09:00)
        assert len(df) == 2
        assert df["time"].min().hour == 8 and df["time"].max().hour == 9
    finally:
        os.unlink(db_path)
        os.unlink(config_path)


def test_load_aligned_full_buffer_returns_buffer_plus_scan():
    """load_aligned_full_buffer returns full range (buffer + scan); first row time is before scan.from."""
    scan_from = "2024-01-01T12:00:00Z"
    scan_to = "2024-01-01T13:00:00Z"
    from_ts = _parse_iso_time(scan_from)
    to_ts = _parse_iso_time(scan_to)
    # Small buffer: buffer_bars=2, M15 -> fetch_from_chart = from_ts - 30min = 11:30
    chart_times = [
        from_ts - timedelta(minutes=30),
        from_ts - timedelta(minutes=15),
        from_ts,
        from_ts + timedelta(minutes=15),
        from_ts + timedelta(minutes=30),
        from_ts + timedelta(minutes=45),
        to_ts,
    ]
    chart_df = pd.DataFrame({
        "time": chart_times,
        "open": [100.0] * 7, "high": [101.0] * 7, "low": [99.0] * 7,
        "close": [100.5] * 7, "volume": [10] * 7,
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "full_buffer_test_config.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  from: "{scan_from}"
  to:   "{scan_to}"
timeframes:
  entry: "M15"
  context: "D"
  validation: "W"
entry_detection:
  slowEMAPeriod: 2
  mediumEMAPeriod: 2
  fastEMAPeriod: 2
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
""".strip()
        )
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        full = load_aligned_full_buffer(config_path=config_path)
        scan_only = load_aligned_for_scan(config_path=config_path)
        assert list(full.keys()) == ["EUR_USD"] and list(scan_only.keys()) == ["EUR_USD"]
        df_full = full["EUR_USD"]
        df_scan = scan_only["EUR_USD"]
        assert len(df_full) >= len(df_scan)
        assert df_full["time"].min() < from_ts
        assert df_full["time"].max() >= to_ts
    finally:
        os.unlink(db_path)
        if os.path.exists(config_path):
            os.unlink(config_path)


def test_buffer_row_has_ctx_ema_after_b11():
    """After B1.1 writes indicators for full buffer, a row with time before scan.from has ctx_ema_slow."""
    scan_from = "2024-01-01T12:00:00Z"
    scan_to = "2024-01-01T13:00:00Z"
    from_ts = _parse_iso_time(scan_from)
    chart_times = [
        from_ts - timedelta(minutes=30),
        from_ts - timedelta(minutes=15),
        from_ts,
        from_ts + timedelta(minutes=15),
        from_ts + timedelta(minutes=30),
    ]
    chart_df = pd.DataFrame({
        "time": chart_times,
        "open": [100.0] * 5, "high": [101.0] * 5, "low": [99.0] * 5,
        "close": [100.5] * 5, "volume": [10] * 5,
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "b11_buffer_ctx_config.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  from: "{scan_from}"
  to:   "{scan_to}"
timeframes:
  entry: "M15"
  context: "D"
  validation: "W"
entry_detection:
  slowEMAPeriod: 2
  mediumEMAPeriod: 2
  fastEMAPeriod: 2
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
""".strip()
        )
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        full = load_aligned_full_buffer(config_path=config_path)
        base = full["EUR_USD"].drop(columns=[c for c in ALL_INDICATOR_COLUMNS if c in full["EUR_USD"].columns], errors="ignore")
        with_ind = add_all_indicators(base, load_config(config_path))
        write_indicators_to_db(conn, "EUR_USD", "M15", with_ind)
        conn.close()

        conn2 = sqlite3.connect(db_path)
        cur = conn2.execute(
            "SELECT time, ctx_ema_slow FROM aligned_candles WHERE time < ? ORDER BY time LIMIT 1",
            (scan_from,),
        )
        row = cur.fetchone()
        conn2.close()
        assert row is not None
        assert row[1] is not None
    finally:
        os.unlink(db_path)
        if os.path.exists(config_path):
            os.unlink(config_path)


def test_write_indicators_to_db():
    """B1.1: write_indicators_to_db updates aligned_candles with indicator columns."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0.replace(hour=8+h) for h in range(4)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        with_indicators = add_all_indicators(aligned)
        write_indicators_to_db(conn, "EUR_USD", "M15", with_indicators)
        conn.close()

        conn2 = sqlite3.connect(db_path)
        cur = conn2.execute(
            "SELECT time, ema_slow, atr, ctx_ema_slow, val_ema_slow, val_ema_medium, val_ema_fast FROM aligned_candles ORDER BY time LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None
        assert "2024-01-01" in row[0]
        assert row[1] is not None  # ema_slow
        assert row[2] is not None  # atr
        assert row[3] is not None  # ctx_ema_slow
        assert row[4] is not None  # val_ema_slow
        assert row[5] is not None  # val_ema_medium
        assert row[6] is not None  # val_ema_fast
        conn2.close()
    finally:
        os.unlink(db_path)


# --- B2.1: Schema (scan_runs, raw_trades) ---


def test_init_b2_tables_creates_scan_runs_and_raw_trades():
    """B2.1: init_b2_tables creates scan_runs and raw_trades tables."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        conn.close()

        conn2 = sqlite3.connect(db_path)
        cur = conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('scan_runs', 'raw_trades')"
        )
        names = {row[0] for row in cur.fetchall()}
        conn2.close()
        assert names == {"scan_runs", "raw_trades"}
    finally:
        os.unlink(db_path)


def test_init_b2_tables_scan_runs_columns():
    """B2.1: scan_runs has run_id, run_key (B2.4), scan_from, scan_to, created_at."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        cur = conn.execute("PRAGMA table_info(scan_runs)")
        columns = {row[1] for row in cur.fetchall()}
        conn.close()
        assert columns >= {"run_id", "run_key", "scan_from", "scan_to", "created_at"}
    finally:
        os.unlink(db_path)


def test_init_b2_tables_raw_trades_columns():
    """B2.1: raw_trades has id, run_id, symbol, chart_tf, setup_time, entry_time, entry_price, sl, tp, exit_reason, etc."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        cur = conn.execute("PRAGMA table_info(raw_trades)")
        columns = {row[1] for row in cur.fetchall()}
        conn.close()
        expected = {
            "id", "run_id", "symbol", "chart_tf", "context_tf", "validation_tf",
            "setup_time", "entry_time", "entry_price", "sl", "tp", "sl_size",
            "exit_reason", "rr", "context_bullish", "validation_ok",
        }
        assert columns >= expected
    finally:
        os.unlink(db_path)


# --- B2.4: get_or_create_run_id (run_key overwrite) ---


def test_get_or_create_run_id_new_key_creates_run():
    """get_or_create_run_id with new run_key inserts scan_runs row and returns run_id."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        snap = "symbols:\n  - EUR_USD\n"
        run_id = get_or_create_run_id(
            conn, "my_run_v1", "2024-01-01T00:00:00Z", "2024-06-30T23:59:59Z",
            "2024-02-13T12:00:00.000000Z", overwrite=True, config_yaml=snap,
        )
        conn.close()
        assert run_id == 1
        conn2 = sqlite3.connect(db_path)
        row = conn2.execute(
            "SELECT run_id, run_key, scan_from, scan_to, config_yaml FROM scan_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        conn2.close()
        assert row[0] == 1 and row[1] == "my_run_v1"
        assert row[4] == snap
    finally:
        os.unlink(db_path)


def test_get_or_create_run_id_existing_key_overwrites_trades():
    """Same run_key with overwrite=True reuses run_id, deletes old raw_trades, updates scan_runs."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        run_id1 = get_or_create_run_id(
            conn, "same_key", "2024-01-01T00:00:00Z", "2024-06-30T23:59:59Z",
            "2024-02-13T10:00:00.000000Z", overwrite=True
        )
        conn.execute(
            "INSERT INTO raw_trades (run_id, symbol, chart_tf, context_tf, validation_tf, setup_time, entry_time, entry_price, sl, tp, sl_size, exit_reason, rr, context_bullish, validation_ok) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id1, "X", "H1", "W", "D", "2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", 100.0, 99.0, 101.0, 1.0, "TP", 2.0, 1, 1),
        )
        conn.commit()
        n_before = conn.execute("SELECT COUNT(*) FROM raw_trades WHERE run_id = ?", (run_id1,)).fetchone()[0]
        assert n_before == 1
        snap2 = "run:\n  key: v2\n"
        run_id2 = get_or_create_run_id(
            conn, "same_key", "2024-01-01T00:00:00Z", "2024-06-30T23:59:59Z",
            "2024-02-13T12:00:00.000000Z", overwrite=True, config_yaml=snap2,
        )
        conn.close()
        assert run_id2 == run_id1
        conn2 = sqlite3.connect(db_path)
        n_after = conn2.execute("SELECT COUNT(*) FROM raw_trades WHERE run_id = ?", (run_id1,)).fetchone()[0]
        row = conn2.execute(
            "SELECT created_at, config_yaml FROM scan_runs WHERE run_id = ?",
            (run_id1,),
        ).fetchone()
        conn2.close()
        assert n_after == 0
        assert row[0] == "2024-02-13T12:00:00.000000Z"
        assert row[1] == snap2
    finally:
        os.unlink(db_path)


def test_get_or_create_run_id_existing_key_overwrite_false_raises():
    """Same run_key with overwrite=False raises ValueError."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        init_b2_tables(conn)
        get_or_create_run_id(
            conn, "existing", "2024-01-01T00:00:00Z", "2024-06-30T23:59:59Z",
            "2024-02-13T10:00:00.000000Z", overwrite=True
        )
        conn.close()
        conn2 = sqlite3.connect(db_path)
        with pytest.raises(ValueError, match="run_key.*already exists"):
            get_or_create_run_id(
                conn2, "existing", "2024-01-01T00:00:00Z", "2024-06-30T23:59:59Z",
                "2024-02-13T12:00:00.000000Z", overwrite=False
            )
        conn2.close()
    finally:
        os.unlink(db_path)


def test_align_empty_chart_returns_empty_aligned():
    """Empty chart DataFrame yields empty aligned with same columns."""
    chart_df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    context_df = pd.DataFrame({"time": [], "open": [], "high": [], "low": [], "close": [], "volume": []})
    validation_df = pd.DataFrame({"time": [], "open": [], "high": [], "low": [], "close": [], "volume": []})

    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)

    assert list(aligned.columns) == _ALIGNED_COLUMNS
    assert len(aligned) == 0


def test_load_config_default_path():
    """load_config loads config.yaml from module dir."""
    config = load_config()
    assert "scan" in config
    assert "timeframes" in config
    windows = resolve_scan_windows(config)
    assert len(windows) >= 1
    assert config["timeframes"]["entry"]


def test_resolve_scan_windows_legacy_from_to():
    """resolve_scan_windows returns single window from legacy scan.from/to."""
    config = {
        "scan": {"from": "2024-01-01T00:00:00Z", "to": "2024-01-02T00:00:00Z"},
    }
    windows = resolve_scan_windows(config)
    assert len(windows) == 1
    assert windows[0][0] == _parse_iso_time("2024-01-01T00:00:00Z")
    assert windows[0][1] == _parse_iso_time("2024-01-02T00:00:00Z")


def test_resolve_scan_windows_list():
    """resolve_scan_windows supports scan.windows list."""
    config = {
        "scan": {
            "windows": [
                {"from": "2024-01-01T00:00:00Z", "to": "2024-01-02T00:00:00Z"},
                {"from": "2024-02-01T00:00:00Z", "to": "2024-02-02T00:00:00Z"},
            ]
        },
    }
    windows = resolve_scan_windows(config)
    assert len(windows) == 2
    assert windows[0][0] == _parse_iso_time("2024-01-01T00:00:00Z")
    assert windows[1][0] == _parse_iso_time("2024-02-01T00:00:00Z")


def test_resolve_scan_windows_invalid_order_raises():
    """resolve_scan_windows rejects windows where from > to."""
    config = {
        "scan": {
            "windows": [
                {"from": "2024-01-02T00:00:00Z", "to": "2024-01-01T00:00:00Z"},
            ]
        },
    }
    with pytest.raises(ValueError, match="from > to"):
        resolve_scan_windows(config)


def test_buffer_fetch_times():
    """_buffer_fetch_times returns four datetimes; fetch_from < scan.from; chart is earliest."""
    config = load_config()
    fetch_chart, fetch_ctx, fetch_val, to_ts = _buffer_fetch_times(config)
    windows = resolve_scan_windows(config)
    from_ts = min(w[0] for w in windows)
    to_expected = max(w[1] for w in windows)

    assert fetch_chart < from_ts
    assert fetch_ctx < from_ts
    assert fetch_val < from_ts
    assert fetch_chart <= fetch_ctx
    assert fetch_chart <= fetch_val
    assert to_ts == to_expected


def test_buffer_fetch_times_chart_extends_to_earliest():
    """With H1/W/D and buffer_bars=100, fetch_from_chart is the earliest of the three boundaries."""
    config = {
        "scan": {"from": "2024-01-01T00:00:00Z", "to": "2024-06-01T00:00:00Z"},
        "timeframes": {"entry": "H1", "context": "W", "validation": "D"},
        "entry_detection": {"ema100Period": 100},
    }
    fetch_chart, fetch_ctx, fetch_val, _ = _buffer_fetch_times(config)
    from_ts = _parse_iso_time(config["scan"]["from"])
    buffer_bars = 100
    delta_c = GRANULARITY_DELTA["H1"]
    delta_ctx = GRANULARITY_DELTA["W"]
    delta_val = GRANULARITY_DELTA["D"]
    earliest = min(
        from_ts - buffer_bars * delta_c,
        from_ts - buffer_bars * delta_ctx,
        from_ts - buffer_bars * delta_val,
    )
    assert fetch_chart == earliest
    # For H1/W/D, week is largest span so chart should equal context boundary
    assert fetch_chart == from_ts - buffer_bars * delta_ctx


def test_load_aligned_for_scan_with_windows_merges_results():
    """load_aligned_for_scan merges multiple configured windows and deduplicates by time."""
    t0 = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    chart_df = pd.DataFrame({
        "time": [t0, t0.replace(hour=9), t0.replace(hour=10), t0.replace(hour=11)],
        "open": [100.0, 101.0, 102.0, 103.0],
        "high": [100.5, 101.8, 102.2, 103.5],
        "low": [99.5, 100.2, 101.5, 102.5],
        "close": [101.0, 101.5, 102.0, 103.0],
        "volume": [10, 20, 30, 40],
    })
    context_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [99.0], "high": [105.0], "low": [98.0], "close": [104.0], "volume": [1000],
    })
    validation_df = pd.DataFrame({
        "time": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        "open": [98.0], "high": [106.0], "low": [97.0], "close": [105.0], "volume": [5000],
    })
    aligned = _align_current_bar_running_ohlc(chart_df, context_df, validation_df)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "b10_test_windows_config.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  windows:
    - from: "2024-01-01T08:00:00Z"
      to:   "2024-01-01T09:00:00Z"
    - from: "2024-01-01T10:00:00Z"
      to:   "2024-01-01T11:00:00Z"
timeframes:
  entry: "M15"
  context: "D"
  validation: "W"
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
""".strip()
        )
    try:
        conn = sqlite3.connect(db_path)
        _init_aligned_candles_db(conn)
        _write_aligned_to_db(conn, "EUR_USD", "M15", aligned)
        conn.close()

        result = load_aligned_for_scan(config_path=config_path)
        df = result["EUR_USD"]
        assert len(df) == 4
        assert list(df["time"].dt.hour) == [8, 9, 10, 11]
    finally:
        os.unlink(db_path)
        os.unlink(config_path)


def test_load_aligned_multi_window_fetches_once_over_global_range(monkeypatch):
    """load_aligned should fetch once per TF over global bounds, not once per window."""
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def fetch_candles(self, instrument, granularity, from_time, to_time, **kwargs):
            calls.append((instrument, granularity))
            base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            if granularity == "H1":
                times = [base + timedelta(hours=i) for i in range(6)]
            elif granularity == "D":
                times = [base]
            else:  # W
                times = [base]
            return pd.DataFrame({
                "time": times,
                "open": [100.0] * len(times),
                "high": [101.0] * len(times),
                "low": [99.0] * len(times),
                "close": [100.5] * len(times),
                "volume": [10] * len(times),
            })

    monkeypatch.setattr("mtf_loader.OandaClient", lambda: FakeClient())

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "load_aligned_multi_window_config.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  windows:
    - from: "2024-01-01T00:00:00Z"
      to:   "2024-01-01T02:00:00Z"
    - from: "2024-01-03T00:00:00Z"
      to:   "2024-01-03T02:00:00Z"
timeframes:
  entry: "H1"
  context: "W"
  validation: "D"
entry_detection:
  slowEMAPeriod: 2
  mediumEMAPeriod: 2
  fastEMAPeriod: 2
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
""".strip()
        )
    try:
        out = load_aligned(config_path=config_path, progress=False)
        assert "EUR_USD" in out
        # one symbol * three timeframes = 3 fetches total (not multiplied by windows)
        assert len(calls) == 3
        assert {(c[0], c[1]) for c in calls} == {
            ("EUR_USD", "H1"),
            ("EUR_USD", "W"),
            ("EUR_USD", "D"),
        }
    finally:
        os.unlink(db_path)
        os.unlink(config_path)


def test_load_aligned_parallel_two_symbols_six_fetches(monkeypatch):
    """With max_symbol_workers=2, two symbols each trigger 3 parallel TF fetches = 6 total."""
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def fetch_candles(self, instrument, granularity, from_time, to_time, **kwargs):
            calls.append((instrument, granularity))
            base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            if granularity == "H1":
                times = [base + timedelta(hours=i) for i in range(6)]
            elif granularity == "D":
                times = [base]
            else:
                times = [base]
            return pd.DataFrame({
                "time": times,
                "open": [100.0] * len(times),
                "high": [101.0] * len(times),
                "low": [99.0] * len(times),
                "close": [100.5] * len(times),
                "volume": [10] * len(times),
            })

    monkeypatch.setattr("mtf_loader.OandaClient", lambda: FakeClient())

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config_path = os.path.join(tempfile.gettempdir(), "load_aligned_parallel_two_symbols.yaml")
    with open(config_path, "w") as cfg_f:
        cfg_f.write(
            f"""
scan:
  from: "2024-01-01T00:00:00Z"
  to:   "2024-01-01T02:00:00Z"
timeframes:
  entry: "H1"
  context: "W"
  validation: "D"
entry_detection:
  slowEMAPeriod: 2
  mediumEMAPeriod: 2
  fastEMAPeriod: 2
mtf_fetch:
  max_symbol_workers: 2
database:
  path: "{db_path.replace(os.sep, '/')}"
symbols:
  - "EUR_USD"
  - "GBP_USD"
""".strip()
        )
    try:
        out = load_aligned(config_path=config_path, progress=False)
        assert set(out.keys()) == {"EUR_USD", "GBP_USD"}
        assert len(calls) == 6
        for sym in ("EUR_USD", "GBP_USD"):
            sym_calls = [c for c in calls if c[0] == sym]
            assert len(sym_calls) == 3
            assert {c[1] for c in sym_calls} == {"H1", "W", "D"}
    finally:
        os.unlink(db_path)
        os.unlink(config_path)


@pytest.mark.skipif(
    not os.path.exists(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")),
    reason="No .env; skip integration test",
)
def test_load_aligned_integration():
    """Integration: load_aligned returns non-empty dict, writes to SQLite, expected columns."""
    from mtf_loader import get_db_path, load_aligned

    aligned = load_aligned(progress=False)
    assert isinstance(aligned, dict)
    assert len(aligned) >= 1  # at least one symbol from config
    total_rows = 0
    for symbol, df in aligned.items():
        assert list(df.columns) == _ALIGNED_COLUMNS
        assert len(df) >= 0  # may be 0 if fetch failed
        total_rows += len(df)
        if len(df) > 0:
            assert df["time"].is_monotonic_increasing

    # Aligned data should be persisted to SQLite
    config = load_config()
    db_path = get_db_path(config)
    if total_rows > 0 and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT COUNT(*) FROM aligned_candles")
        assert cur.fetchone()[0] >= total_rows
        conn.close()
