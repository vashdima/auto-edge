"""
Unit tests for C1 entry_maps (build_entry_maps_for_run).
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timezone

import pytest

from entry_maps import build_entry_maps_for_run
from mtf_loader import (
    _init_aligned_candles_db,
    init_b2_tables,
)


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def test_build_entry_maps_empty_run_returns_empty_list():
    """run_key that does not exist -> empty list."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        with sqlite3.connect(db_path) as conn:
            init_b2_tables(conn)
        config = {"entry_detection": {"slowEMAPeriod": 20, "mediumEMAPeriod": 50, "fastEMAPeriod": 100}}
        result = build_entry_maps_for_run(run_key="nonexistent", db_path=db_path, config=config)
        assert result == []
    finally:
        os.unlink(db_path)


def test_build_entry_maps_one_trade_minimal_db():
    """One raw_trade with exit_time and a few aligned_candles -> one entry map with required keys and chartBuffer."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        with sqlite3.connect(db_path) as conn:
            init_b2_tables(conn)
            _init_aligned_candles_db(conn)
            cur = conn.execute(
                "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at) VALUES (?, ?, ?, ?)",
                ("test_run", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z"),
            )
            run_id = cur.lastrowid
            conn.execute(
                """INSERT INTO raw_trades (
                    run_id, symbol, chart_tf, context_tf, validation_tf,
                    setup_time, entry_time, entry_price, sl, tp, sl_size,
                    exit_reason, rr, context_bullish, validation_ok, exit_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    "USD_JPY",
                    "H1",
                    "W",
                    "D",
                    "2024-01-15T11:00:00.000000Z",
                    "2024-01-15T12:00:00.000000Z",
                    150.0,
                    149.0,
                    152.0,
                    1.0,
                    "TP",
                    2.0,
                    1,
                    1,
                    "2024-01-15T14:00:00.000000Z",
                ),
            )
            # Aligned candles: 5 bars around entry (12:00) so window [10:00, 14:00] has 5 bars (bars_before=2)
            for i, hour in enumerate([10, 11, 12, 13, 14]):
                t = datetime(2024, 1, 15, hour, 0, 0, tzinfo=timezone.utc)
                conn.execute(
                    """INSERT INTO aligned_candles (
                        symbol, chart_tf, time, open, high, low, close, volume,
                        ctx_time, ctx_open, ctx_high, ctx_low, ctx_close, ctx_volume,
                        val_time, val_open, val_high, val_low, val_close, val_volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "USD_JPY",
                        "H1",
                        _iso(t),
                        149.0 + i,
                        150.0 + i,
                        148.0 + i,
                        149.5 + i,
                        100,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        148.0, 151.0, 147.0, 150.0, 1000,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        147.0, 152.0, 146.0, 151.0, 5000,
                    ),
                )
            conn.commit()

        # Small bars_before so we only need a few bars
        config = {
            "entry_detection": {"slowEMAPeriod": 2, "mediumEMAPeriod": 2, "fastEMAPeriod": 2},
            "timeframes": {"context": "W", "validation": "D"},
        }
        result = build_entry_maps_for_run(run_key="test_run", db_path=db_path, config=config)
        assert len(result) == 1
        m = result[0]
        assert m["symbol"] == "USD_JPY"
        assert m["chartTF"] == "H1"
        assert m["contextTF"] == "W"
        assert m["validationTF"] == "D"
        assert m["run_id"] == run_id
        assert m["setupTime"] == "2024-01-15T11:00:00.000000Z"
        assert m["entryTime"] == "2024-01-15T12:00:00.000000Z"
        assert m["entryDay"] == "2024-01-15"
        assert m["entryPrice"] == 150.0
        assert m["sl"] == 149.0
        # USD_JPY pip size (from pip_metadata.json) is 0.01 => (150 - 149) / 0.01 = 100 pips
        assert m["slPips"] == pytest.approx(100.0)
        assert m["tp"] == 152.0
        assert m["rr"] == 2.0
        assert m["exitReason"] == "TP"
        assert m["state"] == "COMPLETED"
        assert m["beActive"] is False
        assert "chartBuffer" in m
        assert len(m["chartBuffer"]) >= 1
        bar = m["chartBuffer"][0]
        assert "time" in bar
        assert "open" in bar and "high" in bar and "low" in bar and "close" in bar
        # Time should be ISO-like
        assert "2024-01-15" in bar["time"]
    finally:
        os.unlink(db_path)


def test_build_entry_maps_resolve_run_id_by_run_key():
    """Passing run_key resolves to run_id and returns same as run_id."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        with sqlite3.connect(db_path) as conn:
            init_b2_tables(conn)
            _init_aligned_candles_db(conn)
            cur = conn.execute(
                "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at) VALUES (?, ?, ?, ?)",
                ("my_key", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z"),
            )
            run_id = cur.lastrowid
            conn.execute(
                """INSERT INTO raw_trades (
                    run_id, symbol, chart_tf, context_tf, validation_tf,
                    setup_time, entry_time, entry_price, sl, tp, sl_size,
                    exit_reason, rr, context_bullish, validation_ok, exit_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, "EUR_USD", "H1", "W", "D", "2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z",
                 1.1, 1.0, 1.2, 0.1, "SL", -1.0, 0, 1, "2024-01-01T12:00:00Z"),
            )
            conn.commit()
        config = {"entry_detection": {"slowEMAPeriod": 2, "mediumEMAPeriod": 2, "fastEMAPeriod": 2}}
        by_key = build_entry_maps_for_run(run_key="my_key", db_path=db_path, config=config)
        by_id = build_entry_maps_for_run(run_id=run_id, db_path=db_path, config=config)
        assert len(by_key) == 1 and len(by_id) == 1
        assert by_key[0]["symbol"] == by_id[0]["symbol"] == "EUR_USD"
    finally:
        os.unlink(db_path)


def test_build_entry_maps_chart_buffer_bar_count_consistent():
    """Bar-count buffer: each trade gets exactly bars_before bars before entry when data exists."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        with sqlite3.connect(db_path) as conn:
            init_b2_tables(conn)
            _init_aligned_candles_db(conn)
            cur = conn.execute(
                "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at) VALUES (?, ?, ?, ?)",
                ("bar_count_run", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z"),
            )
            run_id = cur.lastrowid
            # Two trades: entry at 05:00 and 08:00 UTC; we'll have bars at 01..10 so 3 bars before each entry
            for entry_hour, exit_hour in [(5, 6), (8, 9)]:
                conn.execute(
                    """INSERT INTO raw_trades (
                        run_id, symbol, chart_tf, context_tf, validation_tf,
                        setup_time, entry_time, entry_price, sl, tp, sl_size,
                        exit_reason, rr, context_bullish, validation_ok, exit_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        "EUR_USD",
                        "H1",
                        "W",
                        "D",
                        f"2024-01-15T0{entry_hour-1}:00:00.000000Z",
                        f"2024-01-15T0{entry_hour}:00:00.000000Z",
                        150.0,
                        149.0,
                        152.0,
                        1.0,
                        "TP",
                        2.0,
                        1,
                        1,
                        f"2024-01-15T0{exit_hour}:00:00.000000Z",
                    ),
                )
            # Aligned candles: 10 bars 01:00..10:00 so 3 bars before entry at 05:00 and 3 before 08:00
            for hour in range(1, 11):
                t = datetime(2024, 1, 15, hour, 0, 0, tzinfo=timezone.utc)
                conn.execute(
                    """INSERT INTO aligned_candles (
                        symbol, chart_tf, time, open, high, low, close, volume,
                        ctx_time, ctx_open, ctx_high, ctx_low, ctx_close, ctx_volume,
                        val_time, val_open, val_high, val_low, val_close, val_volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "EUR_USD",
                        "H1",
                        _iso(t),
                        149.0 + hour,
                        150.0 + hour,
                        148.0 + hour,
                        149.5 + hour,
                        100,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        148.0, 151.0, 147.0, 150.0, 1000,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        147.0, 152.0, 146.0, 151.0, 5000,
                    ),
                )
            conn.commit()

        bars_before = 3
        config = {
            "entry_detection": {"slowEMAPeriod": 3, "mediumEMAPeriod": 3, "fastEMAPeriod": 3},
            "timeframes": {"context": "W", "validation": "D"},
        }
        result = build_entry_maps_for_run(run_key="bar_count_run", db_path=db_path, config=config)
        assert len(result) == 2

        entry_times = ["2024-01-15T05:00:00.000000Z", "2024-01-15T08:00:00.000000Z"]
        for i, entry_time in enumerate(entry_times):
            m = result[i]
            chart = m["chartBuffer"]
            pre_entry = [b for b in chart if b["time"] < entry_time]
            assert len(pre_entry) == bars_before, (
                f"Trade {i+1}: expected {bars_before} bars before entry, got {len(pre_entry)}"
            )
    finally:
        os.unlink(db_path)
