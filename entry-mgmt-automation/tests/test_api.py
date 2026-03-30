"""
C2: API tests (mandatory). Uses temp DB fixture; no real config or data/trendfinder.db.
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from api import create_app
from mtf_loader import _init_aligned_candles_db, init_b2_tables


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture
def db_empty():
    """Temp DB with scan_runs + raw_trades tables only (no rows)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        with sqlite3.connect(path) as conn:
            init_b2_tables(conn)
        yield path
    finally:
        os.unlink(path)


@pytest.fixture
def db_one_run(db_empty):
    """Temp DB with one run in scan_runs (no trades)."""
    with sqlite3.connect(db_empty) as conn:
        conn.execute(
            "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at) VALUES (?, ?, ?, ?)",
            ("test_run", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z"),
        )
        conn.commit()
    return db_empty


@pytest.fixture
def db_one_run_one_trade():
    """Temp DB with one run and one raw_trade + minimal aligned_candles for GET /entries full response."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        with sqlite3.connect(path) as conn:
            init_b2_tables(conn)
            _init_aligned_candles_db(conn)
            cur = conn.execute(
                "INSERT INTO scan_runs (run_key, scan_from, scan_to, created_at) VALUES (?, ?, ?, ?)",
                ("entries_run", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z"),
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
            for hour in [10, 11, 12, 13, 14]:
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
                        149.0,
                        150.0,
                        148.0,
                        149.5,
                        100,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        148.0, 151.0, 147.0, 150.0, 1000,
                        _iso(datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)),
                        147.0, 152.0, 146.0, 151.0, 5000,
                    ),
                )
            conn.commit()
        yield path, run_id
    finally:
        os.unlink(path)


MINIMAL_CONFIG = {
    "entry_detection": {"slowEMAPeriod": 2, "mediumEMAPeriod": 2, "fastEMAPeriod": 2},
    "timeframes": {"context": "W", "validation": "D"},
}


# --- GET /runs ---


def test_runs_returns_200_and_array(db_empty):
    """GET /runs returns 200 and a JSON array."""
    app = create_app(db_path=db_empty, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/runs")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


def test_runs_empty_returns_empty_list(db_empty):
    """GET /runs with empty scan_runs returns 200 and []."""
    app = create_app(db_path=db_empty, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/runs")
    assert r.status_code == 200
    assert r.json() == []


def test_runs_one_run_has_shape(db_one_run):
    """GET /runs with one run returns array of length 1 with required keys."""
    app = create_app(db_path=db_one_run, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/runs")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    row = data[0]
    for key in ("run_id", "run_key", "scan_from", "scan_to", "created_at"):
        assert key in row


# --- GET /entries ---


def test_entries_missing_params_returns_400(db_empty):
    """GET /entries with neither run_id nor run_key returns 400."""
    app = create_app(db_path=db_empty, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries")
    assert r.status_code == 400


def test_entries_nonexistent_run_id_returns_404(db_one_run):
    """GET /entries?run_id=999 (not in scan_runs) returns 404."""
    app = create_app(db_path=db_one_run, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries", params={"run_id": 999})
    assert r.status_code == 404


def test_entries_nonexistent_run_key_returns_404(db_one_run):
    """GET /entries?run_key=nonexistent returns 404."""
    app = create_app(db_path=db_one_run, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries", params={"run_key": "nonexistent"})
    assert r.status_code == 404


def test_entries_valid_run_no_trades_returns_200_empty(db_one_run):
    """GET /entries for valid run with no raw_trades returns 200 and []."""
    app = create_app(db_path=db_one_run, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries", params={"run_key": "test_run"})
    assert r.status_code == 200
    assert r.json() == []


def test_entries_valid_run_one_trade_returns_entry_map(db_one_run_one_trade):
    """GET /entries for valid run with one trade returns 200 and one entry map with required keys and chartBuffer."""
    path, run_id = db_one_run_one_trade
    app = create_app(db_path=path, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries", params={"run_id": run_id})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 1
    m = data[0]
    for key in ("symbol", "chartTF", "entryTime", "entryPrice", "sl", "tp", "rr", "exitReason", "chartBuffer"):
        assert key in m, f"missing key {key}"
    assert m["symbol"] == "USD_JPY"
    assert m["chartTF"] == "H1"
    assert m["entryPrice"] == 150.0
    assert m["sl"] == 149.0
    assert m["tp"] == 152.0
    assert m["rr"] == 2.0
    assert m["exitReason"] == "TP"
    assert isinstance(m["chartBuffer"], list)
    assert len(m["chartBuffer"]) >= 1
    bar = m["chartBuffer"][0]
    for key in ("time", "open", "high", "low", "close"):
        assert key in bar, f"chartBuffer bar missing key {key}"


def test_entries_summary_empty_buffers(db_one_run_one_trade):
    """GET /entries?summary=true returns metadata with empty buffers (no aligned load path for UI)."""
    path, run_id = db_one_run_one_trade
    app = create_app(db_path=path, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/entries", params={"run_id": run_id, "summary": "true"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    m = data[0]
    assert m["chartBuffer"] == []
    assert m["contextBuffer"] == []
    assert m["validationBuffer"] == []
    assert m["entryPrice"] == 150.0
    assert m["trade_id"] >= 1


def test_trade_buffers_returns_chart_data(db_one_run_one_trade):
    """GET /trade-buffers fills chart/context/validation buffers for a trade id."""
    path, run_id = db_one_run_one_trade
    app = create_app(db_path=path, config=MINIMAL_CONFIG)
    client = TestClient(app)
    summ = client.get("/entries", params={"run_id": run_id, "summary": "true"}).json()
    tid = summ[0]["trade_id"]
    r = client.get("/trade-buffers", params={"run_id": run_id, "trade_ids": str(tid)})
    assert r.status_code == 200
    patches = r.json()
    assert len(patches) == 1
    p = patches[0]
    assert p["trade_id"] == tid
    assert isinstance(p["chartBuffer"], list)
    assert len(p["chartBuffer"]) >= 1
    assert "time" in p["chartBuffer"][0]


# --- GET /run-stats ---


def test_run_stats_missing_params_returns_400(db_empty):
    """GET /run-stats with neither run_id nor run_key returns 400."""
    app = create_app(db_path=db_empty, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/run-stats")
    assert r.status_code == 400


def test_run_stats_nonexistent_run_returns_404(db_one_run):
    """GET /run-stats for missing run returns 404."""
    app = create_app(db_path=db_one_run, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/run-stats", params={"run_id": 999})
    assert r.status_code == 404


def test_run_stats_valid_run_has_expected_shape(db_one_run_one_trade):
    """GET /run-stats returns summary/drawdown/streaks/equityCurve."""
    path, run_id = db_one_run_one_trade
    app = create_app(db_path=path, config=MINIMAL_CONFIG)
    client = TestClient(app)
    r = client.get("/run-stats", params={"run_id": run_id})
    assert r.status_code == 200
    data = r.json()
    assert data["run_id"] == run_id
    for k in ("summary", "drawdown", "streaks", "equityCurve", "rrSeries", "hourlyByEntryUtc"):
        assert k in data
    summary = data["summary"]
    assert summary["totalTrades"] == 1
    assert summary["wins"] == 1
    assert summary["losses"] == 0
    assert isinstance(data["equityCurve"], list)
    assert isinstance(data["rrSeries"], list)
    assert len(data["rrSeries"]) == len(data["equityCurve"])
    assert isinstance(data["hourlyByEntryUtc"], list)
    assert len(data["hourlyByEntryUtc"]) == 24
    bucket_12 = next((r for r in data["hourlyByEntryUtc"] if r["hour"] == 12), None)
    assert bucket_12 is not None
    assert bucket_12["trades"] == 1
    assert bucket_12["wins"] == 1
    assert bucket_12["losses"] == 0
    assert bucket_12["breakevens"] == 0
    assert bucket_12["totalRR"] == 2.0
