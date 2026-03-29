"""
Unit tests for B2.2/B2.3 scanner_state_machine (candidate_trades_from_df, run_trade_to_completion).
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from scanner_state_machine import candidate_trades_from_df, run_trade_to_completion


def _ts(hour: int, day: int = 1) -> pd.Timestamp:
    return pd.Timestamp(datetime(2024, 1, day, hour, 0, 0, tzinfo=timezone.utc))


def test_candidate_trades_no_setup():
    """All entry_setup_detected False -> empty list."""
    df = pd.DataFrame({
        "time": [_ts(h) for h in range(5)],
        "high": [101.0, 102.0, 103.0, 102.0, 104.0],
        "close": [100.5, 101.5, 102.5, 101.0, 103.0],
        "ema_fast": [100.0] * 5,
        "atr": [0.5] * 5,
        "bars_since_breakout": [0, 1, 2, 3, 4],
        "within_breakout_window": [True] * 5,
        "entry_setup_detected": [False] * 5,
    })
    config = {"risk_management": {"atrMultiplier": 1.5, "rrTakeProfit": 4.0}}
    result = candidate_trades_from_df(df, config, "USD_JPY", "H1", "W", "D")
    assert result == []


def test_candidate_trades_one_setup():
    """One bar with entry_setup_detected True; entry_price = max high in window, sl/tp from config."""
    # Bar 0: breakout (bars_since=0), bar 1: high 102, bar 2: high 103 (new max), bar 3: entry_setup_detected
    # So run_max_high = 103 (at bar 2), setup_time = bar 2's time
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12), _ts(13)],
        "high": [101.0, 102.0, 103.0, 102.5],
        "close": [100.5, 101.5, 102.5, 102.0],
        "ema_fast": [100.0, 100.0, 100.0, 100.0],
        "atr": [0.5, 0.5, 0.5, 0.5],
        "bars_since_breakout": [0, 1, 2, 3],
        "within_breakout_window": [True, True, True, True],
        "entry_setup_detected": [False, False, False, True],
        "context_bullish": [True, True, True, True],
        "validation_ok": [True, True, True, True],
    })
    config = {"risk_management": {"atrMultiplier": 1.5, "rrTakeProfit": 4.0}}
    result = candidate_trades_from_df(df, config, "USD_JPY", "H1", "W", "D")
    assert len(result) == 1
    c = result[0]
    assert c["symbol"] == "USD_JPY" and c["chart_tf"] == "H1"
    assert c["entry_price"] == 103.0  # max high in window (bar 2)
    assert "2024-01-01T12:00:00" in c["setup_time"]
    risk = 0.5 * 1.5  # 0.75
    assert c["sl"] == pytest.approx(103.0 - 0.75)
    assert c["tp"] == pytest.approx(103.0 + 0.75 * 4.0)
    assert c["sl_size"] == pytest.approx(0.75)
    assert c["setup_bar_index"] == 3
    assert c["context_bullish"] is True and c["validation_ok"] is True


def test_candidate_trades_two_consecutive_setup_bars():
    """Two consecutive setup bars in same run -> one candidate (Pine: levels set once on first setup bar)."""
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12), _ts(13)],
        "high": [101.0, 102.0, 103.0, 103.0],
        "close": [100.5, 101.5, 102.5, 102.5],
        "ema_fast": [100.0] * 4,
        "atr": [0.5] * 4,
        "bars_since_breakout": [0, 1, 2, 3],
        "within_breakout_window": [True] * 4,
        "entry_setup_detected": [False, False, True, True],
    })
    df["context_bullish"] = True
    df["validation_ok"] = True
    config = {"risk_management": {"atrMultiplier": 1.5, "rrTakeProfit": 4.0}}
    result = candidate_trades_from_df(df, config, "EUR_USD", "M15", "W", "D")
    assert len(result) == 1
    assert result[0]["entry_price"] == 103.0
    assert result[0]["setup_bar_index"] == 2


def test_candidate_trades_missing_columns_returns_empty():
    """Missing required columns -> empty list."""
    df = pd.DataFrame({"time": [_ts(0)], "high": [101.0], "close": [100.0]})
    config = {}
    result = candidate_trades_from_df(df, config, "X", "H1", "W", "D")
    assert result == []


# --- B2.3: run_trade_to_completion ---


def _make_candidate(
    setup_bar_index: int = 0,
    entry_price: float = 103.0,
    sl: float = 102.25,
    tp: float = 106.0,
) -> dict:
    return {
        "symbol": "USD_JPY",
        "chart_tf": "H1",
        "context_tf": "W",
        "validation_tf": "D",
        "setup_time": "2024-01-01T10:00:00.000000Z",
        "entry_price": entry_price,
        "sl": sl,
        "tp": tp,
        "sl_size": 0.75,
        "context_bullish": True,
        "validation_ok": True,
        "setup_bar_index": setup_bar_index,
    }


def test_run_trade_no_fill():
    """Cancel before fill: close < ema_fast on bar after setup -> None."""
    # Setup bar 0; bar 1 has close < ema_fast so we cancel before checking bar 2
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12)],
        "high": [102.0, 103.5, 104.0],
        "low": [101.0, 102.0, 103.0],
        "close": [101.5, 102.0, 103.5],   # bar 1: close 102 < ema_fast 102.5
        "ema_fast": [102.0, 102.5, 103.0],
    })
    config = {"risk_management": {"rrBreakEven": 1.0}, "entry_detection": {"maxCandlesAfterPause": 20}}
    c = _make_candidate(setup_bar_index=0, entry_price=103.0, sl=102.25, tp=106.0)
    assert run_trade_to_completion(c, df, config) is None


def test_run_trade_fill_then_tp():
    """Fill on bar 1 (high >= entry), then bar 2 hits TP -> exit_reason TP, rr correct."""
    # Bar 0 setup; bar 1 high 103.5 >= 103 fill; bar 2 high 106.5 >= 106 TP
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12)],
        "high": [102.0, 103.5, 106.5],
        "low": [101.0, 102.5, 105.0],
        "close": [101.5, 103.0, 106.0],
        "ema_fast": [101.0, 102.0, 103.0],
    })
    config = {"risk_management": {"rrBreakEven": 1.0}, "entry_detection": {"maxCandlesAfterPause": 20}}
    c = _make_candidate(setup_bar_index=0, entry_price=103.0, sl=102.25, tp=106.0)
    result = run_trade_to_completion(c, df, config)
    assert result is not None
    assert result["entry_time"] == "2024-01-01T11:00:00.000000Z"
    assert "exit_time" in result
    assert result["exit_time"] == "2024-01-01T12:00:00.000000Z"  # bar 2 when TP hit
    assert result["exit_reason"] == "TP"
    risk = 103.0 - 102.25
    assert result["rr"] == pytest.approx((106.0 - 103.0) / risk)


def test_run_trade_fill_then_sl():
    """Fill on bar 1, then bar 2 low <= sl -> exit_reason SL, rr -1."""
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12)],
        "high": [102.0, 103.5, 103.0],
        "low": [101.0, 102.5, 102.0],   # bar 2 low 102 <= sl 102.25? No, 102 < 102.25 so yes
        "close": [101.5, 103.0, 102.2],
        "ema_fast": [101.0, 102.0, 101.5],
    })
    config = {"risk_management": {"rrBreakEven": 1.0}, "entry_detection": {"maxCandlesAfterPause": 20}}
    c = _make_candidate(setup_bar_index=0, entry_price=103.0, sl=102.25, tp=106.0)
    result = run_trade_to_completion(c, df, config)
    assert result is not None
    assert "exit_time" in result
    assert result["exit_reason"] == "SL"
    assert result["rr"] == pytest.approx(-1.0)


def test_run_trade_fill_then_be():
    """Fill, then bar triggers BE (high >= be_target), then bar with high > entry and low <= entry -> BE exit, rr 0."""
    # entry 103, sl 102.25, be_target = 103 + 0.75*1 = 103.75. Bar 2 high 104 >= 103.75 trigger BE.
    # Bar 3 high 104 > 103 (was_in_profit_after_be), low 102.5 <= 103 -> BE exit
    df = pd.DataFrame({
        "time": [_ts(10), _ts(11), _ts(12), _ts(13)],
        "high": [102.0, 103.5, 104.0, 104.0],
        "low": [101.0, 102.5, 103.0, 102.5],
        "close": [101.5, 103.0, 103.5, 103.0],
        "ema_fast": [101.0, 102.0, 102.5, 103.0],
    })
    config = {"risk_management": {"rrBreakEven": 1.0}, "entry_detection": {"maxCandlesAfterPause": 20}}
    c = _make_candidate(setup_bar_index=0, entry_price=103.0, sl=102.25, tp=106.0)
    result = run_trade_to_completion(c, df, config)
    assert result is not None
    assert "exit_time" in result
    assert result["exit_reason"] == "BE"
    assert result["rr"] == pytest.approx(0.0)
