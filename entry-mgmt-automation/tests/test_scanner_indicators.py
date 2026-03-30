"""
Unit tests for scanner_indicators (B1.1).
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from scanner_indicators import (
    ALL_INDICATOR_COLUMNS,
    CHART_INDICATOR_COLUMNS,
    VALIDATION_INDICATOR_COLUMNS,
    add_all_indicators,
    add_chart_indicators,
    add_context_indicators,
    add_validation_indicators,
)


def test_ema_constant_series():
    """Constant close -> EMA equals that value after warm-up."""
    df = pd.DataFrame({
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10,
    }, index=range(30))
    result = add_chart_indicators(
        df,
        ema_slow_period=5,
        ema_medium_period=10,
        ema_fast_period=20,
        atr_period=14,
    )
    assert "ema_slow" in result.columns
    # By bar 5+, ema_slow should be 100 (warm-up)
    assert result["ema_slow"].iloc[-1] == pytest.approx(100.0)
    assert result["ema_medium"].iloc[-1] == pytest.approx(100.0)
    assert result["ema_fast"].iloc[-1] == pytest.approx(100.0)


def test_atr_known_series():
    """Fixed true range -> ATR converges to that value."""
    # High-low = 2 every bar; prev_close not needed for first bar, then TR = max(2, ...) = 2
    n = 20
    df = pd.DataFrame({
        "high": [102.0] * n,
        "low": [100.0] * n,
        "close": [101.0] * n,
    })
    result = add_chart_indicators(
        df,
        ema_slow_period=5,
        ema_medium_period=5,
        ema_fast_period=5,
        atr_period=5,
    )
    # After warm-up, ATR (RMA of 2) should approach 2
    assert result["atr"].iloc[-1] == pytest.approx(2.0, rel=0.01)


def test_add_chart_indicators_adds_columns():
    """add_chart_indicators adds ema_slow, ema_medium, ema_fast, atr."""
    df = pd.DataFrame({
        "time": pd.date_range("2024-01-01", periods=50, freq="h", tz="UTC"),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 1000,
    })
    result = add_chart_indicators(df, ema_slow_period=5, ema_medium_period=10, ema_fast_period=20, atr_period=14)
    for col in CHART_INDICATOR_COLUMNS:
        assert col in result.columns
    assert len(result) == 50
    # First rows can be NaN for long periods
    assert pd.notna(result["ema_slow"].iloc[-1])
    assert pd.notna(result["atr"].iloc[-1])


def test_add_chart_indicators_from_config():
    """add_chart_indicators uses config slowEMAPeriod, etc."""
    config = {
        "entry_detection": {"slowEMAPeriod": 3, "mediumEMAPeriod": 5, "fastEMAPeriod": 7},
        "risk_management": {"atrPeriod": 4},
    }
    df = pd.DataFrame({
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 100,
    }, index=range(20))
    result = add_chart_indicators(df, config=config)
    assert "ema_slow" in result.columns
    assert result["ema_slow"].iloc[-1] == pytest.approx(100.0)


def test_add_context_indicators():
    """add_context_indicators adds ctx_ema_slow, ctx_ema_fast, ctx_atr; one value per ctx_time."""
    # 6 chart bars, 2 context bars (ctx_time A and B)
    t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    df = pd.DataFrame({
        "time": [t0.replace(hour=h) for h in (8, 9, 10, 11, 12, 13)],
        "open": [100.0] * 6, "high": [101.0] * 6, "low": [99.0] * 6,
        "close": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5],
        "volume": [10] * 6,
        "ctx_time": [t0.replace(hour=0)] * 3 + [t0.replace(day=2, hour=0)] * 3,
        "ctx_open": [99.0, 99.0, 99.0, 100.0, 100.0, 100.0],
        "ctx_high": [101.0] * 6, "ctx_low": [99.0] * 6,
        "ctx_close": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5],
        "ctx_volume": [30, 30, 30, 30, 30, 30],
        "val_time": [t0] * 6,
        "val_open": [98.0] * 6, "val_high": [103.0] * 6, "val_low": [97.0] * 6,
        "val_close": [100.0] * 6, "val_volume": [60] * 6,
    })
    result = add_context_indicators(df, ema_slow_period=2, ema_fast_period=2)
    assert "ctx_ema_slow" in result.columns and "ctx_ema_fast" in result.columns
    assert "ctx_atr" in result.columns
    assert result["ctx_ema_slow"].notna().all() or result["ctx_ema_slow"].notna().any()
    assert result["ctx_atr"].notna().all() or result["ctx_atr"].notna().any()
    # Same ctx_time -> same ctx_ema_* for first 3 rows
    assert result["ctx_ema_slow"].iloc[0] == result["ctx_ema_slow"].iloc[1]


def test_add_validation_indicators():
    """add_validation_indicators adds val_ema_slow, val_ema_medium, val_ema_fast, and val_atr."""
    t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    df = pd.DataFrame({
        "time": [t0.replace(hour=h) for h in range(5)],
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10,
        "ctx_time": [t0] * 5, "ctx_open": 99.0, "ctx_high": 101.0, "ctx_low": 99.0,
        "ctx_close": 100.0, "ctx_volume": 50,
        "val_time": [t0] * 5,
        "val_open": 98.0, "val_high": 102.0, "val_low": 97.0, "val_close": 100.0, "val_volume": 50,
    })
    result = add_validation_indicators(
        df,
        ema_slow_period=2,
        ema_medium_period=2,
        ema_fast_period=3,
        atr_period=5,
    )
    for col in VALIDATION_INDICATOR_COLUMNS:
        assert col in result.columns
    assert result["val_ema_slow"].notna().iloc[-1]
    assert result["val_ema_medium"].notna().iloc[-1]
    assert result["val_ema_fast"].notna().iloc[-1]
    assert result["val_atr"].notna().iloc[-1]


def test_add_all_indicators():
    """add_all_indicators adds all chart, context, and validation indicator columns."""
    t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    times = [t0 + timedelta(hours=h) for h in range(25)]
    df = pd.DataFrame({
        "time": times,
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10,
        "ctx_time": [t0 + timedelta(days=h // 24) for h in range(25)],
        "ctx_open": 99.0, "ctx_high": 101.0, "ctx_low": 99.0, "ctx_close": 100.0, "ctx_volume": 100,
        "val_time": [t0] * 25,
        "val_open": 98.0, "val_high": 102.0, "val_low": 97.0, "val_close": 100.0, "val_volume": 250,
    })
    result = add_all_indicators(df)
    for col in ALL_INDICATOR_COLUMNS:
        assert col in result.columns, f"missing {col}"
    assert len(result) == 25
