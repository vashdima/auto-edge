"""
Unit tests for scanner_entry_logic (B1.2).

B1.2a: breakout state — breakout_above_ema, bars_since_breakout, within_breakout_window.
B1.2b: pause pattern — pause_pattern = high < high_prev.
B1.2c: distance filter — distance_from_ema, distance_in_atr, price_not_too_far_from_ema.
B1.2d: EMAs up — ema_slow_sloping_up, ema_medium_sloping_up, ema_fast_sloping_up, all_emas_up.
B1.2e: Combine — chart_setup_detected.
B1.3: Context (HTF) — context_bullish.
B1.4: Validation — val_ema_sloping_up, val_price_above_ema, val_is_locked, validation_ok.
B1.5: Combine — entry_setup_detected.
"""

import pandas as pd
import pytest
from typing import Optional

from scanner_entry_logic import (
    add_breakout_state,
    add_chart_entry_conditions,
    add_chart_setup_detected,
    add_context_bullish,
    add_distance_filter,
    add_emas_up,
    add_entry_setup_detected,
    add_pause_pattern,
    add_validation_ok,
)


def _df(close: list[float], ema_fast: list[float]) -> pd.DataFrame:
    """Build minimal DataFrame with close and ema_fast (breakout EMA); high/low for compatibility."""
    n = len(close)
    return pd.DataFrame({
        "close": close,
        "ema_fast": ema_fast,
        "high": [c + 0.5 for c in close],
        "low": [c - 0.5 for c in close],
    })


def test_breakout_above_ema_only_on_crossover_bar():
    """breakout_above_ema is True only on the bar where close crosses above ema_fast."""
    # Bars 0-1: below; bar 2: cross above (close must be strictly > ema_fast); bars 3-4: stay above
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 104.0],
        ema_fast=[101.0, 101.0, 101.0, 101.0, 101.0],
    )
    result = add_breakout_state(df, max_candles_after_break=5)
    assert result["breakout_above_ema"].tolist() == [False, False, True, False, False]


def test_bars_since_breakout_increments_until_reset():
    """bars_since_breakout is 0 on breakout bar, then 1,2,3... until close < ema_fast."""
    # Breakout at bar 2 (close strictly > ema); bars 3,4,5,6 stay above; bar 7 below (reset)
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 104.0, 105.0, 106.0, 99.0],
        ema_fast=[101.0] * 8,
    )
    result = add_breakout_state(df, max_candles_after_break=10)
    bars = result["bars_since_breakout"]
    assert pd.isna(bars.iloc[0]) and pd.isna(bars.iloc[1])
    assert bars.iloc[2] == 0
    assert bars.iloc[3] == 1 and bars.iloc[4] == 2 and bars.iloc[5] == 3 and bars.iloc[6] == 4
    assert pd.isna(bars.iloc[7])


def test_within_breakout_window_respects_max_candles():
    """within_breakout_window is True only when bars_since_breakout <= maxCandlesAfterBreak."""
    # Breakout at bar 2 (close strictly > ema); max = 3 so window = bars 2,3,4,5 (0,1,2,3); bar 6 has bars_since=4 -> false
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 104.0, 105.0, 106.0],
        ema_fast=[101.0] * 7,
    )
    result = add_breakout_state(df, max_candles_after_break=3)
    within = result["within_breakout_window"].tolist()
    assert within == [False, False, True, True, True, True, False]  # bars 2,3,4,5 in window (0,1,2,3)


def test_reset_when_close_below_ema_fast():
    """When close < ema_fast, breakout state resets; next bar has no state until new crossover."""
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 99.0, 100.0, 102.0],  # bar 2 breakout, bar 4 below, bar 6 new breakout
        ema_fast=[101.0, 101.0, 101.0, 101.0, 101.0, 101.0, 101.0],
    )
    result = add_breakout_state(df, max_candles_after_break=5)
    assert result["breakout_above_ema"].tolist() == [False, False, True, False, False, False, True]
    assert pd.isna(result["bars_since_breakout"].iloc[0])
    assert result["bars_since_breakout"].iloc[2] == 0 and result["bars_since_breakout"].iloc[3] == 1
    assert pd.isna(result["bars_since_breakout"].iloc[4]) and pd.isna(result["bars_since_breakout"].iloc[5])
    assert result["bars_since_breakout"].iloc[6] == 0


def test_first_row_no_breakout():
    """First row has no previous bar so breakout_above_ema is False even if close > ema_fast."""
    df = _df(close=[102.0, 103.0], ema_fast=[101.0, 101.0])
    result = add_breakout_state(df, max_candles_after_break=5)
    assert result["breakout_above_ema"].iloc[0] == False  # no previous bar (pandas may return numpy bool)
    assert result["breakout_above_ema"].iloc[1] == False  # no crossover: prev was already above


def test_add_breakout_state_from_config():
    """add_breakout_state uses config entry_detection.maxCandlesAfterBreak when no kwarg."""
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 104.0, 105.0, 106.0],
        ema_fast=[101.0] * 7,
    )
    config = {"entry_detection": {"maxCandlesAfterBreak": 2}}
    result = add_breakout_state(df, config=config)
    # Window = 0,1,2 -> bars 2,3,4 true; 5,6 false
    assert result["within_breakout_window"].tolist() == [False, False, True, True, True, False, False]


def test_bars_since_breakout_not_reset_when_window_goes_false():
    """bars_since_breakout keeps growing until close < ema_fast; we do not reset when window goes false."""
    df = _df(
        close=[100.0, 100.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
        ema_fast=[101.0] * 8,
    )
    result = add_breakout_state(df, max_candles_after_break=2)
    # bars_since_breakout: 0,1,2,3,4,5,6 at indices 2..8
    assert result["bars_since_breakout"].iloc[2] == 0
    assert result["bars_since_breakout"].iloc[5] == 3
    assert result["bars_since_breakout"].iloc[7] == 5
    # within_breakout_window: only 0,1,2 -> indices 2,3,4 True; 5,6,7 False
    assert result["within_breakout_window"].tolist() == [
        False, False, True, True, True, False, False, False,
    ]


# --- B1.2b: Pause pattern ---


def test_pause_pattern_high_lower_than_prev():
    """pause_pattern is True when high < previous high."""
    df = pd.DataFrame({"high": [101.0, 100.0, 99.0]})
    result = add_pause_pattern(df)
    assert result["pause_pattern"].tolist() == [False, True, True]


def test_pause_pattern_high_higher_than_prev():
    """pause_pattern is False when high >= previous high."""
    df = pd.DataFrame({"high": [100.0, 101.0, 102.0]})
    result = add_pause_pattern(df)
    assert result["pause_pattern"].tolist() == [False, False, False]


def test_pause_pattern_first_row_false():
    """First row has no previous bar so pause_pattern is False."""
    df = pd.DataFrame({"high": [100.0]})
    result = add_pause_pattern(df)
    assert result["pause_pattern"].iloc[0] == False  # no previous bar (pandas may return numpy bool)


def test_pause_pattern_two_rows():
    """Two rows: [101, 100] -> second True; [100, 101] -> second False."""
    df_lower = pd.DataFrame({"high": [101.0, 100.0]})
    result_lower = add_pause_pattern(df_lower)
    assert result_lower["pause_pattern"].tolist() == [False, True]

    df_higher = pd.DataFrame({"high": [100.0, 101.0]})
    result_higher = add_pause_pattern(df_higher)
    assert result_higher["pause_pattern"].tolist() == [False, False]


# --- B1.2c: Distance filter ---


def _df_distance(close: list[float], ema_fast: list[float], atr: list[float]) -> pd.DataFrame:
    """Minimal DataFrame for distance filter tests (ema_fast = breakout EMA for distance)."""
    return pd.DataFrame({"close": close, "ema_fast": ema_fast, "atr": atr})


def test_distance_filter_known_values():
    """distance_from_ema, distance_in_atr, price_not_too_far_from_ema with known close/ema_fast/atr."""
    # close=103, ema_fast=100, atr=1.5 -> distance_from_ema=3, distance_in_atr=2.0 -> within 2.0
    df = _df_distance(close=[103.0], ema_fast=[100.0], atr=[1.5])
    result = add_distance_filter(df, max_distance_from_ema=2.0)
    assert result["distance_from_ema"].iloc[0] == pytest.approx(3.0)
    assert result["distance_in_atr"].iloc[0] == pytest.approx(2.0)
    assert result["price_not_too_far_from_ema"].iloc[0] == True

    # distance_in_atr=3.0 > 2.0 -> price_not_too_far_from_ema False
    df2 = _df_distance(close=[104.5], ema_fast=[100.0], atr=[1.5])
    result2 = add_distance_filter(df2, max_distance_from_ema=2.0)
    assert result2["distance_in_atr"].iloc[0] == pytest.approx(3.0)
    assert result2["price_not_too_far_from_ema"].iloc[0] == False


def test_distance_filter_from_config():
    """add_distance_filter uses config entry_detection.maxDistanceFromEMA when no kwarg."""
    df = _df_distance(close=[102.0], ema_fast=[100.0], atr=[1.0])
    config = {"entry_detection": {"maxDistanceFromEMA": 3.0}}
    result = add_distance_filter(df, config=config)
    assert result["distance_in_atr"].iloc[0] == pytest.approx(2.0)
    assert result["price_not_too_far_from_ema"].iloc[0] == True  # 2.0 <= 3.0


def test_distance_filter_zero_atr():
    """When atr is 0, distance_in_atr is NaN and price_not_too_far_from_ema is False."""
    df = _df_distance(close=[103.0], ema_fast=[100.0], atr=[0.0])
    result = add_distance_filter(df, max_distance_from_ema=2.0)
    assert pd.isna(result["distance_in_atr"].iloc[0])
    assert result["price_not_too_far_from_ema"].iloc[0] == False


def test_distance_filter_adds_columns():
    """add_distance_filter adds distance_from_ema, distance_in_atr, price_not_too_far_from_ema."""
    df = _df_distance(close=[101.0, 102.0], ema_fast=[100.0, 100.0], atr=[1.0, 1.0])
    result = add_distance_filter(df, max_distance_from_ema=5.0)
    for col in ("distance_from_ema", "distance_in_atr", "price_not_too_far_from_ema"):
        assert col in result.columns
    assert len(result) == 2
    assert result["distance_from_ema"].iloc[0] == pytest.approx(1.0)
    assert result["distance_from_ema"].iloc[1] == pytest.approx(2.0)


# --- B1.2d: EMAs up ---


def _df_emas(ema_slow: list[float], ema_medium: list[float], ema_fast: list[float]) -> pd.DataFrame:
    """Minimal DataFrame for add_emas_up tests."""
    return pd.DataFrame({
        "ema_slow": ema_slow,
        "ema_medium": ema_medium,
        "ema_fast": ema_fast,
    })


def test_emas_up_all_sloping_up():
    """When all EMAs increase bar-over-bar, all_emas_up is True (except first row)."""
    # 100, 101, 102 -> sloping up from bar 1
    df = _df_emas(
        ema_slow=[100.0, 101.0, 102.0],
        ema_medium=[99.0, 100.0, 101.0],
        ema_fast=[98.0, 99.0, 100.0],
    )
    result = add_emas_up(df)
    assert result["ema_slow_sloping_up"].tolist() == [False, True, True]
    assert result["ema_medium_sloping_up"].tolist() == [False, True, True]
    assert result["ema_fast_sloping_up"].tolist() == [False, True, True]
    assert result["all_emas_up"].tolist() == [False, True, True]


def test_emas_up_one_dips_all_emas_up_false():
    """When one EMA dips, all_emas_up is False on that bar."""
    df = _df_emas(
        ema_slow=[100.0, 101.0, 102.0],
        ema_medium=[99.0, 100.0, 99.5],  # dips at bar 2
        ema_fast=[98.0, 99.0, 100.0],
    )
    result = add_emas_up(df)
    assert result["all_emas_up"].tolist() == [False, True, False]


def test_emas_up_first_row_false():
    """First row has no previous bar so all sloping_up are False."""
    df = _df_emas(ema_slow=[102.0], ema_medium=[101.0], ema_fast=[100.0])
    result = add_emas_up(df)
    assert result["ema_slow_sloping_up"].iloc[0] == False
    assert result["all_emas_up"].iloc[0] == False  # no previous bar (pandas may return numpy bool)


def test_emas_up_adds_columns():
    """add_emas_up adds all four columns."""
    df = _df_emas(
        ema_slow=[100.0, 101.0],
        ema_medium=[100.0, 101.0],
        ema_fast=[100.0, 101.0],
    )
    result = add_emas_up(df)
    for col in ("ema_slow_sloping_up", "ema_medium_sloping_up", "ema_fast_sloping_up", "all_emas_up"):
        assert col in result.columns
    assert len(result) == 2


# --- B1.2e: Chart setup detected (combine) ---


def test_chart_setup_detected_all_true():
    """chart_setup_detected is True only when all four inputs are True (uses within_pause_window)."""
    df = pd.DataFrame({
        "within_pause_window": [False, True, True, True],
        "pause_pattern": [True, False, True, True],
        "all_emas_up": [True, True, False, True],
        "price_not_too_far_from_ema": [True, True, True, True],
    })
    result = add_chart_setup_detected(df)
    # Only row 3 has all True
    assert result["chart_setup_detected"].tolist() == [False, False, False, True]


def test_chart_setup_detected_adds_column():
    """add_chart_setup_detected adds chart_setup_detected column (uses within_pause_window)."""
    df = pd.DataFrame({
        "within_pause_window": [True, False],
        "pause_pattern": [True, False],
        "all_emas_up": [True, True],
        "price_not_too_far_from_ema": [True, True],
    })
    result = add_chart_setup_detected(df)
    assert "chart_setup_detected" in result.columns
    assert result["chart_setup_detected"].tolist() == [True, False]


def test_add_chart_entry_conditions_integration():
    """add_chart_entry_conditions runs all steps and adds chart_setup_detected; one bar can be True."""
    # Breakout uses ema_fast: need (prev_close <= prev_ema_fast) & (close > ema_fast) at bar 2.
    # Bars 0–1: close at or below ema_fast so bar 2 is crossover (close 102 > ema_fast 100).
    # Bar 4: in window (bars_since=2), pause (high < prev high), all EMAs up, distance from ema_fast <= 10.
    df = pd.DataFrame({
        "close": [99.0, 99.0, 102.0, 103.0, 104.0, 105.0],
        "ema_slow": [101.0, 101.0, 101.0, 101.5, 102.0, 102.5],  # sloping up from bar 3
        "ema_medium": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5],
        "ema_fast": [99.0, 99.5, 100.0, 100.5, 101.0, 101.5],
        "high": [101.0, 101.5, 102.5, 104.0, 103.5, 105.5],  # bar 4: 103.5 < 104.0 -> pause True
        "atr": [1.0] * 6,
    })
    result = add_chart_entry_conditions(df, config={"entry_detection": {"maxCandlesAfterBreak": 5, "maxDistanceFromEMA": 10.0}})
    assert "chart_setup_detected" in result.columns
    # Bar 4: in window (breakout at 2, bars_since=2), pause True, all EMAs up, distance (104-101)/1=3 <= 10
    assert result["chart_setup_detected"].iloc[4] == True
    assert result["chart_setup_detected"].sum() >= 1


# --- B1.3: Context (HTF) — context_bullish ---


def _df_context(
    ctx_ema_slow: list[float],
    ctx_ema_fast: list[float],
    ctx_close: list[float],
    ctx_high: Optional[list[float]] = None,
    ctx_low: Optional[list[float]] = None,
    ctx_atr: Optional[list[float]] = None,
    ctx_time: Optional[list[int]] = None,
) -> pd.DataFrame:
    """Minimal DataFrame for add_context_bullish tests."""
    n = len(ctx_close)
    if ctx_high is None:
        ctx_high = [c + 1.0 for c in ctx_close]
    if ctx_low is None:
        ctx_low = [c - 1.0 for c in ctx_close]
    if ctx_atr is None:
        ctx_atr = [1.0] * n
    if ctx_time is None:
        ctx_time = list(range(n))
    return pd.DataFrame({
        "ctx_time": ctx_time,
        "ctx_ema_slow": ctx_ema_slow,
        "ctx_ema_fast": ctx_ema_fast,
        "ctx_close": ctx_close,
        "ctx_high": ctx_high,
        "ctx_low": ctx_low,
        "ctx_atr": ctx_atr,
    })


def test_context_bullish_both_conditions_true():
    """context_bullish is True when ctx_ema_fast > ctx_ema_slow and ctx_close > ctx_ema_slow."""
    df = _df_context(
        ctx_ema_slow=[100.0, 99.0],
        ctx_ema_fast=[102.0, 101.0],
        ctx_close=[103.0, 102.0],
    )
    result = add_context_bullish(df)
    assert result["context_bullish"].tolist() == [True, True]


def test_context_bullish_ema_fast_below_slow():
    """context_bullish is False when ctx_ema_fast < ctx_ema_slow (downtrend)."""
    df = _df_context(
        ctx_ema_slow=[102.0],
        ctx_ema_fast=[100.0],
        ctx_close=[101.0],
    )
    result = add_context_bullish(df)
    assert result["context_bullish"].iloc[0] == False


def test_context_bullish_close_below_slow():
    """context_bullish is False when ctx_close <= ctx_ema_slow."""
    df = _df_context(
        ctx_ema_slow=[100.0],
        ctx_ema_fast=[102.0],
        ctx_close=[99.0],
    )
    result = add_context_bullish(df)
    assert result["context_bullish"].iloc[0] == False


def test_context_bullish_nan_returns_false():
    """When any of ctx_ema_slow, ctx_ema_fast, ctx_close is NaN, context_bullish is False."""
    df = _df_context(
        ctx_ema_slow=[100.0, float("nan"), 100.0],
        ctx_ema_fast=[102.0, 102.0, 102.0],
        ctx_close=[103.0, 103.0, float("nan")],
    )
    result = add_context_bullish(df)
    assert result["context_bullish"].iloc[0] == True
    assert result["context_bullish"].iloc[1] == False
    assert result["context_bullish"].iloc[2] == False


def test_context_bullish_adds_column():
    """add_context_bullish adds context_bullish column."""
    df = _df_context(
        ctx_ema_slow=[100.0, 102.0],
        ctx_ema_fast=[102.0, 100.0],
        ctx_close=[103.0, 98.0],
    )
    result = add_context_bullish(df)
    assert "context_bullish" in result.columns
    assert len(result) == 2
    assert result["context_bullish"].tolist() == [True, False]


def test_context_bullish_re_enabled_stateful_trigger_persists_until_reset():
    """With contextReEnabled, RE trigger on prior context bar keeps later bars valid until reset."""
    df = _df_context(
        ctx_time=[1, 2, 3, 4, 5],
        ctx_ema_slow=[100.0, 100.0, 100.0, 100.0, 100.0],
        ctx_ema_fast=[102.0, 102.0, 102.0, 102.0, 102.0],
        ctx_close=[103.0, 103.0, 103.0, 101.0, 103.0],
        ctx_high=[101.0, 104.0, 101.0, 101.0, 104.0],  # triggers at bars 2 and 5
        ctx_low=[100.0, 100.0, 100.0, 100.0, 100.0],
        ctx_atr=[2.0, 2.0, 2.0, 2.0, 2.0],  # re_ratio: 0.5, 2.0, 0.5, 0.5, 2.0
    )
    cfg = {"entry_detection": {"contextReEnabled": True, "contextReAtrMultiplier": 1.4}}
    result = add_context_bullish(df, cfg)
    assert result["ctx_re_trigger"].tolist() == [False, True, False, False, True]
    assert result["ctx_re_reset"].tolist() == [False, False, False, True, False]
    assert result["ctx_re_state_active"].tolist() == [False, True, True, False, True]
    assert result["context_bullish"].tolist() == [False, True, True, False, True]


def test_context_bullish_re_disabled_matches_base_logic():
    """With contextReEnabled false, context_bullish equals context_bullish_base."""
    df = _df_context(
        ctx_time=[1, 2],
        ctx_ema_slow=[100.0, 100.0],
        ctx_ema_fast=[102.0, 102.0],
        ctx_close=[103.0, 103.0],
        ctx_high=[101.0, 104.0],
        ctx_low=[100.0, 100.0],
        ctx_atr=[2.0, 2.0],
    )
    cfg = {"entry_detection": {"contextReEnabled": False, "contextReAtrMultiplier": 1.4}}
    result = add_context_bullish(df, cfg)
    assert result["context_bullish"].tolist() == result["context_bullish_base"].tolist()
    assert result["context_bullish"].tolist() == [True, True]


def test_context_bullish_re_atr_zero_safe():
    """ATR zero/NaN should not trigger RE and should keep state inactive."""
    df = _df_context(
        ctx_time=[1, 2],
        ctx_ema_slow=[100.0, 100.0],
        ctx_ema_fast=[102.0, 102.0],
        ctx_close=[103.0, 103.0],
        ctx_high=[104.0, 104.0],
        ctx_low=[100.0, 100.0],
        ctx_atr=[0.0, float("nan")],
    )
    cfg = {"entry_detection": {"contextReEnabled": True, "contextReAtrMultiplier": 1.4}}
    result = add_context_bullish(df, cfg)
    assert result["ctx_re_trigger"].tolist() == [False, False]
    assert result["ctx_re_state_active"].tolist() == [False, False]
    assert result["context_bullish"].tolist() == [False, False]


def test_context_re_uses_finalized_ctx_bar_not_first_row():
    """RE trigger uses finalized (last) row per ctx_time so intra-bar range growth is captured."""
    df = pd.DataFrame({
        "ctx_time": [1, 1, 2, 2],
        "ctx_ema_slow": [100.0, 100.0, 100.0, 100.0],
        "ctx_ema_fast": [102.0, 102.0, 102.0, 102.0],
        "ctx_close": [103.0, 103.0, 103.0, 103.0],
        # Same ctx_time=1: first row small range (no trigger), last row expanded range (should trigger)
        "ctx_high": [101.0, 104.0, 101.0, 101.0],
        "ctx_low": [100.0, 100.0, 100.0, 100.0],
        "ctx_atr": [2.0, 2.0, 2.0, 2.0],
    })
    cfg = {"entry_detection": {"contextReEnabled": True, "contextReAtrMultiplier": 1.4}}
    result = add_context_bullish(df, cfg)
    # Expected finalized ctx_time=1 ratio=(104-100)/2=2.0 trigger true; ctx_time=2 ratio=(101-100)/2=0.5 trigger false
    assert result["ctx_re_trigger"].tolist() == [True, True, False, False]
    assert result["ctx_re_state_active"].tolist() == [True, True, True, True]
    assert result["context_bullish"].tolist() == [True, True, True, True]


def test_context_re_reset_uses_finalized_ctx_bar_close():
    """Reset should apply when finalized ctx_close for a context bar falls below ctx_ema_fast."""
    df = pd.DataFrame({
        "ctx_time": [1, 1, 2, 2, 3, 3],
        "ctx_ema_slow": [100.0] * 6,
        "ctx_ema_fast": [102.0] * 6,
        # ctx_time=1 triggers, ctx_time=2 resets on finalized close=101, ctx_time=3 re-triggers
        "ctx_close": [103.0, 103.0, 103.0, 101.0, 103.0, 103.0],
        "ctx_high": [101.0, 104.0, 101.0, 101.0, 101.0, 104.0],
        "ctx_low": [100.0] * 6,
        "ctx_atr": [2.0] * 6,
    })
    cfg = {"entry_detection": {"contextReEnabled": True, "contextReAtrMultiplier": 1.4}}
    result = add_context_bullish(df, cfg)
    assert result["ctx_re_trigger"].tolist() == [True, True, False, False, True, True]
    assert result["ctx_re_reset"].tolist() == [False, False, True, True, False, False]
    assert result["ctx_re_state_active"].tolist() == [True, True, False, False, True, True]


# --- B1.4: Validation — val_ema_sloping_up, val_price_above_ema, val_is_locked, validation_ok ---


def _df_validation(
    val_time: list,
    val_ema_fast: list[float],
    val_open: list[float],
    val_high: list[float],
    val_low: list[float],
    val_close: list[float],
) -> pd.DataFrame:
    """Minimal DataFrame for add_validation_ok tests (uses val_ema_fast = 20-period). val_time can be same for multiple rows."""
    return pd.DataFrame({
        "val_time": val_time,
        "val_ema_fast": val_ema_fast,
        "val_open": val_open,
        "val_high": val_high,
        "val_low": val_low,
        "val_close": val_close,
    })


def test_val_ema_sloping_up():
    """val_ema_sloping_up is True when val_ema_fast > val_ema_fast_prev (previous validation bar)."""
    # Two validation bars: val_time 1 has ema 100, val_time 2 has ema 102 -> sloping up on bar 2
    df = _df_validation(
        val_time=[1, 1, 2, 2],
        val_ema_fast=[100.0, 100.0, 102.0, 102.0],
        val_open=[101.0, 101.0, 103.0, 103.0],
        val_high=[102.0, 102.0, 104.0, 104.0],
        val_low=[100.0, 100.0, 102.0, 102.0],
        val_close=[101.0, 101.0, 103.0, 103.0],
    )
    result = add_validation_ok(df)
    # First val_time has no prev -> False; second val_time 102 > 100 -> True
    assert result["val_ema_sloping_up"].tolist() == [False, False, True, True]


def test_val_ema_sloping_down_false():
    """val_ema_sloping_up is False when val_ema_fast <= val_ema_fast_prev."""
    df = _df_validation(
        val_time=[1, 2],
        val_ema_fast=[102.0, 101.0],
        val_open=[103.0, 102.0],
        val_high=[104.0, 103.0],
        val_low=[102.0, 101.0],
        val_close=[103.0, 102.0],
    )
    result = add_validation_ok(df)
    assert result["val_ema_sloping_up"].iloc[0] == False  # no prev
    assert result["val_ema_sloping_up"].iloc[1] == False  # 101 < 102


def test_val_price_above_ema():
    """val_price_above_ema is True when any of open/close/high/low > val_ema_fast."""
    df = _df_validation(
        val_time=[1],
        val_ema_fast=[100.0],
        val_open=[99.0],
        val_high=[101.0],  # high above EMA
        val_low=[98.0],
        val_close=[99.0],
    )
    result = add_validation_ok(df)
    assert result["val_price_above_ema"].iloc[0] == True

    df_below = _df_validation(
        val_time=[1],
        val_ema_fast=[100.0],
        val_open=[99.0],
        val_high=[99.5],
        val_low=[98.0],
        val_close=[99.0],
    )
    result_below = add_validation_ok(df_below)
    assert result_below["val_price_above_ema"].iloc[0] == False


def test_val_is_locked_full_candle_below():
    """val_is_locked becomes True when validation bar has val_high < val_ema_fast."""
    # Bar 1: high 102 > ema 100 -> unlocked. Bar 2: high 99 < ema 100 -> locked. Bar 3: stays locked until unlock
    df = _df_validation(
        val_time=[1, 2, 3],
        val_ema_fast=[100.0, 100.0, 100.0],
        val_open=[101.0, 99.0, 99.0],
        val_high=[102.0, 99.0, 98.0],   # bar 2: high < ema -> lock
        val_low=[100.0, 98.0, 97.0],
        val_close=[101.0, 98.5, 97.5],
    )
    result = add_validation_ok(df)
    assert result["val_is_locked"].tolist() == [False, True, True]


def test_val_is_locked_unlock_full_candle_above():
    """val_is_locked becomes False when validation bar has val_low > val_ema_fast."""
    # Bar 1: lock (high < ema). Bar 2: unlock (low > ema)
    df = _df_validation(
        val_time=[1, 2, 3],
        val_ema_fast=[100.0, 100.0, 100.0],
        val_open=[98.0, 102.0, 103.0],
        val_high=[99.0, 104.0, 105.0],
        val_low=[97.0, 101.0, 102.0],   # bar 2: low > ema -> unlock
        val_close=[98.5, 103.0, 104.0],
    )
    result = add_validation_ok(df)
    assert result["val_is_locked"].tolist() == [True, False, False]


def test_validation_ok_combination():
    """validation_ok is True only when not locked and slope up and price above EMA."""
    # All three true -> validation_ok True
    df = _df_validation(
        val_time=[1, 2],
        val_ema_fast=[100.0, 101.0],
        val_open=[101.0, 102.0],
        val_high=[102.0, 103.0],
        val_low=[100.0, 101.0],
        val_close=[101.0, 102.0],
    )
    result = add_validation_ok(df)
    assert result["validation_ok"].iloc[0] == False  # no slope (first bar)
    assert result["validation_ok"].iloc[1] == True

    # When locked (and not unlocked), validation_ok False even if slope and price would be ok
    df_stay_locked = _df_validation(
        val_time=[1, 2],
        val_ema_fast=[100.0, 101.0],
        val_open=[98.0, 100.0],
        val_high=[99.0, 100.5],   # bar 1: high < ema -> locked; bar 2: high < ema -> stay locked
        val_low=[97.0, 99.0],
        val_close=[98.0, 100.0],
    )
    result_stay = add_validation_ok(df_stay_locked)
    assert result_stay["validation_ok"].iloc[1] == False


def test_validation_ok_nan_false():
    """When val_ema_fast or val_ema_fast_prev is NaN, validation_ok is False."""
    df = _df_validation(
        val_time=[1, 2],
        val_ema_fast=[100.0, float("nan")],
        val_open=[101.0, 102.0],
        val_high=[102.0, 103.0],
        val_low=[100.0, 101.0],
        val_close=[101.0, 102.0],
    )
    result = add_validation_ok(df)
    assert result["validation_ok"].iloc[0] == False  # first bar no slope
    assert result["validation_ok"].iloc[1] == False  # NaN ema


def test_validation_ok_adds_columns():
    """add_validation_ok adds val_ema_fast_prev, val_is_locked, val_ema_sloping_up, val_price_above_ema, validation_ok."""
    df = _df_validation(
        val_time=[1, 2],
        val_ema_fast=[100.0, 101.0],
        val_open=[101.0, 102.0],
        val_high=[102.0, 103.0],
        val_low=[100.0, 101.0],
        val_close=[101.0, 102.0],
    )
    result = add_validation_ok(df)
    for col in ("val_ema_fast_prev", "val_is_locked", "val_ema_sloping_up", "val_price_above_ema", "validation_ok"):
        assert col in result.columns
    assert len(result) == 2


# --- B1.5: Combine — entry_setup_detected ---


def test_entry_setup_detected_all_true():
    """entry_setup_detected is True only when chart_setup_detected, context_bullish, and validation_ok are all True."""
    df = pd.DataFrame({
        "chart_setup_detected": [True, True, True, False, True],
        "context_bullish": [True, False, True, True, False],
        "validation_ok": [True, True, False, True, True],
    })
    result = add_entry_setup_detected(df)
    assert "entry_setup_detected" in result.columns
    # Only row 0 has all True; row 1 missing context, row 2 missing validation, row 3 missing chart, row 4 missing context
    assert result["entry_setup_detected"].tolist() == [True, False, False, False, False]


def test_entry_setup_detected_chart_only():
    """When context_bullish/validation_ok are missing, entry_setup_detected equals chart_setup_detected."""
    df = pd.DataFrame({
        "chart_setup_detected": [True, False],
    })
    result = add_entry_setup_detected(df)
    assert result["entry_setup_detected"].tolist() == [True, False]
