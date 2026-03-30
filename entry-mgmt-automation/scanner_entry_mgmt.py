"""
Phase B entry point: B1.2–B1.5 (entry logic) and B2 (state machine + persist when implemented).

Loads aligned data from DB, runs chart/context/validation conditions and entry_setup_detected.
Ensures B2 tables (scan_runs, raw_trades) exist before any B2 write. B2.4 will insert run + trades here.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

from mtf_loader import (
    get_db_path,
    get_or_create_run_id,
    init_b2_tables,
    load_aligned_for_scan,
    load_config,
    resolve_scan_windows,
)
from scanner_entry_logic import (
    add_chart_entry_conditions,
    add_context_bullish,
    add_entry_setup_detected,
    add_validation_ok,
)
from scanner_state_machine import candidate_trades_from_df, run_trade_to_completion


def main(config_path: str | None = None) -> None:
    config = load_config(config_path)
    run_cfg = config.get("run") or {}
    run_key = run_cfg.get("key")
    if not run_key:
        print("run.key is required (Option B). Add run.key to config.yaml.")
        raise SystemExit(1)
    overwrite = run_cfg.get("overwrite", True)

    db_path = get_db_path(config, config_path)
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}. Run mtf_loader.py first, then scanner_indicators.py (B1.1).")
        raise SystemExit(1)

    # B2.1: ensure scan_runs and raw_trades exist before any B2 write
    with sqlite3.connect(db_path) as conn:
        init_b2_tables(conn)

    windows = resolve_scan_windows(config)
    chart_tf = config["timeframes"]["entry"]
    context_tf = config["timeframes"]["context"]
    validation_tf = config["timeframes"]["validation"]
    print(f"B1.2 + B1.3 + B1.4 + B1.5: chart_tf={chart_tf}, db={db_path}, windows={len(windows)}")
    all_candidates: list[dict] = []
    all_completed: list[dict] = []
    for idx, (w_from, w_to) in enumerate(windows):
        print(f"Window {idx+1}/{len(windows)}: {w_from} → {w_to}")
        data = load_aligned_for_scan(config_path, from_time=w_from, to_time=w_to)
        for symbol, df in data.items():
            if df.empty:
                print(f"  {symbol}: no rows, skip")
                continue
            required = ["close", "high", "ema_slow", "ema_medium", "ema_fast", "atr"]
            missing = [c for c in required if c not in df.columns]
            if missing:
                print(f"  {symbol}: missing columns {missing}. Run scanner_indicators.py (B1.1) first.")
                continue
            with_conditions = add_chart_entry_conditions(df, config)
            ctx_required = ["ctx_ema_slow", "ctx_ema_fast", "ctx_close"]
            if all(c in with_conditions.columns for c in ctx_required):
                with_conditions = add_context_bullish(with_conditions, config)
                n_ctx = with_conditions["context_bullish"].sum()
            else:
                n_ctx = None
            val_required = ["val_time", "val_ema_fast", "val_open", "val_high", "val_low", "val_close"]
            if all(c in with_conditions.columns for c in val_required):
                with_conditions = add_validation_ok(with_conditions, config)
                n_val = with_conditions["validation_ok"].sum()
            else:
                n_val = None
            with_conditions = add_entry_setup_detected(with_conditions, config)
            n_entry = with_conditions["entry_setup_detected"].sum()
            n_chart = with_conditions["chart_setup_detected"].sum()
            msg = f"  {symbol}: {len(with_conditions)} rows, chart_setup_detected={n_chart}"
            if n_ctx is not None:
                msg += f", context_bullish={n_ctx}"
            else:
                msg += " (no ctx columns, skip context_bullish)"
            if n_val is not None:
                msg += f", validation_ok={n_val}"
            else:
                msg += " (no val columns, skip validation_ok)"
            msg += f", entry_setup_detected={n_entry}"
            print(msg)
            cols = ["time", "close", "chart_setup_detected"]
            if "context_bullish" in with_conditions.columns:
                cols.append("context_bullish")
            if "validation_ok" in with_conditions.columns:
                cols.append("validation_ok")
            cols.append("entry_setup_detected")
            setup_rows = with_conditions.loc[with_conditions["chart_setup_detected"], cols]
            if len(setup_rows) > 0:
                print("  Sample setup bars (chart_setup_detected=True):")
                print(setup_rows.head(5).to_string())
                if len(setup_rows) > 5:
                    print("  ...")
            else:
                print("  (no chart_setup_detected bars)")
            full_setup_rows = with_conditions.loc[with_conditions["entry_setup_detected"], cols]
            n_full = len(full_setup_rows)
            if n_full > 0:
                print(f"  Sample bars (entry_setup_detected=True) [{n_full} total]:")
                print(full_setup_rows.head(5).to_string())
                if n_full > 5:
                    print("  ...")
            else:
                print("  (no bars with entry_setup_detected)")
            # B2.2: collect candidate trades for this symbol
            cands = candidate_trades_from_df(
                with_conditions, config, symbol, chart_tf, context_tf, validation_tf
            )
            all_candidates.extend(cands)
            # B2.3: run each candidate to completion (fill + exit)
            for c in cands:
                completed = run_trade_to_completion(c, with_conditions, config)
                if completed is not None:
                    all_completed.append(completed)
    # B2.2: print candidate trade count and sample
    n_cand = len(all_candidates)
    print(f"B2.2: candidate_trades={n_cand}")
    if n_cand > 0:
        sample = all_candidates[:5]
        for i, c in enumerate(sample):
            print(f"  [{i+1}] {c['setup_time']} entry={c['entry_price']:.4f} sl={c['sl']:.4f} tp={c['tp']:.4f} {c['symbol']}")
        if n_cand > 5:
            print("  ...")
    # B2.3: print completed trade count and sample
    n_done = len(all_completed)
    print(f"B2.3: completed_trades={n_done}")
    if n_done > 0:
        for i, t in enumerate(all_completed[:5]):
            rr_str = f"{t['rr']:+.2f}" if t.get("rr") is not None else "-"
            print(f"  [{i+1}] {t['setup_time']} -> {t['entry_time']} {t['exit_reason']} rr={rr_str} {t['symbol']}")
        if n_done > 5:
            print("  ...")

    # B2.4: persist run + completed trades (single run id across all windows)
    scan_from = min(w[0] for w in windows).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    scan_to = max(w[1] for w in windows).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with sqlite3.connect(db_path) as conn:
        try:
            run_id = get_or_create_run_id(
                conn, run_key, scan_from, scan_to, created_at, overwrite=overwrite
            )
        except ValueError as e:
            print(f"B2.4: {e}")
            raise SystemExit(1) from e
        for t in all_completed:
            conn.execute(
                """INSERT INTO raw_trades (
                    run_id, symbol, chart_tf, context_tf, validation_tf,
                    setup_time, entry_time, entry_price, sl, tp, sl_size,
                    exit_reason, rr, context_bullish, validation_ok, exit_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    t["symbol"],
                    t["chart_tf"],
                    t["context_tf"],
                    t["validation_tf"],
                    t["setup_time"],
                    t["entry_time"],
                    t["entry_price"],
                    t["sl"],
                    t["tp"],
                    t["sl_size"],
                    t["exit_reason"],
                    t.get("rr"),
                    1 if t.get("context_bullish") else 0,
                    1 if t.get("validation_ok") else 0,
                    t.get("exit_time"),
                ),
            )
        conn.commit()
    print(f"B2.4: run_key={run_key}, run_id={run_id}, inserted_trades={len(all_completed)}")
    print("Done.")


if __name__ == "__main__":
    main()
