"""
C2: HTTP API for run selector, entry maps, and run-level stats.

Exposes:
- GET /runs (list runs for selector)
- GET /entries?run_id=... or run_key=... [&summary=1]
- GET /trade-buffers?run_id=...&trade_ids=1,2,3
- GET /run-stats?run_id=... or run_key=...
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from entry_maps import (
    build_entry_maps_for_run,
    build_entry_summaries_for_run,
    build_trade_buffers_for_run,
)
from mtf_loader import get_db_path, load_config

# In-memory cache: invalidate when SQLite file mtime changes.
_entries_cache: dict[tuple, tuple[float, Any]] = {}


def _cache_get_or_set(path: str, key: tuple, factory: Callable[[], Any]) -> Any:
    mtime = os.path.getmtime(path) if os.path.isfile(path) else 0.0
    cache_key = (path, key)
    hit = _entries_cache.get(cache_key)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    data = factory()
    _entries_cache[cache_key] = (mtime, data)
    return data


def _resolve_run_id_or_404(
    conn: sqlite3.Connection,
    run_id: Optional[int],
    run_key: Optional[str],
) -> int:
    if run_id is None and not run_key:
        raise HTTPException(status_code=400, detail="Provide run_id or run_key")
    if run_id is not None:
        cur = conn.execute("SELECT run_id FROM scan_runs WHERE run_id = ?", (run_id,))
    else:
        cur = conn.execute("SELECT run_id FROM scan_runs WHERE run_key = ?", (run_key,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return int(row[0])


def _parse_utc_hour(iso_ts: Optional[str]) -> Optional[int]:
    if not iso_ts:
        return None
    normalized = str(iso_ts).strip()
    if not normalized:
        return None
    try:
        if normalized.endswith("Z"):
            dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.hour)


def create_app(
    db_path: Optional[str] = None,
    config: Optional[dict[str, Any]] = None,
) -> FastAPI:
    """
    Create FastAPI app. If db_path or config are None, load from config.yaml.
    Used for production; tests pass db_path and config explicitly.
    """
    if config is None:
        config = load_config()
    if db_path is None:
        db_path = get_db_path(config)

    app = FastAPI(title="Entry maps API", description="C2: runs and entry maps for frontend")
    app.state.db_path = db_path
    app.state.config = config

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=500)

    @app.get("/runs")
    def list_runs() -> list[dict[str, Any]]:
        """Return all runs (run_id, run_key, scan_from, scan_to, created_at), newest first."""
        path = app.state.db_path
        try:
            with sqlite3.connect(path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    """SELECT run_id, run_key, scan_from, scan_to, created_at
                       FROM scan_runs ORDER BY run_id DESC"""
                )
                rows = cur.fetchall()
                return [dict(row) for row in rows]
        except sqlite3.OperationalError:
            return []

    @app.get("/entries")
    def get_entries(
        run_id: Optional[int] = Query(None, description="Run ID"),
        run_key: Optional[str] = Query(None, description="Run key"),
        summary: bool = Query(
            False,
            description="If true, return trade metadata with empty buffers (fast); use /trade-buffers to load charts",
        ),
    ) -> list[dict[str, Any]]:
        """Return entry maps for the given run (by run_id or run_key). 404 if run not found."""
        path = app.state.db_path
        config = app.state.config
        with sqlite3.connect(path) as conn:
            resolved_run_id = _resolve_run_id_or_404(conn, run_id, run_key)
        if summary:

            def _build_summary() -> list[dict[str, Any]]:
                return build_entry_summaries_for_run(
                    run_id=resolved_run_id,
                    run_key=None,
                    db_path=path,
                    config=config,
                )

            return _cache_get_or_set(path, ("entries_summary", resolved_run_id), _build_summary)
        maps = build_entry_maps_for_run(
            run_id=resolved_run_id,
            run_key=None,
            db_path=path,
            config=config,
        )
        return maps

    @app.get("/trade-buffers")
    def get_trade_buffers(
        run_id: Optional[int] = Query(None, description="Run ID"),
        run_key: Optional[str] = Query(None, description="Run key"),
        trade_ids: str = Query(..., description="Comma-separated raw_trades.id values"),
    ) -> list[dict[str, Any]]:
        """Return chart/context/validation buffers for specific trades (lazy-load for UI)."""
        path = app.state.db_path
        config = app.state.config
        raw = trade_ids.strip()
        if not raw:
            return []
        try:
            ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError as e:
            raise HTTPException(status_code=400, detail="trade_ids must be comma-separated integers") from e
        if not ids:
            return []
        with sqlite3.connect(path) as conn:
            resolved_run_id = _resolve_run_id_or_404(conn, run_id, run_key)

        cache_key = ("trade_buffers", resolved_run_id, tuple(sorted(ids)))

        def _build() -> list[dict[str, Any]]:
            return build_trade_buffers_for_run(
                run_id=resolved_run_id,
                run_key=None,
                trade_ids=ids,
                db_path=path,
                config=config,
            )

        return _cache_get_or_set(path, cache_key, _build)

    @app.get("/run-stats")
    def get_run_stats(
        run_id: Optional[int] = Query(None, description="Run ID"),
        run_key: Optional[str] = Query(None, description="Run key"),
    ) -> dict[str, Any]:
        """Return run-level stats: summary, drawdown, streaks, and equity curve."""
        path = app.state.db_path
        with sqlite3.connect(path) as conn:
            resolved_run_id = _resolve_run_id_or_404(conn, run_id, run_key)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT id, entry_time, exit_time, rr, exit_reason
                   FROM raw_trades
                   WHERE run_id = ?
                   ORDER BY COALESCE(exit_time, entry_time), id""",
                (resolved_run_id,),
            ).fetchall()

        total_trades = len(rows)
        wins = 0
        losses = 0
        be = 0
        total_rr = 0.0

        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0

        max_winning_streak = 0
        max_losing_streak = 0
        cur_winning_streak = 0
        cur_losing_streak = 0

        equity_curve: list[dict[str, Any]] = []
        rr_series: list[float] = []
        hourly_stats: list[dict[str, Any]] = [
            {
                "hour": h,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "breakevens": 0,
                "winRate": 0.0,
                "avgRR": 0.0,
                "totalRR": 0.0,
            }
            for h in range(24)
        ]

        for idx, row in enumerate(rows, start=1):
            rr_raw = row["rr"]
            rr_val = float(rr_raw) if rr_raw is not None else 0.0
            exit_reason = row["exit_reason"] or ""

            if rr_raw is None and str(exit_reason).upper() == "BE":
                be += 1
            elif rr_val > 0:
                wins += 1
            elif rr_val < 0:
                losses += 1
            else:
                be += 1

            # Streaks: BE/flat breaks both streaks.
            if rr_val > 0:
                cur_winning_streak += 1
                cur_losing_streak = 0
            elif rr_val < 0:
                cur_losing_streak += 1
                cur_winning_streak = 0
            else:
                cur_winning_streak = 0
                cur_losing_streak = 0
            max_winning_streak = max(max_winning_streak, cur_winning_streak)
            max_losing_streak = max(max_losing_streak, cur_losing_streak)

            total_rr += rr_val
            rr_series.append(rr_val)
            equity += rr_val
            peak = max(peak, equity)
            drawdown = peak - equity
            max_drawdown = max(max_drawdown, drawdown)

            entry_hour = _parse_utc_hour(row["entry_time"])
            if entry_hour is not None:
                bucket = hourly_stats[entry_hour]
                bucket["trades"] += 1
                bucket["totalRR"] += rr_val
                if rr_raw is None and str(exit_reason).upper() == "BE":
                    bucket["breakevens"] += 1
                elif rr_val > 0:
                    bucket["wins"] += 1
                elif rr_val < 0:
                    bucket["losses"] += 1
                else:
                    bucket["breakevens"] += 1

            equity_curve.append(
                {
                    "index": idx,
                    "time": row["exit_time"] or row["entry_time"],
                    "rr": rr_val,
                    "equity": equity,
                    "drawdown": drawdown,
                    "exitReason": exit_reason,
                }
            )

        win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
        avg_rr = (total_rr / total_trades) if total_trades > 0 else 0.0
        expectancy = avg_rr
        for bucket in hourly_stats:
            trades = int(bucket["trades"])
            wins_h = int(bucket["wins"])
            total_rr_h = float(bucket["totalRR"])
            bucket["winRate"] = (wins_h / trades * 100.0) if trades > 0 else 0.0
            bucket["avgRR"] = (total_rr_h / trades) if trades > 0 else 0.0

        return {
            "run_id": resolved_run_id,
            "summary": {
                "totalTrades": total_trades,
                "wins": wins,
                "losses": losses,
                "breakevens": be,
                "winRate": win_rate,
                "totalRR": total_rr,
                "avgRR": avg_rr,
                "expectancy": expectancy,
            },
            "drawdown": {
                "maxDrawdownR": max_drawdown,
            },
            "streaks": {
                "maxWinningStreak": max_winning_streak,
                "maxLosingStreak": max_losing_streak,
            },
            "equityCurve": equity_curve,
            "rrSeries": rr_series,
            "hourlyByEntryUtc": hourly_stats,
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    config = load_config()
    port = int((config.get("server") or {}).get("port", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
