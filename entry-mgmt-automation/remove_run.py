"""
Remove one or more runs from the database.

Deletes all raw_trades for each run, then the scan_runs rows.

Selectors (use exactly one):
  --run-id ID          Single run by numeric id (no --yes required).
  --all                Remove every run (same as --run-key '*'). Requires --yes.
                       Use this if your shell expands bare * to filenames.
  --run-key KEY        Exact match, or special cases:
                       - '*' (after strip) removes every run.
                       - If KEY contains glob metacharacters (*, ?, [), match run_key via
                         fnmatch.fnmatchcase (e.g. usd_*, name*, *_v1).
                       - Otherwise exact run_key match.

For --run-key '*' or any glob pattern (and for --all), --yes is required or the script lists matches and exits.

Shell: quote patterns so * is not expanded to filenames — e.g. --run-key '*' not --run-key *.
Or use: python remove_run.py --all --yes

Database path comes from config (default config.yaml); override with --config.

Examples:
  python remove_run.py --run-key my_exact_key
  python remove_run.py --run-key '*' --yes
  python remove_run.py --all --yes
  python remove_run.py --run-key 'usd_*' --yes
"""

from __future__ import annotations

import argparse
import fnmatch
import sqlite3

from mtf_loader import get_db_path, init_b2_tables, load_config


def _has_glob_metachar(s: str) -> bool:
    return any(c in s for c in "*?[")


def run_key_requires_yes(run_key: str) -> bool:
    """True for '*' (all runs) or any fnmatch-style pattern."""
    rk = run_key.strip()
    if rk == "*":
        return True
    return _has_glob_metachar(rk)


def resolve_run_targets(
    conn: sqlite3.Connection,
    run_id: int | None,
    run_key: str | None,
) -> list[tuple[int, str | None]]:
    """
    Return list of (run_id, run_key) rows from scan_runs to remove.
    run_key mode: '*' = all; glob if metachars; else exact SQL match.
    """
    if run_id is not None:
        cur = conn.execute(
            "SELECT run_id, run_key FROM scan_runs WHERE run_id = ?", (run_id,)
        )
        row = cur.fetchone()
        return [(row[0], row[1])] if row else []

    if run_key is None:
        return []

    rk = run_key.strip()
    if rk == "*":
        cur = conn.execute("SELECT run_id, run_key FROM scan_runs ORDER BY run_id")
        return list(cur.fetchall())

    if _has_glob_metachar(rk):
        cur = conn.execute("SELECT run_id, run_key FROM scan_runs ORDER BY run_id")
        return [
            (r[0], r[1])
            for r in cur.fetchall()
            if fnmatch.fnmatchcase(r[1] or "", rk)
        ]

    cur = conn.execute("SELECT run_id, run_key FROM scan_runs WHERE run_key = ?", (rk,))
    row = cur.fetchone()
    return [(row[0], row[1])] if row else []


def _print_preview(targets: list[tuple[int, str | None]], *, cap: int = 20) -> None:
    print(f"Would remove {len(targets)} run(s). Matches:")
    for i, (rid, rkey) in enumerate(targets[:cap]):
        key_repr = repr(rkey) if rkey is not None else "(null)"
        print(f"  run_id={rid} run_key={key_repr}")
    if len(targets) > cap:
        print(f"  ... and {len(targets) - cap} more")
    print("Re-run with --yes to confirm.")


def remove_runs(
    run_id: int | None = None,
    run_key: str | None = None,
    *,
    yes: bool = False,
    db_path: str | None = None,
    config_path: str | None = None,
) -> int:
    """
    Remove matching runs. Returns number of scan_runs rows removed.
    Raises ValueError if neither run_id nor run_key, or if glob/all pattern without yes=True.
    """
    if run_id is None and not run_key:
        raise ValueError("Provide run_id or run_key")

    config = load_config(config_path)
    path = db_path or get_db_path(config, config_path)

    conn = sqlite3.connect(path)
    init_b2_tables(conn)

    targets = resolve_run_targets(conn, run_id, run_key)
    if not targets:
        conn.close()
        return 0

    if run_key is not None and run_key_requires_yes(run_key) and not yes:
        _print_preview(targets)
        conn.close()
        raise ValueError("Confirmation required: pass yes=True for '*' or glob --run-key")

    total_trades = 0
    conn.execute("BEGIN IMMEDIATE")
    try:
        for rid, rkey in targets:
            row = conn.execute(
                "SELECT run_key, scan_from, scan_to FROM scan_runs WHERE run_id = ?", (rid,)
            ).fetchone()
            if not row:
                continue
            run_key_resolved, scan_from, scan_to = row[0], row[1], row[2]
            n_trades = conn.execute(
                "SELECT COUNT(*) FROM raw_trades WHERE run_id = ?", (rid,)
            ).fetchone()[0]
            conn.execute("DELETE FROM raw_trades WHERE run_id = ?", (rid,))
            conn.execute("DELETE FROM scan_runs WHERE run_id = ?", (rid,))
            total_trades += int(n_trades)
            key_str = f" run_key={run_key_resolved!r}" if run_key_resolved else ""
            print(
                f"Removed run_id={rid}{key_str} (scan {scan_from} → {scan_to}): "
                f"{n_trades} trades deleted."
            )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    if len(targets) > 1:
        print(f"Total: {len(targets)} run(s), {total_trades} trades deleted.")

    return len(targets)


def remove_run(
    run_id: int | None = None,
    run_key: str | None = None,
    db_path: str | None = None,
    config_path: str | None = None,
) -> bool:
    """
    Remove a single run (backward compatible). Glob or '*' requires programmatic confirm:
    use remove_runs(..., yes=True) instead.
    Returns True if a run was removed, False if run not found.
    """
    if run_key is not None and run_key_requires_yes(run_key):
        raise ValueError(
            "run_key is '*' or a glob pattern; use remove_runs(..., yes=True) or CLI with --yes"
        )
    n = remove_runs(
        run_id=run_id,
        run_key=run_key,
        yes=True,
        db_path=db_path,
        config_path=config_path,
    )
    return n > 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove run(s) from the database (raw_trades + scan_runs).",
        epilog=(
            "Shell note: unquoted * is expanded to filenames. Use --run-key '*' or --all --yes "
            "to remove all runs; quote globs e.g. --run-key 'usd_*'."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id", type=int, help="Run ID to remove (single)")
    group.add_argument(
        "--all",
        action="store_true",
        help="Remove all runs (same as --run-key '*'). Requires --yes; avoids shell * expansion.",
    )
    group.add_argument(
        "--run-key",
        type=str,
        help="Exact run_key, '*' for all runs, or glob pattern (*, ?, [). "
        "Glob and '*' require --yes (quote * in the shell).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm removal for --all, --run-key '*', or any glob --run-key",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Config YAML path (default: config.yaml)",
    )
    args = parser.parse_args()

    run_key = "*" if args.all else args.run_key

    try:
        n = remove_runs(
            run_id=args.run_id,
            run_key=run_key,
            yes=args.yes,
            config_path=args.config,
        )
    except ValueError as e:
        if str(e):
            print(e)
        raise SystemExit(1)

    if n == 0:
        print("Run not found.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
