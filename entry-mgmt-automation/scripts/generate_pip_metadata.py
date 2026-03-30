"""
Generate/update `data/pip_metadata.json` using OANDA instrument metadata.

This script is optional for runtime: the app uses the JSON cache
offline-friendly. Running this script refreshes pip sizes for your
instruments so SL-pips display remains accurate.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv

_DIR = os.path.dirname(os.path.abspath(__file__))
_ENTRY_MGMT_DIR = os.path.dirname(_DIR)

# Ensure sibling imports like `mtf_loader.py` work regardless of cwd.
if _ENTRY_MGMT_DIR not in sys.path:
    sys.path.insert(0, _ENTRY_MGMT_DIR)

from mtf_loader import load_config, resolve_config_path

_DEFAULT_OUT = os.path.join(_ENTRY_MGMT_DIR, "data", "pip_metadata.json")
_ENV_PATH = os.path.join(_ENTRY_MGMT_DIR, ".env")


def _get_base_url(env_name: str) -> str:
    env = (env_name or "").strip().lower()
    if env == "live":
        return "https://api-fxtrade.oanda.com"
    # Default to practice.
    return "https://api-fxpractice.oanda.com"


def _fetch_instrument(instrument: str, *, token: str, base_url: str) -> dict[str, Any]:
    url = f"{base_url}/v3/instruments/{instrument}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=60)
    r.raise_for_status()
    data = r.json() or {}
    # OANDA returns pipLocation at top-level for v3 instruments.
    if "pipLocation" not in data:
        # Some responses wrap or omit fields; keep error explicit.
        raise RuntimeError(f"Missing pipLocation in OANDA response for {instrument}")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OANDA pip metadata cache")
    parser.add_argument(
        "--instruments",
        type=str,
        default="",
        help='Comma-separated list (e.g. "EUR_USD,USD_JPY,XAU_USD"). If empty, reads `symbols:` from config.',
    )
    parser.add_argument("--config", type=str, default=None, help="Config YAML path (defaults to entry-mgmt-automation/config.yaml)")
    parser.add_argument("--out", type=str, default=_DEFAULT_OUT, help="Output JSON path")
    args = parser.parse_args()

    load_dotenv(_ENV_PATH)

    token = os.getenv("OANDA_ACCESS_TOKEN")
    env_name = os.getenv("OANDA_ENVIRONMENT") or "practice"
    if not token:
        raise SystemExit("OANDA_ACCESS_TOKEN is not set in entry-mgmt-automation/.env")

    base_url = _get_base_url(env_name)

    instruments: list[str]
    if args.instruments.strip():
        instruments = [x.strip().upper() for x in args.instruments.split(",") if x.strip()]
    else:
        cfg_path = resolve_config_path(args.config)
        cfg = load_config(cfg_path)
        instruments = [x.strip().upper() for x in (cfg.get("symbols") or []) if isinstance(x, str) and x.strip()]
        if not instruments:
            raise SystemExit("No instruments provided and `symbols:` missing/empty in config.yaml")

    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    meta: dict[str, dict[str, Any]] = {}
    for inst in instruments:
        data = _fetch_instrument(inst, token=token, base_url=base_url)
        pip_location = data.get("pipLocation")
        if pip_location is None:
            continue
        if not isinstance(pip_location, (int, float)):
            # Try to parse string exponent.
            try:
                pip_location = int(str(pip_location).strip())
            except ValueError:
                continue
        pip_size = 10 ** float(pip_location)
        meta[inst] = {"pipLocation": int(pip_location), "pipSize": float(pip_size)}

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)

    print(f"Wrote {len(meta)} instrument pip entries to {out_path}")


if __name__ == "__main__":
    main()

