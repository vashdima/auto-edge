"""
Pip math helpers for SL pips display.

Runtime is intentionally offline-friendly: pip sizes come from a small JSON
cache (generated from OANDA instrument metadata when desired) with safe
heuristics as a fallback to avoid UI crashes.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any

import requests
from dotenv import load_dotenv


_DEFAULT_CACHE_PATH = os.path.join(os.path.dirname(__file__), "data", "pip_metadata.json")
_CACHED_METADATA: dict[str, dict[str, Any]] | None = None

_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_DIR, ".env")

_BASE_URLS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}


def load_pip_metadata(cache_path: str = _DEFAULT_CACHE_PATH) -> dict[str, dict[str, Any]]:
    """
    Load instrument pip metadata from JSON cache.

    Expected structure:
      {
        "EUR_USD": { "pipLocation": -4, "pipSize": 0.0001 },
        "USD_JPY": { "pipLocation": -2, "pipSize": 0.01 }
      }
    """
    global _CACHED_METADATA
    if _CACHED_METADATA is not None and cache_path == _DEFAULT_CACHE_PATH:
        return _CACHED_METADATA

    if not os.path.isfile(cache_path):
        data: dict[str, dict[str, Any]] = {}
        if cache_path == _DEFAULT_CACHE_PATH:
            _CACHED_METADATA = data
        return data

    with open(cache_path, "r", encoding="utf-8") as f:
        raw = json.load(f) or {}

    if not isinstance(raw, dict):
        return {}

    # Ensure values are dicts.
    meta: dict[str, dict[str, Any]] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, dict):
            meta[k] = v

    if cache_path == _DEFAULT_CACHE_PATH:
        _CACHED_METADATA = meta
    return meta


def _heuristic_pip_size(instrument: str) -> float:
    """
    Conservative fallback heuristics when metadata cache is missing.
    This is primarily to keep the UI stable for dev/tests.
    """
    inst = (instrument or "").strip().upper()
    if not inst:
        return 0.0001

    # Common FX: JPY quote pairs use 0.01 pip size.
    if inst.endswith("_JPY"):
        return 0.01

    # Common metals: treat as 0.01 pip for now (varies by broker, but better than crashing).
    if inst.startswith("XAU_") or inst.startswith("XAG_") or inst.startswith("XPT_") or inst.startswith("XPD_"):
        return 0.01

    # Default FX pip size.
    return 0.0001


def get_pip_size(instrument: str, entry_price: float | None = None) -> float:
    """
    Return pip size for an instrument.

    Prefer cached OANDA-derived metadata:
      - if `pipSize` exists use it
      - else derive from `pipLocation` as `10 ** pipLocation`

    Falls back to heuristics when the instrument is missing from the cache.
    """
    # entry_price is currently unused, but kept for future compatibility.
    _ = entry_price

    meta = load_pip_metadata()
    info = meta.get(instrument) or meta.get((instrument or "").strip().upper())

    if info:
        pip_size = info.get("pipSize")
        if isinstance(pip_size, (int, float)) and pip_size > 0:
            return float(pip_size)

        pip_location = info.get("pipLocation")
        if isinstance(pip_location, (int, float)):
            # pipLocation is typically an integer power-of-10 exponent.
            if math.isfinite(pip_location):
                v = 10 ** float(pip_location)
                if v > 0:
                    return float(v)

        # If cache exists but values are invalid, fall through to heuristics.

    return _heuristic_pip_size(instrument)


def calc_sl_pips(entry_price: float, sl_price: float, pip_size: float) -> float:
    """Return absolute SL distance in pips."""
    if pip_size is None or pip_size == 0:
        return 0.0
    risk = abs(float(entry_price) - float(sl_price))
    return risk / float(pip_size)


def _load_oanda_metadata_for_instrument(instrument: str) -> dict[str, Any]:
    """
    Fetch instrument metadata from OANDA.

    Only the `pipLocation` field is required by the app (used to derive pipSize).
    """
    # Load .env only when we are about to hit OANDA.
    load_dotenv(_ENV_PATH)

    token = os.getenv("OANDA_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("OANDA_ACCESS_TOKEN is not set (cannot refresh pip metadata)")

    env_name = (os.getenv("OANDA_ENVIRONMENT") or "practice").strip().lower()
    base_url = _BASE_URLS.get(env_name, _BASE_URLS["practice"]).rstrip("/")

    url = f"{base_url}/v3/instruments/{instrument}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=60)
    r.raise_for_status()
    data = r.json() or {}
    if "pipLocation" not in data:
        raise RuntimeError(f"Missing pipLocation in OANDA response for {instrument}")
    return data


def ensure_pip_metadata_for_symbols(
    symbols: list[str],
    *,
    cache_path: str = _DEFAULT_CACHE_PATH,
    fail_if_missing_token: bool = True,
) -> None:
    """
    Ensure pip metadata cache has entries for all requested symbols.

    If entries are missing, refresh them from OANDA and write back to `cache_path`.
    """
    global _CACHED_METADATA

    wanted = {str(s).strip().upper() for s in (symbols or []) if str(s).strip()}
    if not wanted:
        return

    meta = load_pip_metadata(cache_path=cache_path)
    missing = sorted(wanted - set(meta.keys()))
    if not missing:
        return

    # If we need to refresh but token isn't available, fail or proceed with heuristics.
    load_dotenv(_ENV_PATH)
    token = os.getenv("OANDA_ACCESS_TOKEN")
    if not token:
        if fail_if_missing_token:
            raise RuntimeError("OANDA_ACCESS_TOKEN is not set (cannot refresh missing pip metadata)")
        return

    for inst in missing:
        try:
            data = _load_oanda_metadata_for_instrument(inst)
            pip_location = data.get("pipLocation")
            # pipLocation is typically an int exponent (power-of-10).
            if not isinstance(pip_location, (int, float)):
                try:
                    pip_location = int(str(pip_location).strip())
                except ValueError:
                    continue
            pip_size = 10 ** float(pip_location)
            if pip_size <= 0:
                continue
            meta[inst] = {"pipLocation": int(pip_location), "pipSize": float(pip_size)}
        except Exception:
            # If OANDA refresh fails for an instrument, don't hard-fail the loader;
            # SL pips will fall back to heuristics via get_pip_size().
            continue

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)

    # Invalidate default in-memory cache so subsequent calls see updates.
    if cache_path == _DEFAULT_CACHE_PATH:
        _CACHED_METADATA = None

