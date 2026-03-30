import json
import tempfile
from typing import Any

import pytest

import pip_math


class _FakeResp:
    def __init__(self, data: dict[str, Any]):
        self._data = data
        self.status_code = 200

    def raise_for_status(self) -> None:
        return

    def json(self) -> dict[str, Any]:
        return self._data


def test_ensure_pip_metadata_refreshes_missing_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    """If pip cache is missing a symbol, ensure_pip_metadata_for_symbols refreshes via OANDA (mocked)."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        cache_path = f.name

    try:
        # Start with only EUR_USD in the cache.
        with open(cache_path, "w", encoding="utf-8") as out:
            json.dump({"EUR_USD": {"pipLocation": -4, "pipSize": 0.0001}}, out)

        # Prevent dotenv from overwriting our test token.
        monkeypatch.setattr(pip_math, "load_dotenv", lambda *args, **kwargs: None)
        monkeypatch.setenv("OANDA_ACCESS_TOKEN", "test-token")
        monkeypatch.setenv("OANDA_ENVIRONMENT", "practice")

        def _fake_get(url: str, headers: dict[str, str], timeout: int) -> _FakeResp:  # noqa: ARG001
            # Extract instrument from ".../v3/instruments/{instrument}".
            inst = url.rsplit("/", 1)[-1].strip().upper()
            if inst == "GBP_USD":
                # 1 pip = 0.0001 for pipLocation -4.
                return _FakeResp({"pipLocation": -4})
            raise AssertionError(f"Unexpected instrument in test: {inst}")

        monkeypatch.setattr(pip_math.requests, "get", _fake_get)

        pip_math.ensure_pip_metadata_for_symbols(
            ["GBP_USD"],
            cache_path=cache_path,
            fail_if_missing_token=True,
        )

        with open(cache_path, "r", encoding="utf-8") as inp:
            refreshed = json.load(inp)

        assert "GBP_USD" in refreshed
        assert refreshed["GBP_USD"]["pipSize"] == pytest.approx(0.0001)
    finally:
        # NamedTemporaryFile creates an already-closed fd; remove the file explicitly.
        import os

        if os.path.exists(cache_path):
            os.unlink(cache_path)

