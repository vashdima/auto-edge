"""Tests for Oanda HTTP client behavior."""

import os
from unittest.mock import MagicMock, patch

import pytest


@patch.dict(
    os.environ,
    {
        "OANDA_ACCESS_TOKEN": "test-token",
        "OANDA_ACCOUNT_ID": "test-account",
        "OANDA_ENVIRONMENT": "practice",
    },
)
@patch("oanda_client.requests.get")
@patch("oanda_client._time.sleep", return_value=None)
def test_fetch_candles_retries_429_then_succeeds(_mock_sleep, mock_get):
    from oanda_client import OandaClient

    resp_429 = MagicMock()
    resp_429.status_code = 429
    resp_429.headers = {}
    resp_429.ok = False
    resp_429.text = "rate limit"

    resp_ok = MagicMock()
    resp_ok.status_code = 200
    resp_ok.ok = True
    resp_ok.json.return_value = {"candles": []}

    mock_get.side_effect = [resp_429, resp_ok]

    client = OandaClient()
    df = client.fetch_candles(
        "EUR_USD",
        "H1",
        "2024-01-01T00:00:00Z",
        "2024-01-01T02:00:00Z",
    )
    assert mock_get.call_count == 2
    assert len(df) == 0


@patch.dict(
    os.environ,
    {
        "OANDA_ACCESS_TOKEN": "test-token",
        "OANDA_ACCOUNT_ID": "test-account",
        "OANDA_ENVIRONMENT": "practice",
    },
)
@patch("oanda_client.requests.get")
def test_fetch_candles_raises_on_persistent_error(mock_get):
    from oanda_client import OandaClient, OandaClientError

    resp = MagicMock()
    resp.status_code = 500
    resp.ok = False
    resp.text = "server error"
    mock_get.return_value = resp

    client = OandaClient()
    with pytest.raises(OandaClientError, match="500"):
        client.fetch_candles(
            "EUR_USD",
            "H1",
            "2024-01-01T00:00:00Z",
            "2024-01-01T02:00:00Z",
        )
