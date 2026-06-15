import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.backfill import (
    fetch_historical_prices,
    format_historical_event,
    save_historical_to_postgres,
    save_historical_to_s3,
)

SAMPLE_PRICES = [[1704067200000, 42000.0], [1704153600000, 43500.0], [1704240000000, 41800.0]]

SAMPLE_EVENTS = [
    {
        "crypto_id": "bitcoin",
        "price_usd": 42000.0,
        "market_cap_usd": None,
        "change_24h_pct": None,
        "timestamp": "2024-01-01T00:00:00+00:00",
        "source": "coingecko_historical",
    },
    {
        "crypto_id": "bitcoin",
        "price_usd": 43500.0,
        "market_cap_usd": None,
        "change_24h_pct": None,
        "timestamp": "2024-01-02T00:00:00+00:00",
        "source": "coingecko_historical",
    },
]


def test_fetch_historical_prices_success():
    mock_response = MagicMock()
    mock_response.json.return_value = {"prices": SAMPLE_PRICES}
    mock_response.raise_for_status.return_value = None
    with patch("producer.backfill.requests.get", return_value=mock_response):
        result = fetch_historical_prices("bitcoin", days=3)
    assert isinstance(result, list)
    assert len(result) == 3
    assert result[0] == [1704067200000, 42000.0]


def test_fetch_historical_prices_failure():
    with patch("producer.backfill.requests.get", side_effect=Exception("Network error")):
        result = fetch_historical_prices("bitcoin", days=3)
    assert result == []


def test_format_historical_event_structure():
    event = format_historical_event("bitcoin", 1704067200000, 42000.0)
    assert event["crypto_id"] == "bitcoin"
    assert event["price_usd"] == 42000.0
    assert "timestamp" in event
    assert event["source"] == "coingecko_historical"
    assert event["market_cap_usd"] is None
    assert event["change_24h_pct"] is None


def test_format_historical_event_timestamp():
    event = format_historical_event("ethereum", 1704067200000, 2200.0)
    ts = event["timestamp"]
    assert isinstance(ts, str)
    assert "T" in ts
    assert "2024-01-01" in ts


def test_save_historical_to_postgres_success():
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1
    conn.cursor.return_value = cur
    result = save_historical_to_postgres(SAMPLE_EVENTS, conn)
    assert result == 2
    conn.commit.assert_called_once()


def test_save_historical_to_s3_success():
    mock_s3 = MagicMock()
    with patch("producer.backfill.boto3.client", return_value=mock_s3):
        result = save_historical_to_s3(SAMPLE_EVENTS, "bitcoin", "my-bucket")
    assert result == 2
    assert mock_s3.put_object.call_count == 2
