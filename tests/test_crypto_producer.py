import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import MagicMock, patch

import requests

from producer.crypto_producer import fetch_crypto_prices, publish_price_event

MOCK_PRICE_RESPONSE = {
    "bitcoin": {
        "usd": 65000.0,
        "usd_market_cap": 1280000000000.0,
        "usd_24h_change": 2.5,
    },
    "ethereum": {
        "usd": 3500.0,
        "usd_market_cap": 420000000000.0,
        "usd_24h_change": -1.2,
    },
}


def test_fetch_crypto_prices_success():
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_PRICE_RESPONSE
    mock_response.raise_for_status.return_value = None

    with patch("producer.crypto_producer.requests.get", return_value=mock_response):
        result = fetch_crypto_prices(["bitcoin", "ethereum"])

    assert isinstance(result, dict)
    assert "bitcoin" in result
    assert result["bitcoin"]["usd"] == 65000.0


def test_fetch_crypto_prices_api_failure():
    with patch(
        "producer.crypto_producer.requests.get",
        side_effect=ConnectionError("Network unreachable"),
    ):
        result = fetch_crypto_prices(["bitcoin"])

    assert result == {}


def test_publish_price_event_success():
    mock_producer = MagicMock()
    mock_producer.send.return_value = MagicMock()

    price_data = {
        "usd": 65000.0,
        "usd_market_cap": 1280000000000.0,
        "usd_24h_change": 2.5,
    }
    result = publish_price_event(mock_producer, "bitcoin", price_data)

    assert result is True
    mock_producer.send.assert_called_once()
    mock_producer.flush.assert_called_once()


def test_publish_price_event_failure():
    from kafka.errors import KafkaError

    mock_producer = MagicMock()
    mock_producer.send.side_effect = KafkaError("Broker unavailable")

    price_data = {
        "usd": 65000.0,
        "usd_market_cap": 1280000000000.0,
        "usd_24h_change": 2.5,
    }
    result = publish_price_event(mock_producer, "bitcoin", price_data)

    assert result is False


def test_create_event_format():
    mock_producer = MagicMock()
    mock_producer.send.return_value = MagicMock()

    price_data = {
        "usd": 65000.0,
        "usd_market_cap": 1280000000000.0,
        "usd_24h_change": 2.5,
    }
    publish_price_event(mock_producer, "bitcoin", price_data)

    call_kwargs = mock_producer.send.call_args
    event = call_kwargs[1]["value"] if call_kwargs[1] else call_kwargs[0][1]

    required_keys = {
        "crypto_id",
        "price_usd",
        "market_cap_usd",
        "change_24h_pct",
        "timestamp",
        "source",
    }
    assert required_keys.issubset(event.keys())
    assert event["crypto_id"] == "bitcoin"
    assert event["source"] == "coingecko"


def test_fetch_crypto_prices_timeout():
    with patch(
        "producer.crypto_producer.requests.get",
        side_effect=requests.exceptions.Timeout("Request timed out"),
    ):
        result = fetch_crypto_prices(["bitcoin"])

    assert result == {}


def test_fetch_crypto_prices_empty_response():
    mock_response = MagicMock()
    mock_response.json.return_value = {}
    mock_response.raise_for_status.return_value = None

    with patch("producer.crypto_producer.requests.get", return_value=mock_response):
        result = fetch_crypto_prices(["bitcoin", "ethereum"])

    assert result == {}


def test_publish_price_event_kafka_timeout():
    from kafka.errors import KafkaTimeoutError

    mock_producer = MagicMock()
    mock_producer.send.side_effect = KafkaTimeoutError("Producer send timed out")

    price_data = {
        "usd": 65000.0,
        "usd_market_cap": 1280000000000.0,
        "usd_24h_change": 2.5,
    }
    result = publish_price_event(mock_producer, "bitcoin", price_data)

    assert result is False
