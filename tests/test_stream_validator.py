import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.data_validator import (
    calculate_stream_quality_score,
    validate_aggregation,
    validate_price_event,
)

VALID_EVENT = {
    "crypto_id": "bitcoin",
    "price_usd": 65000.0,
    "market_cap_usd": 1280000000000.0,
    "change_24h_pct": 2.5,
    "timestamp": "2026-06-09T10:00:00+00:00",
    "source": "coingecko",
}

VALID_AGG = {
    "crypto_id": "bitcoin",
    "avg_price": 65000.0,
    "min_price": 64000.0,
    "max_price": 66000.0,
    "price_range": 2000.0,
    "record_count": 5,
    "window_start": "2026-06-09T10:00:00",
    "window_end": "2026-06-09T10:01:00",
}


def test_validate_price_event_valid():
    result = validate_price_event(VALID_EVENT)
    assert result["valid"] is True
    assert all(result["checks"].values())


def test_validate_price_event_missing_field():
    event = {**VALID_EVENT}
    del event["crypto_id"]
    result = validate_price_event(event)
    assert result["valid"] is False


def test_validate_price_event_negative_price():
    event = {**VALID_EVENT, "price_usd": -1.0}
    result = validate_price_event(event)
    assert result["valid"] is False


def test_validate_price_event_invalid_crypto():
    event = {**VALID_EVENT, "crypto_id": "invalid_coin"}
    result = validate_price_event(event)
    assert result["valid"] is False


def test_validate_aggregation_valid():
    result = validate_aggregation(VALID_AGG)
    assert result["valid"] is True


def test_validate_aggregation_invalid_range():
    agg = {**VALID_AGG, "min_price": 67000.0, "max_price": 64000.0}
    result = validate_aggregation(agg)
    assert result["valid"] is False


def test_calculate_stream_quality_score():
    valid_results = [{"valid": True}] * 8
    invalid_results = [{"valid": False}] * 2
    score = calculate_stream_quality_score(valid_results + invalid_results)
    assert score == 80.0
