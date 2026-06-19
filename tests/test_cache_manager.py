import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from consumer.cache_manager import (
    clear_expired_cache,
    generate_cache_key,
    get_from_cache,
    save_to_cache,
)


def test_generate_cache_key_consistent():
    key1 = generate_cache_key("get_prices", {"crypto": "bitcoin"})
    key2 = generate_cache_key("get_prices", {"crypto": "bitcoin"})
    assert key1 == key2
    assert len(key1) == 16


def test_generate_cache_key_different_params():
    key1 = generate_cache_key("get_prices", {"crypto": "bitcoin"})
    key2 = generate_cache_key("get_prices", {"crypto": "ethereum"})
    assert key1 != key2


def test_save_to_cache_success():
    mock_s3 = MagicMock()
    with patch("consumer.cache_manager.boto3.client", return_value=mock_s3):
        result = save_to_cache("abc123", {"price": 50000}, "my-bucket", ttl_seconds=60)
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_get_from_cache_hit():
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=300)
    cached_payload = json.dumps(
        {
            "cached_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "data": {"price": 50000},
        }
    ).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = cached_payload
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.cache_manager.boto3.client", return_value=mock_s3):
        result = get_from_cache("abc123", "my-bucket", ttl_seconds=300)
    assert result == {"price": 50000}


def test_get_from_cache_expired():
    now = datetime.now(timezone.utc)
    expired_at = now - timedelta(seconds=10)
    cached_payload = json.dumps(
        {
            "cached_at": now.isoformat(),
            "expires_at": expired_at.isoformat(),
            "data": {"price": 50000},
        }
    ).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = cached_payload
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.cache_manager.boto3.client", return_value=mock_s3):
        result = get_from_cache("abc123", "my-bucket", ttl_seconds=300)
    assert result is None


def test_clear_expired_cache_returns_count():
    now = datetime.now(timezone.utc)
    expired_payload = json.dumps(
        {
            "cached_at": now.isoformat(),
            "expires_at": (now - timedelta(seconds=10)).isoformat(),
            "data": {},
        }
    ).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = expired_payload
    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "cache/crypto/abc123.json"}]}
    ]
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.cache_manager.boto3.client", return_value=mock_s3):
        count = clear_expired_cache("my-bucket")
    assert count == 1
