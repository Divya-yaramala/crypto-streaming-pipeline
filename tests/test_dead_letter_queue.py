import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.dead_letter_queue import (
    get_dlq_events,
    run_dlq_replay,
    send_to_dlq,
)

SAMPLE_EVENT = {
    "crypto_id": "bitcoin",
    "price_usd": 65000.0,
    "market_cap_usd": 1_200_000_000.0,
    "change_24h_pct": 2.5,
    "timestamp": "2026-01-01T00:00:00",
    "source": "coingecko",
}


def test_send_to_dlq_success():
    mock_s3 = MagicMock()
    with patch("consumer.dead_letter_queue.boto3.client", return_value=mock_s3):
        result = send_to_dlq(SAMPLE_EVENT, "test error", "validate", "my-bucket")
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_send_to_dlq_failure():
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("S3 unavailable")
    with patch("consumer.dead_letter_queue.boto3.client", return_value=mock_s3):
        result = send_to_dlq(SAMPLE_EVENT, "test error", "validate", "my-bucket")
    assert result is False


def test_send_to_dlq_correct_path():
    mock_s3 = MagicMock()
    with patch("consumer.dead_letter_queue.boto3.client", return_value=mock_s3):
        send_to_dlq(SAMPLE_EVENT, "test error", "postgres", "my-bucket")
    call_kwargs = mock_s3.put_object.call_args[1]
    key = call_kwargs["Key"]
    assert key.startswith("errors/crypto/")
    assert "/postgres/" in key
    assert "bitcoin" in key


def test_get_dlq_events_success():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "errors/crypto/2026/01/01/validate/bitcoin_2026.json"}]
    }
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"event": {}, "error": "bad", "step": "validate"}'
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.dead_letter_queue.boto3.client", return_value=mock_s3):
        events = get_dlq_events("my-bucket", "2026/01/01")
    assert isinstance(events, list)
    assert len(events) == 1


def test_get_dlq_events_empty():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    with patch("consumer.dead_letter_queue.boto3.client", return_value=mock_s3):
        events = get_dlq_events("my-bucket", "2026/01/01")
    assert events == []


def test_run_dlq_replay_summary():
    dlq_items = [
        {"event": SAMPLE_EVENT, "error": "bad", "step": "validate"},
        {"event": SAMPLE_EVENT, "error": "fail", "step": "s3"},
    ]
    with patch("consumer.dead_letter_queue.get_dlq_events", return_value=dlq_items):
        with patch("consumer.dead_letter_queue.replay_dlq_event", side_effect=[True, False]):
            result = run_dlq_replay("my-bucket", "2026/01/01")
    assert "total" in result
    assert "replayed" in result
    assert "still_failing" in result
    assert result["total"] == 2
    assert result["replayed"] == 1
    assert result["still_failing"] == 1
