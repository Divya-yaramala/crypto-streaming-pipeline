import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from consumer.alert_aggregator import (
    aggregate_alerts_by_hour,
    find_alert_patterns,
    generate_alert_digest,
    suppress_duplicate_alerts,
)


def _make_s3_mock(alerts: list) -> MagicMock:
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(alerts).encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}
    return mock_s3


def test_generate_alert_digest_format():
    alerts = [
        {"rule_id": "A001", "severity": "CRITICAL", "crypto_id": "bitcoin"},
        {"rule_id": "A002", "severity": "CRITICAL", "crypto_id": "ethereum"},
        {"rule_id": "A003", "severity": "HIGH", "crypto_id": "solana"},
    ]
    mock_s3 = _make_s3_mock(alerts)
    with patch("consumer.alert_aggregator.boto3.client", return_value=mock_s3):
        digest = generate_alert_digest("my-bucket", "2026/06/25")
    assert "alerts" in digest
    assert "3" in digest
    assert "CRITICAL" in digest


def test_suppress_duplicate_alerts_true():
    now = datetime.now(timezone.utc).isoformat()
    history = [{"rule_id": "A001", "crypto_id": "bitcoin", "triggered_at": now}]
    mock_s3 = _make_s3_mock(history)
    alert = {"rule_id": "A001", "crypto_id": "bitcoin"}
    with patch("consumer.alert_aggregator.boto3.client", return_value=mock_s3):
        result = suppress_duplicate_alerts(alert, "my-bucket", suppress_minutes=30)
    assert result is True


def test_suppress_duplicate_alerts_false():
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("NoSuchKey")
    alert = {"rule_id": "A001", "crypto_id": "bitcoin"}
    with patch("consumer.alert_aggregator.boto3.client", return_value=mock_s3):
        result = suppress_duplicate_alerts(alert, "my-bucket", suppress_minutes=30)
    assert result is False


def test_aggregate_alerts_by_hour_structure():
    alerts = [
        {"rule_id": "A001", "crypto_id": "bitcoin", "triggered_at": "2026-06-25T10:30:00+00:00"},
        {"rule_id": "A002", "crypto_id": "ethereum", "triggered_at": "2026-06-25T10:45:00+00:00"},
        {"rule_id": "A003", "crypto_id": "solana", "triggered_at": "2026-06-25T14:15:00+00:00"},
    ]
    mock_s3 = _make_s3_mock(alerts)
    with patch("consumer.alert_aggregator.boto3.client", return_value=mock_s3):
        result = aggregate_alerts_by_hour("my-bucket", "2026/06/25")
    assert isinstance(result, dict)
    assert "10" in result
    assert result["10"] == 2
    assert "14" in result
    assert result["14"] == 1


def test_find_alert_patterns_structure():
    alerts = [
        {"rule_id": "A001", "crypto_id": "bitcoin", "triggered_at": "2026-06-25T10:00:00+00:00"},
        {"rule_id": "A001", "crypto_id": "bitcoin", "triggered_at": "2026-06-25T11:00:00+00:00"},
        {"rule_id": "A002", "crypto_id": "ethereum", "triggered_at": "2026-06-25T12:00:00+00:00"},
    ]
    mock_s3 = _make_s3_mock(alerts)
    with patch("consumer.alert_aggregator.boto3.client", return_value=mock_s3):
        result = find_alert_patterns("my-bucket", days=7)
    assert isinstance(result, dict)
    assert "most_alerted_crypto" in result
    assert result["most_alerted_crypto"] == "bitcoin"
    assert "most_common_rule" in result
    assert result["most_common_rule"] == "A001"
