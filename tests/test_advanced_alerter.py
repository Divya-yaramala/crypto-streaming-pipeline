import json
from unittest.mock import MagicMock, patch

from consumer.advanced_alerter import (
    evaluate_alert_rules,
    get_alert_statistics,
    save_alert_history,
    send_tiered_alert,
)


def test_evaluate_alert_rules_critical():
    event = {"crypto_id": "bitcoin", "change_24h_pct": 25.0}
    result = evaluate_alert_rules(event)
    rule_ids = [r["rule_id"] for r in result]
    assert "A001" in rule_ids
    assert any(r["severity"] == "CRITICAL" for r in result)


def test_evaluate_alert_rules_no_trigger():
    event = {"crypto_id": "bitcoin", "change_24h_pct": 2.0}
    result = evaluate_alert_rules(event)
    assert result == []


def test_send_tiered_alert_critical_color():
    rule = {"rule_id": "A001", "name": "extreme_pump", "severity": "CRITICAL"}
    event = {"crypto_id": "bitcoin", "price_usd": 65000.0, "change_24h_pct": 25.0}
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("consumer.advanced_alerter.requests.post", return_value=mock_response) as mock_post:
        result = send_tiered_alert(rule, event, "https://hooks.slack.com/test")

    assert result is True
    call_kwargs = mock_post.call_args[1]
    payload = call_kwargs["json"]
    assert payload["attachments"][0]["color"] == "danger"


def test_save_alert_history_success():
    alerts = [{"rule_id": "A001", "severity": "CRITICAL", "crypto_id": "bitcoin"}]
    mock_s3 = MagicMock()
    with patch("consumer.advanced_alerter.boto3.client", return_value=mock_s3):
        result = save_alert_history(alerts, "my-bucket", "2026/06/25")
    assert result is True
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "monitoring/alerts/history/2026/06/25/alerts.json" == call_kwargs["Key"]


def test_get_alert_statistics_structure():
    alerts = [
        {"rule_id": "A001", "severity": "CRITICAL", "crypto_id": "bitcoin"},
        {"rule_id": "A002", "severity": "CRITICAL", "crypto_id": "ethereum"},
        {"rule_id": "A003", "severity": "HIGH", "crypto_id": "bitcoin"},
    ]
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(alerts).encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    with patch("consumer.advanced_alerter.boto3.client", return_value=mock_s3):
        result = get_alert_statistics("my-bucket", "2026/06/25")

    assert "total_alerts" in result
    assert result["total_alerts"] == 3
    assert "by_severity" in result
    assert result["by_severity"]["CRITICAL"] == 2
    assert "by_crypto" in result
    assert result["by_crypto"]["bitcoin"] == 2
