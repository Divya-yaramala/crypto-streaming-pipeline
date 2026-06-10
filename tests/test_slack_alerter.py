import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.slack_alerter import (
    alert_price_dump,
    alert_price_pump,
    send_daily_summary,
    send_slack_message,
)


def test_send_slack_message_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch("consumer.slack_alerter.SLACK_WEBHOOK_URL", "https://hooks.slack.com/test"):
        with patch("consumer.slack_alerter.requests.post", return_value=mock_response):
            result = send_slack_message("test message")
    assert result is True


def test_send_slack_message_failure():
    with patch("consumer.slack_alerter.SLACK_WEBHOOK_URL", "https://hooks.slack.com/test"):
        with patch("consumer.slack_alerter.requests.post", side_effect=Exception("Network error")):
            result = send_slack_message("test message")
    assert result is False


def test_send_slack_message_no_webhook():
    with patch("consumer.slack_alerter.SLACK_WEBHOOK_URL", ""):
        result = send_slack_message("test message")
    assert result is False


def test_alert_price_pump_message():
    with patch("consumer.slack_alerter.send_slack_message", return_value=True) as mock_send:
        result = alert_price_pump("bitcoin", 15.0, 65000.0)
    assert result is True
    message = mock_send.call_args[0][0]
    assert "PUMP" in message
    assert "bitcoin" in message


def test_alert_price_dump_message():
    with patch("consumer.slack_alerter.send_slack_message", return_value=True) as mock_send:
        result = alert_price_dump("ethereum", -12.0, 3000.0)
    assert result is True
    message = mock_send.call_args[0][0]
    assert "DUMP" in message


def test_send_daily_summary_green():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch("consumer.slack_alerter.SLACK_WEBHOOK_URL", "https://hooks.slack.com/test"):
        with patch("consumer.slack_alerter.requests.post", return_value=mock_response) as mock_post:
            result = send_daily_summary(100, 90, 5, 90.0)
    assert result is True
    payload = mock_post.call_args[1]["json"]
    assert payload["attachments"][0]["color"] == "good"
