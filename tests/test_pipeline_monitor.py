import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.pipeline_monitor import (
    calculate_step_stats,
    generate_daily_report,
    get_step_metrics,
    record_event_metric,
)


def test_record_event_metric_success():
    mock_s3 = MagicMock()
    with patch("consumer.pipeline_monitor.boto3.client", return_value=mock_s3):
        result = record_event_metric("consume", "bitcoin", "success", 0.123, "my-bucket")
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_record_event_metric_failure():
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("S3 unavailable")
    with patch("consumer.pipeline_monitor.boto3.client", return_value=mock_s3):
        result = record_event_metric("consume", "bitcoin", "success", 0.1, "my-bucket")
    assert result is False


def test_calculate_step_stats_success_rate():
    metrics = [{"status": "success", "duration_seconds": 0.1}] * 8 + [
        {"status": "failure", "duration_seconds": 0.5}
    ] * 2
    stats = calculate_step_stats(metrics)
    assert stats["success_rate_pct"] == 80.0
    assert stats["total_events"] == 10
    assert stats["success_count"] == 8
    assert stats["failure_count"] == 2


def test_calculate_step_stats_avg_duration():
    metrics = [
        {"status": "success", "duration_seconds": 1.0},
        {"status": "success", "duration_seconds": 2.0},
        {"status": "success", "duration_seconds": 3.0},
    ]
    stats = calculate_step_stats(metrics)
    assert stats["avg_duration_seconds"] == 2.0


def test_generate_daily_report_structure():
    mock_s3 = MagicMock()
    metric_payload = json.dumps(
        {
            "step": "consume",
            "crypto_id": "bitcoin",
            "status": "success",
            "duration_seconds": 0.1,
            "recorded_at": "2026-01-01T10:00:00+00:00",
        }
    ).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = metric_payload
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "monitoring/crypto/2026/01/01/consume/bitcoin_2026.json"}]
    }
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.pipeline_monitor.boto3.client", return_value=mock_s3):
        report = generate_daily_report("my-bucket", "2026/01/01")
    assert "total_events" in report
    assert "success_rate_pct" in report
    assert "steps" in report


def test_get_step_metrics_success():
    mock_s3 = MagicMock()
    metric_payload = json.dumps(
        {
            "step": "postgres",
            "crypto_id": "ethereum",
            "status": "success",
            "duration_seconds": 0.05,
            "recorded_at": "2026-01-01T10:00:00+00:00",
        }
    ).encode()
    mock_body = MagicMock()
    mock_body.read.return_value = metric_payload
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "monitoring/crypto/2026/01/01/postgres/ethereum_2026.json"}]
    }
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.pipeline_monitor.boto3.client", return_value=mock_s3):
        metrics = get_step_metrics("postgres", "my-bucket", "2026/01/01")
    assert isinstance(metrics, list)
    assert len(metrics) == 1
    assert metrics[0]["status"] == "success"
