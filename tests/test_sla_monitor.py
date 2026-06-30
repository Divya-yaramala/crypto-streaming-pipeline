from unittest.mock import MagicMock, patch

from consumer.sla_monitor import (
    calculate_sla_compliance,
    generate_sla_report,
    record_sla_metric,
)


def test_record_sla_metric_met():
    mock_s3 = MagicMock()
    with patch("consumer.sla_monitor.boto3.client", return_value=mock_s3):
        result = record_sla_metric("S003", 5.0, "my-bucket", "2026/06/29")
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_record_sla_metric_breached():
    mock_s3 = MagicMock()
    with patch("consumer.sla_monitor.boto3.client", return_value=mock_s3):
        result = record_sla_metric("S003", 15.0, "my-bucket", "2026/06/29")
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_calculate_sla_compliance_perfect():
    metrics = [{"met": True} for _ in range(5)]
    result = calculate_sla_compliance(metrics)
    assert result["compliance_pct"] == 100.0
    assert result["met"] == 5
    assert result["breached"] == 0


def test_calculate_sla_compliance_partial():
    metrics = [{"met": True} for _ in range(8)] + [{"met": False} for _ in range(2)]
    result = calculate_sla_compliance(metrics)
    assert result["compliance_pct"] == 80.0
    assert result["met"] == 8
    assert result["breached"] == 2


def test_generate_sla_report_structure():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    with patch("consumer.sla_monitor.boto3.client", return_value=mock_s3):
        result = generate_sla_report("my-bucket", "2026/06/29")
    assert "overall_compliance_pct" in result
    assert "by_sla" in result
    assert isinstance(result["overall_compliance_pct"], float)
