import json
from unittest.mock import MagicMock, patch

from consumer.audit_logger import (
    create_audit_entry,
    generate_audit_report,
    get_audit_logs,
    log_pipeline_event,
    save_audit_log,
)


def test_create_audit_entry_structure():
    entry = create_audit_entry("price_ingested", "bitcoin", "producer")
    assert "audit_id" in entry
    assert "timestamp" in entry
    assert "action" in entry
    assert entry["action"] == "price_ingested"
    assert entry["resource"] == "bitcoin"
    assert entry["actor"] == "producer"


def test_save_audit_log_success():
    entry = create_audit_entry("price_ingested", "bitcoin", "producer")
    mock_s3 = MagicMock()
    with patch("consumer.audit_logger.boto3.client", return_value=mock_s3):
        result = save_audit_log(entry, "my-bucket")
    assert result is True
    mock_s3.put_object.assert_called_once()


def test_get_audit_logs_with_filter():
    entries = [
        {"audit_id": "1", "action": "price_ingested", "resource": "bitcoin"},
        {"audit_id": "2", "action": "alert_triggered", "resource": "ethereum"},
        {"audit_id": "3", "action": "price_ingested", "resource": "solana"},
    ]

    def _make_body(entry: dict) -> dict:
        body = MagicMock()
        body.read.return_value = json.dumps(entry).encode()
        return {"Body": body}

    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "audit/2026/07/01/id1.json"},
            {"Key": "audit/2026/07/01/id2.json"},
            {"Key": "audit/2026/07/01/id3.json"},
        ]
    }
    mock_s3.get_object.side_effect = [_make_body(e) for e in entries]

    with patch("consumer.audit_logger.boto3.client", return_value=mock_s3):
        result = get_audit_logs("my-bucket", "2026/07/01", action="price_ingested")

    assert len(result) == 2
    assert all(str(e["action"]) == "price_ingested" for e in result)


def test_generate_audit_report_structure():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    with patch("consumer.audit_logger.boto3.client", return_value=mock_s3):
        result = generate_audit_report("my-bucket", "2026/07/01")
    assert "total" in result
    assert "by_action" in result
    assert result["total"] == 0


def test_log_pipeline_event_success():
    mock_s3 = MagicMock()
    with patch("consumer.audit_logger.boto3.client", return_value=mock_s3):
        result = log_pipeline_event("price_ingested", "bitcoin", bucket="my-bucket")
    assert result is True
