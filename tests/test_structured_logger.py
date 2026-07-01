import json
from unittest.mock import patch

from consumer.structured_logger import (
    create_structured_log,
    log_error,
    log_metric,
    log_pipeline_start,
)


def test_create_structured_log_required_fields():
    log_dict = create_structured_log("INFO", "test message", "test_module")
    assert "timestamp" in log_dict
    assert "level" in log_dict
    assert "message" in log_dict
    assert "module" in log_dict
    assert log_dict["level"] == "INFO"
    assert log_dict["message"] == "test message"


def test_create_structured_log_with_crypto_id():
    log_dict = create_structured_log("INFO", "test message", "test_module", crypto_id="bitcoin")
    assert log_dict["crypto_id"] == "bitcoin"
    assert "bitcoin" in log_dict["tags"]


def test_log_error_with_exception():
    err = ValueError("test error")
    log_error("An error occurred", "test_module", error=err)


def test_log_metric_structure():
    with patch("consumer.structured_logger.logger") as mock_logger:
        log_metric("price_events", 42.0, "test_module", unit="count")
    mock_logger.info.assert_called_once()
    call_arg = mock_logger.info.call_args[0][0]
    logged = json.loads(call_arg)
    assert "metric_name" in logged["extra"]
    assert logged["extra"]["value"] == 42.0


def test_log_pipeline_start_no_error():
    log_pipeline_start("bitcoin", "consumer")
