import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage.s3_storage import (
    archive_old_data,
    get_daily_summary,
    save_alert_to_s3,
    save_price_event_to_s3,
)

SAMPLE_EVENT = {
    "crypto_id": "bitcoin",
    "price_usd": 65000.0,
    "market_cap_usd": 1280000000000.0,
    "change_24h_pct": 2.5,
    "timestamp": "2026-06-06T10:00:00+00:00",
    "source": "coingecko",
}

SAMPLE_ALERT = {
    "crypto_id": "bitcoin",
    "alert_type": "PUMP",
    "message": "Price up more than 10% in 24h",
    "price_usd": 70000.0,
}


def test_save_price_event_to_s3_success():
    mock_client = MagicMock()

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        result = save_price_event_to_s3(SAMPLE_EVENT, "test-bucket")

    assert result is True
    mock_client.put_object.assert_called_once()


def test_save_price_event_to_s3_failure():
    mock_client = MagicMock()
    mock_client.put_object.side_effect = Exception("S3 connection failed")

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        result = save_price_event_to_s3(SAMPLE_EVENT, "test-bucket")

    assert result is False


def test_save_price_event_correct_path():
    mock_client = MagicMock()

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        save_price_event_to_s3(SAMPLE_EVENT, "test-bucket")

    call_kwargs = mock_client.put_object.call_args[1]
    assert "raw/crypto/2026/06/06" in call_kwargs["Key"]
    assert "bitcoin" in call_kwargs["Key"]


def test_save_alert_to_s3_success():
    mock_client = MagicMock()

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        result = save_alert_to_s3(SAMPLE_ALERT, "test-bucket")

    assert result is True
    mock_client.put_object.assert_called_once()


def test_archive_old_data_success():
    mock_client = MagicMock()
    mock_paginator = MagicMock()
    old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "raw/crypto/2020/01/01/bitcoin/ts.json",
                    "LastModified": old_time,
                }
            ]
        }
    ]
    mock_client.get_paginator.return_value = mock_paginator

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        result = archive_old_data("test-bucket", days_to_keep=7)

    assert result > 0
    mock_client.copy_object.assert_called_once()
    mock_client.delete_object.assert_called_once()


def test_get_daily_summary_structure():
    mock_client = MagicMock()
    mock_client.list_objects_v2.return_value = {"KeyCount": 5}

    with patch("storage.s3_storage.get_s3_client", return_value=mock_client):
        result = get_daily_summary("test-bucket", "2026/06/06")

    assert "total_events" in result
    assert "total_alerts" in result
    assert "total_aggregations" in result
    assert isinstance(result["total_events"], int)
