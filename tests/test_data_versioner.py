import json
from unittest.mock import MagicMock, patch

from consumer.data_versioner import (
    generate_version_id,
    list_versions,
    rollback_to_version,
    save_versioned_snapshot,
)


def test_generate_version_id_consistent():
    data = {"price_usd": 50000.0, "crypto_id": "bitcoin"}
    v1 = generate_version_id("bitcoin", "2026-06-26", data)
    v2 = generate_version_id("bitcoin", "2026-06-26", data)
    assert v1 == v2
    assert len(v1) == 8


def test_generate_version_id_different():
    data1 = {"price_usd": 50000.0}
    data2 = {"price_usd": 51000.0}
    v1 = generate_version_id("bitcoin", "2026-06-26", data1)
    v2 = generate_version_id("bitcoin", "2026-06-26", data2)
    assert v1 != v2


def test_save_versioned_snapshot_success():
    data = {"price_usd": 50000.0, "crypto_id": "bitcoin"}
    mock_s3 = MagicMock()
    with patch("consumer.data_versioner.boto3.client", return_value=mock_s3):
        result = save_versioned_snapshot(data, "bitcoin", "consume", "my-bucket", "2026-06-26")
    assert isinstance(result, str)
    assert len(result) == 8
    mock_s3.put_object.assert_called_once()


def test_save_versioned_snapshot_correct_path():
    data = {"price_usd": 50000.0}
    mock_s3 = MagicMock()
    with patch("consumer.data_versioner.boto3.client", return_value=mock_s3):
        save_versioned_snapshot(data, "bitcoin", "consume", "my-bucket", "2026-06-26")
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "versions/crypto/2026/06/26/" in call_kwargs["Key"]


def test_list_versions_success():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "versions/crypto/2026/06/26/consume/bitcoin_abc12345.json",
                "LastModified": "2026-06-26T10:00:00Z",
            },
        ]
    }
    with patch("consumer.data_versioner.boto3.client", return_value=mock_s3):
        result = list_versions("bitcoin", "consume", "my-bucket", "2026-06-26")
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["version_id"] == "abc12345"


def test_rollback_to_version_success():
    payload = {
        "data": {"price_usd": 50000.0},
        "version_id": "abc12345",
        "crypto_id": "bitcoin",
        "step": "consume",
        "created_at": "2026-06-26T10:00:00Z",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(payload).encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("consumer.data_versioner.boto3.client", return_value=mock_s3):
        result = rollback_to_version("bitcoin", "consume", "abc12345", "my-bucket", "2026-06-26")
    assert isinstance(result, dict)
    assert "data" in result
    assert result["version_id"] == "abc12345"
