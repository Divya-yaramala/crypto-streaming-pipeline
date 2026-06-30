from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from consumer.s3_optimizer import (
    calculate_estimated_savings,
    calculate_storage_size,
    identify_archival_candidates,
    move_to_glacier,
    run_cost_optimization,
)


def test_calculate_storage_size_structure():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "raw/file1.json", "Size": 1024 * 1024},
            {"Key": "raw/file2.json", "Size": 2 * 1024 * 1024},
        ]
    }
    with patch("consumer.s3_optimizer.boto3.client", return_value=mock_s3):
        result = calculate_storage_size("my-bucket", "raw/")
    assert "total_size_mb" in result
    assert result["total_size_mb"] > 0
    assert result["object_count"] == 2


def test_identify_archival_candidates_finds_old():
    old_date = datetime.now(timezone.utc) - timedelta(days=60)
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "raw/old.json", "LastModified": old_date, "Size": 1024},
            {"Key": "raw/new.json", "LastModified": datetime.now(timezone.utc), "Size": 512},
        ]
    }
    with patch("consumer.s3_optimizer.boto3.client", return_value=mock_s3):
        result = identify_archival_candidates("my-bucket", "raw/", days_old=30)
    assert len(result) == 1
    assert "raw/old.json" in result


def test_move_to_glacier_success():
    mock_s3 = MagicMock()
    keys = ["raw/file1.json", "raw/file2.json"]
    with patch("consumer.s3_optimizer.boto3.client", return_value=mock_s3):
        result = move_to_glacier("my-bucket", keys)
    assert "moved" in result
    assert result["moved"] == 2
    assert result["failed"] == 0


def test_calculate_estimated_savings_positive():
    result = calculate_estimated_savings(1000, 1.0)
    assert result["savings"] > 0
    assert result["standard_cost"] > result["glacier_cost"]


def test_run_cost_optimization_structure():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    with patch("consumer.s3_optimizer.boto3.client", return_value=mock_s3):
        result = run_cost_optimization("my-bucket")
    assert isinstance(result, dict)
    assert "savings" in result
    assert "archival_candidates" in result
