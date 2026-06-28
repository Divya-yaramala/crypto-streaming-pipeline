import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from consumer.data_observatory import (
    calculate_observatory_score,
    check_data_completeness,
    check_data_freshness,
    check_price_anomaly,
    run_observatory_check,
)


def _make_s3_fresh(crypto_id: str, hours_ago: float) -> MagicMock:
    ts = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    body = MagicMock()
    body.read.return_value = json.dumps({"timestamp": ts, "crypto_id": crypto_id}).encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": body}
    return mock_s3


def test_check_data_freshness_fresh():
    mock_s3 = _make_s3_fresh("bitcoin", 0.5)
    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = check_data_freshness("my-bucket", "bitcoin")
    assert result["crypto_id"] == "bitcoin"
    assert result["is_fresh"] is True
    assert result["hours_since_update"] < 2.0


def test_check_data_freshness_stale():
    mock_s3 = _make_s3_fresh("ethereum", 5.0)
    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = check_data_freshness("my-bucket", "ethereum")
    assert result["is_fresh"] is False
    assert result["hours_since_update"] >= 2.0


def test_check_data_freshness_s3_error():
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("NoSuchKey")
    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = check_data_freshness("my-bucket", "bitcoin")
    assert result["is_fresh"] is False
    assert result["hours_since_update"] == 999.0


def test_check_data_completeness_all_present():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "some-key"}]}
    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = check_data_completeness("my-bucket", "bitcoin", "2026/06/28")
    assert result["crypto_id"] == "bitcoin"
    assert result["completeness_pct"] == 100.0
    assert result["missing"] == []


def test_check_data_completeness_some_missing():
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "k1"}]},
        {},
        {"Contents": [{"Key": "k3"}]},
        {},
        {"Contents": [{"Key": "k5"}]},
    ]
    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = check_data_completeness("my-bucket", "bitcoin", "2026/06/28")
    assert result["completeness_pct"] == 60.0
    assert len(result["missing"]) == 2


def test_check_price_anomaly_no_anomalies():
    prices = [100.0, 101.0, 99.5, 100.5, 100.2, 99.8]
    result = check_price_anomaly(prices, "bitcoin")
    assert result["crypto_id"] == "bitcoin"
    assert result["anomaly_count"] == 0
    assert result["anomaly_pct"] == 0.0


def test_check_price_anomaly_with_outlier():
    prices = [100.0] * 20 + [10000.0]
    result = check_price_anomaly(prices, "bitcoin")
    assert result["anomaly_count"] >= 1
    assert result["anomaly_pct"] > 0.0


def test_check_price_anomaly_too_few_prices():
    result = check_price_anomaly([100.0, 200.0], "bitcoin")
    assert result["anomaly_count"] == 0
    assert result["anomaly_pct"] == 0.0


def test_calculate_observatory_score_perfect():
    freshness = {"is_fresh": True}
    completeness = {"completeness_pct": 100.0}
    anomaly = {"anomaly_pct": 0.0}
    score = calculate_observatory_score(freshness, completeness, anomaly)
    assert score == 100.0


def test_calculate_observatory_score_partial():
    freshness = {"is_fresh": False}
    completeness = {"completeness_pct": 80.0}
    anomaly = {"anomaly_pct": 0.0}
    score = calculate_observatory_score(freshness, completeness, anomaly)
    # f=0*40=0, c=0.8*40=32, a=1.0*20=20 => 52.0
    assert score == 52.0


def test_run_observatory_check_structure():
    mock_s3 = MagicMock()
    fresh_body = MagicMock()
    ts = (datetime.now(timezone.utc) - timedelta(hours=0.5)).isoformat()
    fresh_body.read.return_value = json.dumps({"timestamp": ts}).encode()
    mock_s3.get_object.return_value = {"Body": fresh_body}
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "k"}]}

    with patch("consumer.data_observatory.boto3.client", return_value=mock_s3):
        result = run_observatory_check("my-bucket")

    assert "overall_score" in result
    assert "by_crypto" in result
    assert 0.0 <= result["overall_score"] <= 100.0
