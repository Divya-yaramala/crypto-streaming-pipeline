import json
from unittest.mock import MagicMock, patch

from consumer.quality_reporter import (
    compare_quality_trends,
    generate_daily_quality_report,
    generate_quality_alert,
)


def _make_quality_body(score: float) -> dict:
    body = MagicMock()
    body.read.return_value = json.dumps({"quality_score_pct": score}).encode()
    return {"Body": body}


def test_generate_daily_quality_report_structure():
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("Not found")
    with patch("consumer.quality_reporter.boto3.client", return_value=mock_s3):
        result = generate_daily_quality_report("my-bucket", "2026/06/29")
    assert "quality_score_pct" in result
    assert "by_hour" in result
    assert isinstance(result["quality_score_pct"], float)


def test_compare_quality_trends_improving():
    scores = [95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 65.0]
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = [_make_quality_body(s) for s in scores]
    with patch("consumer.quality_reporter.boto3.client", return_value=mock_s3):
        result = compare_quality_trends("my-bucket", days=7)
    assert result["trend"] == "improving"


def test_compare_quality_trends_declining():
    scores = [65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0]
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = [_make_quality_body(s) for s in scores]
    with patch("consumer.quality_reporter.boto3.client", return_value=mock_s3):
        result = compare_quality_trends("my-bucket", days=7)
    assert result["trend"] == "declining"


def test_generate_quality_alert_triggered():
    result = generate_quality_alert(70.0, threshold=80.0)
    assert result is not None
    assert isinstance(result, str)
    assert "70.0" in result


def test_generate_quality_alert_not_triggered():
    result = generate_quality_alert(90.0, threshold=80.0)
    assert result is None
