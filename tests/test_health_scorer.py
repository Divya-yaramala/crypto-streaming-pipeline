import json
from unittest.mock import MagicMock, patch

from consumer.health_scorer import (
    build_health_report,
    compute_health_recommendations,
    compute_health_trend,
    get_health_status,
    save_health_report,
    score_to_grade,
)


def _observatory_result(overall_score: float = 85.0, is_fresh: bool = True) -> dict:
    return {
        "overall_score": overall_score,
        "by_crypto": {
            "bitcoin": {
                "score": overall_score,
                "freshness": {"is_fresh": is_fresh, "hours_since_update": 0.5 if is_fresh else 5.0},
                "completeness": {"completeness_pct": 100.0, "missing": []},
                "anomaly": {"anomaly_pct": 0.0},
            }
        },
    }


def test_score_to_grade_a():
    assert score_to_grade(95.0) == "A"
    assert score_to_grade(90.0) == "A"


def test_score_to_grade_b():
    assert score_to_grade(80.0) == "B"
    assert score_to_grade(75.0) == "B"


def test_score_to_grade_c():
    assert score_to_grade(65.0) == "C"
    assert score_to_grade(60.0) == "C"


def test_score_to_grade_f():
    assert score_to_grade(30.0) == "F"
    assert score_to_grade(0.0) == "F"


def test_get_health_status_healthy():
    assert get_health_status(90.0) == "HEALTHY"
    assert get_health_status(75.0) == "HEALTHY"


def test_get_health_status_degraded():
    assert get_health_status(60.0) == "DEGRADED"
    assert get_health_status(50.0) == "DEGRADED"


def test_get_health_status_critical():
    assert get_health_status(40.0) == "CRITICAL"
    assert get_health_status(0.0) == "CRITICAL"


def test_compute_health_recommendations_all_healthy():
    result = _observatory_result(90.0, is_fresh=True)
    recs = compute_health_recommendations(result)
    assert len(recs) == 1
    assert "healthy" in recs[0].lower()


def test_compute_health_recommendations_stale_data():
    result = _observatory_result(40.0, is_fresh=False)
    recs = compute_health_recommendations(result)
    assert any("bitcoin" in r for r in recs)
    assert any("ingestion" in r for r in recs)


def test_compute_health_recommendations_anomalies():
    result = {
        "overall_score": 60.0,
        "by_crypto": {
            "solana": {
                "freshness": {"is_fresh": True, "hours_since_update": 0.5},
                "completeness": {"completeness_pct": 100.0, "missing": []},
                "anomaly": {"anomaly_pct": 8.5},
            }
        },
    }
    recs = compute_health_recommendations(result)
    assert any("anomalies" in r for r in recs)
    assert any("solana" in r for r in recs)


def test_build_health_report_structure():
    result = _observatory_result(85.0)
    report = build_health_report(result)
    assert "overall_score" in report
    assert "grade" in report
    assert "status" in report
    assert "by_crypto" in report
    assert "recommendations" in report
    assert "generated_at" in report


def test_build_health_report_grade_and_status():
    report = build_health_report(_observatory_result(85.0))
    assert report["grade"] == "B"
    assert report["status"] == "HEALTHY"

    report_critical = build_health_report(_observatory_result(30.0))
    assert report_critical["grade"] == "F"
    assert report_critical["status"] == "CRITICAL"


def test_save_health_report_success():
    report = {"overall_score": 85.0, "grade": "B", "status": "HEALTHY"}
    mock_s3 = MagicMock()
    with patch("consumer.health_scorer.boto3.client", return_value=mock_s3):
        result = save_health_report(report, "my-bucket")
    assert result is True
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "monitoring/health/" in call_kwargs["Key"]


def test_save_health_report_s3_error():
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("AccessDenied")
    with patch("consumer.health_scorer.boto3.client", return_value=mock_s3):
        result = save_health_report({}, "my-bucket")
    assert result is False


def test_compute_health_trend_structure():
    reports = [
        {"overall_score": 80.0},
        {"overall_score": 75.0},
        {"overall_score": 70.0},
    ]
    bodies = []
    for r in reports:
        body = MagicMock()
        body.read.return_value = json.dumps(r).encode()
        bodies.append(body)

    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = [{"Body": b} for b in bodies]

    with patch("consumer.health_scorer.boto3.client", return_value=mock_s3):
        result = compute_health_trend("my-bucket", days=3)

    assert "trend" in result
    assert "scores" in result
    assert "avg_score" in result
    assert len(result["scores"]) == 3
    assert result["avg_score"] == 75.0


def test_compute_health_trend_no_history():
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("NoSuchKey")
    with patch("consumer.health_scorer.boto3.client", return_value=mock_s3):
        result = compute_health_trend("my-bucket", days=3)
    assert result["trend"] == "unknown"
    assert result["scores"] == []
    assert result["avg_score"] == 0.0
