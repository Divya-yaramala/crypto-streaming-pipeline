import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

from consumer.data_observatory import run_observatory_check

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

_GRADE_THRESHOLDS = [
    (90, "A"),
    (75, "B"),
    (60, "C"),
    (45, "D"),
]


def score_to_grade(score: float) -> str:
    for threshold, grade in _GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "F"


def get_health_status(score: float) -> str:
    if score >= 75:
        return "HEALTHY"
    if score >= 50:
        return "DEGRADED"
    return "CRITICAL"


def compute_health_recommendations(observatory_result: dict) -> list:
    recs = []
    by_crypto = observatory_result.get("by_crypto", {})
    for crypto_id, data in by_crypto.items():
        freshness = data.get("freshness", {})
        completeness = data.get("completeness", {})
        anomaly = data.get("anomaly", {})

        if not freshness.get("is_fresh"):
            hours = freshness.get("hours_since_update", 0.0)
            recs.append(f"{crypto_id}: data is {hours:.1f}h old — check ingestion pipeline")

        completeness_pct = completeness.get("completeness_pct", 100.0)
        if completeness_pct < 80:
            missing_count = len(completeness.get("missing", []))
            recs.append(f"{crypto_id}: {missing_count} S3 paths missing — check data pipeline")

        anomaly_pct = anomaly.get("anomaly_pct", 0.0)
        if anomaly_pct >= 5:
            recs.append(f"{crypto_id}: {anomaly_pct:.1f}% price anomalies — validate source data")

    if not recs:
        recs.append("All systems healthy — no action required")

    return recs


def build_health_report(observatory_result: dict) -> dict:
    overall_score = observatory_result.get("overall_score", 0.0)
    grade = score_to_grade(overall_score)
    status = get_health_status(overall_score)
    recommendations = compute_health_recommendations(observatory_result)

    by_crypto_summary: dict = {}
    for crypto_id, data in observatory_result.get("by_crypto", {}).items():
        crypto_score = data.get("score", 0.0)
        by_crypto_summary[crypto_id] = {
            "score": crypto_score,
            "grade": score_to_grade(crypto_score),
            "status": get_health_status(crypto_score),
        }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_score": overall_score,
        "grade": grade,
        "status": status,
        "by_crypto": by_crypto_summary,
        "recommendations": recommendations,
    }
    logger.info("Health report: score=%.1f grade=%s status=%s", overall_score, grade, status)
    return report


def save_health_report(report: dict, bucket: str) -> bool:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"monitoring/health/{today}/report.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, default=str),
            ContentType="application/json",
        )
        logger.info("Health report saved to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save health report: %s", e)
        return False


def load_health_history(bucket: str, days: int = 7) -> list:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    today = datetime.now(timezone.utc)
    history = []
    for i in range(days):
        day = today - timedelta(days=i)
        date_str = day.strftime("%Y/%m/%d")
        key = f"monitoring/health/{date_str}/report.json"
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            report = json.loads(response["Body"].read().decode("utf-8"))
            history.append(report)
        except Exception:
            pass
    return history


def compute_health_trend(bucket: str, days: int = 7) -> dict:
    history = load_health_history(bucket, days)
    if not history:
        return {"trend": "unknown", "scores": [], "avg_score": 0.0}

    scores = [r.get("overall_score", 0.0) for r in history]
    avg_score = round(sum(scores) / len(scores), 2)

    if len(scores) >= 2:
        recent_n = min(3, len(scores))
        older_n = min(3, len(scores))
        recent_avg = sum(scores[:recent_n]) / recent_n
        older_avg = sum(scores[-older_n:]) / older_n
        if recent_avg > older_avg + 5:
            trend = "improving"
        elif recent_avg < older_avg - 5:
            trend = "degrading"
        else:
            trend = "stable"
    else:
        trend = "stable"

    logger.info("Health trend: %s (avg=%.1f over %d days)", trend, avg_score, len(scores))
    return {"trend": trend, "scores": scores, "avg_score": avg_score}


def run_health_check(bucket: str) -> dict:
    observatory_result = run_observatory_check(bucket)
    report = build_health_report(observatory_result)
    if bucket:
        save_health_report(report, bucket)
    return report


if __name__ == "__main__":
    pass
