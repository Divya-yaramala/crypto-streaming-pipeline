import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def generate_hourly_quality_report(
    bucket: str,
    date: str,
    hour: int,
) -> Dict[str, Any]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = f"monitoring/validation/{date}/{hour:02d}/metrics.json"
    total_events = 0
    valid_events = 0
    try:
        body = s3.get_object(Bucket=bucket, Key=key)
        data: Dict[str, Any] = json.loads(body["Body"].read().decode("utf-8"))
        total_events = int(str(data.get("total_events", 0)))
        valid_events = int(str(data.get("valid_events", 0)))
    except Exception:
        pass
    invalid_events = total_events - valid_events
    quality_score_pct = (
        round(valid_events / total_events * 100, 1) if total_events > 0 else 100.0
    )
    report: Dict[str, Any] = {
        "date": date,
        "hour": hour,
        "total_events": total_events,
        "valid_events": valid_events,
        "invalid_events": invalid_events,
        "quality_score_pct": quality_score_pct,
    }
    logger.info(
        "Hourly quality score for %s hour %02d: %.1f%%", date, hour, quality_score_pct
    )
    return report


def generate_daily_quality_report(
    bucket: str,
    date: str,
) -> Dict[str, Any]:
    by_hour: Dict[str, Any] = {}
    total_events = 0
    valid_events = 0
    for hour in range(24):
        hourly = generate_hourly_quality_report(bucket, date, hour)
        by_hour[str(hour)] = hourly
        total_events += int(str(hourly.get("total_events", 0)))
        valid_events += int(str(hourly.get("valid_events", 0)))
    invalid_events = total_events - valid_events
    quality_score_pct = (
        round(valid_events / total_events * 100, 1) if total_events > 0 else 100.0
    )
    report: Dict[str, Any] = {
        "date": date,
        "total_events": total_events,
        "valid_events": valid_events,
        "invalid_events": invalid_events,
        "quality_score_pct": quality_score_pct,
        "by_hour": by_hour,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"reports/quality/crypto/{date}/daily_report.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, default=str),
            ContentType="application/json",
        )
        logger.info("Daily quality report saved to S3: %s", key)
    except Exception as e:
        logger.error("Failed to save daily quality report: %s", e)
    logger.info("Daily quality score for %s: %.1f%%", date, quality_score_pct)
    return report


def compare_quality_trends(
    bucket: str,
    days: int = 7,
) -> Dict[str, Any]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    today = datetime.now(timezone.utc)
    daily_scores: List[float] = []
    for i in range(days):
        day = today - timedelta(days=i)
        date_str = day.strftime("%Y/%m/%d")
        key = f"reports/quality/crypto/{date_str}/daily_report.json"
        try:
            body = s3.get_object(Bucket=bucket, Key=key)
            data: Dict[str, Any] = json.loads(body["Body"].read().decode("utf-8"))
            score = float(str(data.get("quality_score_pct", 0.0)))
            daily_scores.append(score)
        except Exception:
            pass
    avg_quality = (
        round(sum(daily_scores) / len(daily_scores), 1) if daily_scores else 0.0
    )
    trend = "stable"
    if len(daily_scores) >= 2:
        recent_n = min(3, len(daily_scores))
        older_n = min(3, len(daily_scores))
        recent_avg = sum(daily_scores[:recent_n]) / recent_n
        older_avg = sum(daily_scores[-older_n:]) / older_n
        if recent_avg > older_avg + 2:
            trend = "improving"
        elif recent_avg < older_avg - 2:
            trend = "declining"
    result: Dict[str, Any] = {
        "trend": trend,
        "avg_quality": avg_quality,
        "daily_scores": daily_scores,
    }
    logger.info("Quality trend: %s (avg=%.1f%%)", trend, avg_quality)
    return result


def generate_quality_alert(
    quality_score: float,
    threshold: float = 80.0,
) -> Optional[str]:
    if quality_score < threshold:
        msg = (
            f"Quality alert: score {quality_score:.1f}% is below "
            f"threshold {threshold:.1f}%"
        )
        logger.warning(
            "Quality alert generated: %.1f%% < %.1f%%", quality_score, threshold
        )
        return msg
    return None


def run_quality_reporting(bucket: str) -> Dict[str, Any]:
    date = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    daily_report = generate_daily_quality_report(bucket, date)
    trends = compare_quality_trends(bucket)
    quality_score = float(str(daily_report.get("quality_score_pct", 100.0)))
    alert = generate_quality_alert(quality_score)
    result: Dict[str, Any] = {
        "daily_report": daily_report,
        "trends": trends,
        "alert": alert,
    }
    logger.info("Quality Reporting Complete")
    return result


if __name__ == "__main__":
    pass
