import json
import logging
import os
from datetime import datetime, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

PIPELINE_STEPS = ["consume", "validate", "postgres", "s3", "alert", "spark_process"]


def record_event_metric(
    step: str,
    crypto_id: str,
    status: str,
    duration_seconds: float,
    bucket: str,
) -> bool:
    if not bucket:
        return False
    try:
        now = datetime.now(timezone.utc)
        key = (
            f"monitoring/crypto/{now.strftime('%Y/%m/%d')}/{step}"
            f"/{crypto_id}_{now.strftime('%Y-%m-%dT%H-%M-%S-%f')}.json"
        )
        payload = {
            "step": step,
            "crypto_id": crypto_id,
            "status": status,
            "duration_seconds": duration_seconds,
            "recorded_at": now.isoformat(),
        }
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
        )
        logger.info(
            "Metric recorded: step=%s crypto=%s status=%s %.3fs",
            step, crypto_id, status, duration_seconds,
        )
        return True
    except Exception as e:
        logger.error("Failed to record metric for step=%s: %s", step, e)
        return False


def get_step_metrics(step: str, bucket: str, date: str) -> list:
    if not bucket:
        return []
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        prefix = f"monitoring/crypto/{date}/{step}/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        metrics = []
        for obj in contents:
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])
            metrics.append(json.loads(body["Body"].read()))
        logger.info("Found %d metrics for step=%s date=%s", len(metrics), step, date)
        return metrics
    except Exception as e:
        logger.error("Failed to retrieve metrics for step=%s: %s", step, e)
        return []


def calculate_step_stats(metrics: list) -> dict:
    if not metrics:
        return {
            "total_events": 0,
            "success_count": 0,
            "failure_count": 0,
            "success_rate_pct": 0.0,
            "avg_duration_seconds": 0.0,
            "max_duration_seconds": 0.0,
            "min_duration_seconds": 0.0,
        }
    total = len(metrics)
    success_count = sum(1 for m in metrics if m.get("status") == "success")
    failure_count = total - success_count
    durations = [m.get("duration_seconds", 0.0) for m in metrics]
    return {
        "total_events": total,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate_pct": round(success_count / total * 100, 2),
        "avg_duration_seconds": round(sum(durations) / total, 4),
        "max_duration_seconds": round(max(durations), 4),
        "min_duration_seconds": round(min(durations), 4),
    }


def generate_hourly_report(bucket: str, date: str, hour: int) -> dict:
    report: dict = {"date": date, "hour": hour, "steps": {}}
    for step in PIPELINE_STEPS:
        metrics = get_step_metrics(step, bucket, date)
        hour_metrics = [
            m for m in metrics
            if datetime.fromisoformat(m["recorded_at"]).hour == hour
        ]
        report["steps"][step] = calculate_step_stats(hour_metrics)
    total = sum(s["total_events"] for s in report["steps"].values())
    logger.info("Hourly report for %s hour=%d: %d total events", date, hour, total)
    return report


def generate_daily_report(bucket: str, date: str) -> dict:
    all_metrics: list = []
    step_stats: dict = {}
    for step in PIPELINE_STEPS:
        metrics = get_step_metrics(step, bucket, date)
        all_metrics.extend(metrics)
        step_stats[step] = calculate_step_stats(metrics)

    overall = calculate_step_stats(all_metrics)
    slowest_step = max(
        step_stats, key=lambda s: step_stats[s]["avg_duration_seconds"], default="n/a"
    )

    report = {
        "date": date,
        "total_events": overall["total_events"],
        "success_rate_pct": overall["success_rate_pct"],
        "avg_duration_seconds": overall["avg_duration_seconds"],
        "slowest_step": slowest_step,
        "steps": step_stats,
    }

    if bucket:
        try:
            s3 = boto3.client("s3", region_name=AWS_REGION)
            key = f"monitoring/reports/crypto/{date}/daily_report.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(report, default=str),
                ContentType="application/json",
            )
        except Exception as e:
            logger.error("Failed to save daily report to S3: %s", e)

    logger.info(
        "Daily report for %s: total=%d success_rate=%.1f%% slowest_step=%s",
        date,
        overall["total_events"],
        overall["success_rate_pct"],
        slowest_step,
    )
    return report


if __name__ == "__main__":
    pass
