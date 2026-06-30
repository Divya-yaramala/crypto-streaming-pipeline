import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

SLA_THRESHOLDS: List[Dict[str, Any]] = [
    {
        "sla_id": "S001",
        "name": "producer_latency",
        "max_seconds": 60,
        "description": "Price fetched within 60s",
    },
    {
        "sla_id": "S002",
        "name": "consumer_latency",
        "max_seconds": 30,
        "description": "Event processed within 30s",
    },
    {
        "sla_id": "S003",
        "name": "s3_upload_latency",
        "max_seconds": 10,
        "description": "S3 upload within 10s",
    },
    {
        "sla_id": "S004",
        "name": "postgres_insert_latency",
        "max_seconds": 5,
        "description": "DB insert within 5s",
    },
    {
        "sla_id": "S005",
        "name": "dashboard_refresh",
        "max_seconds": 60,
        "description": "Dashboard refreshes within 60s",
    },
    {
        "sla_id": "S006",
        "name": "alert_delivery",
        "max_seconds": 30,
        "description": "Alert delivered within 30s",
    },
]


def record_sla_metric(
    sla_id: str,
    actual_seconds: float,
    bucket: str,
    date: str,
) -> bool:
    threshold = next((t for t in SLA_THRESHOLDS if str(t["sla_id"]) == sla_id), None)
    max_seconds = int(str(threshold["max_seconds"])) if threshold else 0
    met = actual_seconds <= max_seconds
    ts = datetime.now(timezone.utc).isoformat()
    ts_safe = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    metric: Dict[str, Any] = {
        "sla_id": sla_id,
        "actual_seconds": actual_seconds,
        "max_seconds": max_seconds,
        "met": met,
        "recorded_at": ts,
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"monitoring/sla/{date}/{sla_id}_{ts_safe}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(metric),
            ContentType="application/json",
        )
        status = "met" if met else "breached"
        logger.info(
            "SLA %s %s: %.2fs (max=%ds)", sla_id, status, actual_seconds, max_seconds
        )
        return True
    except Exception as e:
        logger.error("Failed to record SLA metric %s: %s", sla_id, e)
        return False


def get_sla_metrics(
    sla_id: str,
    bucket: str,
    date: str,
) -> List[Dict[str, Any]]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"monitoring/sla/{date}/{sla_id}_"
    metrics: List[Dict[str, Any]] = []
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            try:
                body = s3.get_object(Bucket=bucket, Key=str(obj["Key"]))
                metric: Dict[str, Any] = json.loads(
                    body["Body"].read().decode("utf-8")
                )
                metrics.append(metric)
            except Exception:
                pass
    except Exception as e:
        logger.error("Failed to list SLA metrics for %s: %s", sla_id, e)
    logger.info("Found %d metrics for SLA %s on %s", len(metrics), sla_id, date)
    return metrics


def calculate_sla_compliance(
    metrics: List[Dict[str, Any]],
) -> Dict[str, Any]:
    total = len(metrics)
    met = sum(1 for m in metrics if m.get("met") is True)
    breached = total - met
    compliance_pct = round(met / total * 100, 1) if total > 0 else 100.0
    result: Dict[str, Any] = {
        "total": total,
        "met": met,
        "breached": breached,
        "compliance_pct": compliance_pct,
    }
    logger.info("SLA compliance: %.1f%% (%d/%d met)", compliance_pct, met, total)
    return result


def generate_sla_report(
    bucket: str,
    date: str,
) -> Dict[str, Any]:
    by_sla: Dict[str, Any] = {}
    all_compliance: List[float] = []
    for threshold in SLA_THRESHOLDS:
        sla_id = str(threshold["sla_id"])
        metrics = get_sla_metrics(sla_id, bucket, date)
        compliance = calculate_sla_compliance(metrics)
        by_sla[sla_id] = {
            "name": str(threshold["name"]),
            "description": str(threshold["description"]),
            "compliance": compliance,
        }
        all_compliance.append(float(str(compliance["compliance_pct"])))
    overall = (
        round(sum(all_compliance) / len(all_compliance), 1) if all_compliance else 0.0
    )
    report: Dict[str, Any] = {
        "date": date,
        "overall_compliance_pct": overall,
        "by_sla": by_sla,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"monitoring/sla/reports/{date}/report.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, default=str),
            ContentType="application/json",
        )
        logger.info("SLA report saved to S3: %s", key)
    except Exception as e:
        logger.error("Failed to save SLA report: %s", e)
    logger.info("Overall SLA compliance: %.1f%%", overall)
    return report


def run_sla_check(bucket: str) -> Dict[str, Any]:
    date = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    report = generate_sla_report(bucket, date)
    overall = float(str(report.get("overall_compliance_pct", 0.0)))
    logger.info("SLA Check Complete: %.1f%% compliance", overall)
    return report


if __name__ == "__main__":
    pass
