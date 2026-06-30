import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_system_resources() -> Dict[str, float]:
    cpu_pct = float(str(psutil.cpu_percent(interval=None)))
    mem_pct = float(str(psutil.virtual_memory().percent))
    disk_pct = float(str(psutil.disk_usage("/").percent))
    resources: Dict[str, float] = {
        "cpu_percent": cpu_pct,
        "memory_percent": mem_pct,
        "disk_percent": disk_pct,
    }
    logger.info(
        "Resources: CPU=%.1f%% MEM=%.1f%% DISK=%.1f%%", cpu_pct, mem_pct, disk_pct
    )
    return resources


def check_resource_thresholds(
    resources: Dict[str, float],
) -> Dict[str, Any]:
    warnings: List[str] = []
    cpu_pct = float(str(resources.get("cpu_percent", 0.0)))
    mem_pct = float(str(resources.get("memory_percent", 0.0)))
    disk_pct = float(str(resources.get("disk_percent", 0.0)))
    if cpu_pct > 80:
        warnings.append(f"CPU usage high: {cpu_pct:.1f}% (threshold 80%)")
    if mem_pct > 85:
        warnings.append(f"Memory usage high: {mem_pct:.1f}% (threshold 85%)")
    if disk_pct > 90:
        warnings.append(f"Disk usage high: {disk_pct:.1f}% (threshold 90%)")
    healthy = len(warnings) == 0
    result: Dict[str, Any] = {"healthy": healthy, "warnings": warnings}
    status = "HEALTHY" if healthy else f"WARNING ({len(warnings)} issues)"
    logger.info("Resource threshold check: %s", status)
    return result


def estimate_kafka_throughput(
    messages_per_second: float,
    avg_message_size_kb: float,
) -> Dict[str, float]:
    mb_per_second = round(messages_per_second * avg_message_size_kb / 1024, 4)
    gb_per_day = round(mb_per_second * 86400 / 1024, 2)
    result: Dict[str, float] = {
        "messages_per_second": messages_per_second,
        "avg_message_size_kb": avg_message_size_kb,
        "mb_per_second": mb_per_second,
        "gb_per_day": gb_per_day,
    }
    logger.info("Kafka throughput: %.4f MB/s, %.2f GB/day", mb_per_second, gb_per_day)
    return result


def calculate_consumer_lag_estimate(
    produced_count: int,
    consumed_count: int,
) -> Dict[str, Any]:
    lag = produced_count - consumed_count
    lag_pct = round(lag / produced_count * 100, 1) if produced_count > 0 else 0.0
    if lag_pct < 5:
        status = "healthy"
    elif lag_pct < 20:
        status = "warning"
    else:
        status = "critical"
    result: Dict[str, Any] = {
        "lag": lag,
        "lag_pct": lag_pct,
        "status": status,
    }
    logger.info("Consumer lag: %d (%.1f%%) — %s", lag, lag_pct, status)
    return result


def run_resource_check(bucket: str) -> Dict[str, Any]:
    resources = get_system_resources()
    thresholds = check_resource_thresholds(resources)
    ts = datetime.now(timezone.utc).isoformat()
    report: Dict[str, Any] = {
        "checked_at": ts,
        "resources": resources,
        "thresholds": thresholds,
    }
    error_msg: Optional[str] = None
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        date = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"monitoring/resources/{date}/check.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, default=str),
            ContentType="application/json",
        )
        logger.info("Resource check saved to S3: %s", key)
    except Exception as e:
        error_msg = str(e)
        logger.error("Failed to save resource check: %s", error_msg)
    healthy = thresholds.get("healthy", True)
    logger.info("Resource check complete: healthy=%s", healthy)
    return report


if __name__ == "__main__":
    pass
