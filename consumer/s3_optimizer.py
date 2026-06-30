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

_STANDARD_COST_PER_GB = 0.023
_GLACIER_COST_PER_GB = 0.004


def calculate_storage_size(bucket: str, prefix: str) -> Dict[str, Any]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    total_bytes = 0
    object_count = 0
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            total_bytes += int(str(obj.get("Size", 0)))
            object_count += 1
    except Exception as e:
        logger.error("Failed to calculate storage size for %s: %s", prefix, e)
    total_size_mb = round(total_bytes / (1024 * 1024), 4)
    result: Dict[str, Any] = {
        "prefix": prefix,
        "total_size_mb": total_size_mb,
        "object_count": object_count,
    }
    logger.info(
        "Storage size for %s: %.4f MB (%d objects)", prefix, total_size_mb, object_count
    )
    return result


def identify_archival_candidates(
    bucket: str,
    prefix: str,
    days_old: int = 30,
) -> List[str]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_old)
    candidates: List[str] = []
    response: Optional[Dict[str, Any]] = None
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        logger.error("Failed to list objects for %s: %s", prefix, e)
        return candidates
    if response is None:
        return candidates
    for obj in response.get("Contents", []):
        last_modified = obj.get("LastModified")
        if last_modified and last_modified < cutoff:
            candidates.append(str(obj["Key"]))
    logger.info(
        "Found %d archival candidates in %s (>%d days old)",
        len(candidates),
        prefix,
        days_old,
    )
    return candidates


def move_to_glacier(bucket: str, keys: List[str]) -> Dict[str, Any]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    moved = 0
    failed = 0
    for key in keys:
        try:
            s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource={"Bucket": bucket, "Key": key},
                StorageClass="GLACIER",
                MetadataDirective="COPY",
            )
            moved += 1
        except Exception as e:
            logger.error("Failed to move %s to Glacier: %s", key, e)
            failed += 1
    result: Dict[str, Any] = {"moved": moved, "failed": failed}
    logger.info("Glacier archival: %d moved, %d failed", moved, failed)
    return result


def calculate_estimated_savings(
    object_count: int,
    avg_size_mb: float,
) -> Dict[str, float]:
    total_gb = object_count * avg_size_mb / 1024
    standard_cost = round(total_gb * _STANDARD_COST_PER_GB, 6)
    glacier_cost = round(total_gb * _GLACIER_COST_PER_GB, 6)
    savings = round(standard_cost - glacier_cost, 6)
    result: Dict[str, float] = {
        "standard_cost": standard_cost,
        "glacier_cost": glacier_cost,
        "savings": savings,
    }
    logger.info(
        "Estimated savings: $%.4f/month (standard=$%.4f, glacier=$%.4f)",
        savings,
        standard_cost,
        glacier_cost,
    )
    return result


def run_cost_optimization(bucket: str) -> Dict[str, Any]:
    prefixes = ["raw/", "processed/", "monitoring/", "cache/"]
    by_prefix: Dict[str, Any] = {}
    total_objects = 0
    total_size_mb = 0.0
    total_candidates = 0
    for prefix in prefixes:
        size_info = calculate_storage_size(bucket, prefix)
        candidates = identify_archival_candidates(bucket, prefix)
        by_prefix[prefix] = {
            "size_info": size_info,
            "archival_candidates": len(candidates),
        }
        total_objects += int(str(size_info.get("object_count", 0)))
        total_size_mb += float(str(size_info.get("total_size_mb", 0.0)))
        total_candidates += len(candidates)
    avg_size_mb = total_size_mb / total_objects if total_objects > 0 else 0.0
    savings = calculate_estimated_savings(total_candidates, avg_size_mb)
    report: Dict[str, Any] = {
        "total_objects": total_objects,
        "total_size_mb": round(total_size_mb, 4),
        "archival_candidates": total_candidates,
        "savings": savings,
        "by_prefix": by_prefix,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    potential_savings = float(str(savings.get("savings", 0.0)))
    logger.info("Cost Optimization Complete: $%.4f potential savings", potential_savings)
    return report


if __name__ == "__main__":
    pass
