import hashlib
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


def create_audit_entry(
    action: str,
    resource: str,
    actor: str,
    details: Optional[Dict[str, Any]] = None,
    status: str = "success",
) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    hash_input = f"{action}{resource}{ts}".encode()
    audit_id = hashlib.md5(hash_input).hexdigest()
    entry: Dict[str, Any] = {
        "audit_id": audit_id,
        "action": action,
        "resource": resource,
        "actor": actor,
        "details": details or {},
        "status": status,
        "timestamp": ts,
    }
    logger.info(
        "Audit entry created: %s action=%s resource=%s", audit_id, action, resource
    )
    return entry


def save_audit_log(entry: Dict[str, Any], bucket: str) -> bool:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        date = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        audit_id = str(entry.get("audit_id", "unknown"))
        key = f"audit/{date}/{audit_id}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(entry, default=str),
            ContentType="application/json",
        )
        logger.info("Audit log saved to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save audit log: %s", e)
        return False


def get_audit_logs(
    bucket: str,
    date: str,
    action: Optional[str] = None,
    resource: Optional[str] = None,
) -> List[Dict[str, Any]]:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"audit/{date}/"
    logs: List[Dict[str, Any]] = []
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            try:
                body = s3.get_object(Bucket=bucket, Key=str(obj["Key"]))
                entry: Dict[str, Any] = json.loads(body["Body"].read().decode("utf-8"))
                if action and str(entry.get("action", "")) != action:
                    continue
                if resource and str(entry.get("resource", "")) != resource:
                    continue
                logs.append(entry)
            except Exception:
                pass
    except Exception as e:
        logger.error("Failed to get audit logs for %s: %s", date, e)
    logger.info("Found %d audit logs for %s", len(logs), date)
    return logs


def generate_audit_report(bucket: str, date: str) -> Dict[str, Any]:
    logs = get_audit_logs(bucket, date)
    by_action: Dict[str, int] = {}
    by_resource: Dict[str, int] = {}
    for log in logs:
        action_key = str(log.get("action", "unknown"))
        resource_key = str(log.get("resource", "unknown"))
        by_action[action_key] = by_action.get(action_key, 0) + 1
        by_resource[resource_key] = by_resource.get(resource_key, 0) + 1
    report: Dict[str, Any] = {
        "date": date,
        "total": len(logs),
        "by_action": by_action,
        "by_resource": by_resource,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"audit/reports/{date}/report.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, default=str),
            ContentType="application/json",
        )
        logger.info("Audit report saved to S3: %s", key)
    except Exception as e:
        logger.error("Failed to save audit report: %s", e)
    logger.info("Audit report generated: %d events for %s", len(logs), date)
    return report


def log_pipeline_event(
    event_type: str,
    crypto_id: str,
    details: Optional[Dict[str, Any]] = None,
    bucket: str = "",
) -> bool:
    entry = create_audit_entry(
        action=event_type,
        resource=crypto_id,
        actor="pipeline",
        details=details,
    )
    if bucket:
        return save_audit_log(entry, bucket)
    return True


if __name__ == "__main__":
    pass
