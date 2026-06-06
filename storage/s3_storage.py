import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_s3_client():
    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    logger.info("S3 client created for region '%s'", AWS_REGION)
    return client


def save_price_event_to_s3(event: dict, bucket: str) -> bool:
    if not bucket:
        return False
    try:
        s3 = get_s3_client()
        ts = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        date_str = str(ts)[:10].replace("-", "/")
        safe_ts = str(ts).replace(":", "-").replace("+", "_")
        crypto_id = event.get("crypto_id", "unknown")
        key = f"raw/crypto/{date_str}/{crypto_id}/{safe_ts}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(event, default=str),
            ContentType="application/json",
        )
        logger.info("Saved price event to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save price event to S3: %s", e)
        return False


def save_aggregation_to_s3(agg: dict, bucket: str) -> bool:
    if not bucket:
        return False
    try:
        s3 = get_s3_client()
        now = datetime.now(timezone.utc)
        window_start = agg.get("window_start")
        if hasattr(window_start, "strftime"):
            date_str = window_start.strftime("%Y/%m/%d")
            ts_str = window_start.strftime("%Y-%m-%dT%H-%M-%S")
        else:
            date_str = now.strftime("%Y/%m/%d")
            ts_str = now.strftime("%Y-%m-%dT%H-%M-%S")
        crypto_id = agg.get("crypto_id", "unknown")
        key = f"processed/aggregations/{date_str}/{crypto_id}/{ts_str}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(agg, default=str),
            ContentType="application/json",
        )
        logger.info("Saved aggregation to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save aggregation to S3: %s", e)
        return False


def save_alert_to_s3(alert: dict, bucket: str) -> bool:
    if not bucket:
        return False
    try:
        s3 = get_s3_client()
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y/%m/%d")
        ts_str = now.strftime("%Y-%m-%dT%H-%M-%S")
        crypto_id = alert.get("crypto_id", "unknown")
        alert_type = alert.get("alert_type", "UNKNOWN")
        key = f"processed/alerts/{date_str}/{crypto_id}/{alert_type}_{ts_str}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(alert, default=str),
            ContentType="application/json",
        )
        logger.info("Saved alert to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save alert to S3: %s", e)
        return False


def archive_old_data(bucket: str, days_to_keep: int = 7) -> int:
    if not bucket:
        return 0
    s3 = get_s3_client()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
    archived = 0
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix="raw/crypto/")
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                archive_key = obj["Key"].replace("raw/crypto/", "archive/crypto/", 1)
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": obj["Key"]},
                    Key=archive_key,
                )
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                archived += 1
    logger.info("Archived %d files to archive/crypto/", archived)
    return archived


def get_daily_summary(bucket: str, date: str) -> dict:
    if not bucket:
        return {"total_events": 0, "total_alerts": 0, "total_aggregations": 0}
    s3 = get_s3_client()

    def count_objects(prefix: str) -> int:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return response.get("KeyCount", 0)

    summary = {
        "total_events": count_objects(f"raw/crypto/{date}/"),
        "total_alerts": count_objects(f"processed/alerts/{date}/"),
        "total_aggregations": count_objects(f"processed/aggregations/{date}/"),
    }
    logger.info("Daily summary for %s: %s", date, summary)
    return summary


if __name__ == "__main__":
    print(get_daily_summary(AWS_BUCKET_NAME, datetime.now(timezone.utc).strftime("%Y/%m/%d")))
