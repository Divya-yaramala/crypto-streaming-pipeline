import hashlib
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

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]


def generate_version_id(crypto_id: str, date: str, data: dict) -> str:
    content = crypto_id + date + json.dumps(data, sort_keys=True)
    version_id = hashlib.md5(content.encode()).hexdigest()[:8]
    logger.info("Version ID generated: %s (crypto=%s, date=%s)", version_id, crypto_id, date)
    return version_id


def save_versioned_snapshot(
    data: dict,
    crypto_id: str,
    step: str,
    bucket: str,
    date: str,
) -> str:
    version_id = generate_version_id(crypto_id, date, data)
    date_path = date.replace("-", "/")
    key = f"versions/crypto/{date_path}/{step}/{crypto_id}_{version_id}.json"
    payload = {
        "data": data,
        "version_id": version_id,
        "crypto_id": crypto_id,
        "step": step,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
        )
        logger.info("Snapshot saved: s3://%s/%s", bucket, key)
    except Exception as e:
        logger.error("Failed to save versioned snapshot: %s", e)
    return version_id


def list_versions(
    crypto_id: str,
    step: str,
    bucket: str,
    date: str,
) -> list:
    date_path = date.replace("-", "/")
    prefix = f"versions/crypto/{date_path}/{step}/{crypto_id}_"
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        versions = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            version_id = key.rsplit("_", 1)[-1].replace(".json", "")
            versions.append(
                {"version_id": version_id, "created_at": obj.get("LastModified", "")}
            )
        logger.info(
            "Found %d version(s) for %s/%s on %s", len(versions), crypto_id, step, date
        )
        return versions
    except Exception as e:
        logger.error("Failed to list versions: %s", e)
        return []


def rollback_to_version(
    crypto_id: str,
    step: str,
    version_id: str,
    bucket: str,
    date: str,
) -> dict:
    date_path = date.replace("-", "/")
    key = f"versions/crypto/{date_path}/{step}/{crypto_id}_{version_id}.json"
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(
            "Rollback performed: %s/%s to version %s on %s",
            crypto_id,
            step,
            version_id,
            date,
        )
        return payload
    except Exception as e:
        logger.error("Failed to rollback to version %s: %s", version_id, e)
        return {}


def run_versioning_check(bucket: str) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_versions = 0
    cryptos_with_versions = 0
    for crypto_id in CRYPTO_IDS:
        versions = list_versions(crypto_id, "consume", bucket, today)
        if versions:
            cryptos_with_versions += 1
            total_versions += len(versions)
    logger.info(
        "Versioning check: %d versions found across %d cryptos",
        total_versions,
        cryptos_with_versions,
    )


if __name__ == "__main__":
    run_versioning_check(os.getenv("AWS_BUCKET_NAME", ""))
