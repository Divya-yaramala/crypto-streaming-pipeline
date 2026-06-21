import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import boto3

logger = logging.getLogger(__name__)


def generate_cache_key(func_name: str, params: dict) -> str:
    """Return a short MD5-based cache key derived from the function name and parameters."""
    raw = f"{func_name}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def get_from_cache(cache_key: str, bucket: str, ttl_seconds: int = 300) -> Optional[Any]:
    """Fetch cached data from S3 if it exists and has not expired; return None otherwise."""
    s3 = boto3.client("s3")
    key = f"cache/crypto/{cache_key}.json"
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        cached = json.loads(response["Body"].read().decode("utf-8"))
        expires_at = datetime.fromisoformat(cached["expires_at"])
        now = datetime.now(timezone.utc)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if now > expires_at:
            logger.info("Cache expired for key %s", cache_key)
            return None
        return cached["data"]
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        logger.warning("Cache read failed for %s: %s", cache_key, exc)
        return None


def save_to_cache(cache_key: str, data: Any, bucket: str, ttl_seconds: int = 300) -> bool:
    """Serialise data to S3 with an expiry timestamp; return True on success."""
    s3 = boto3.client("s3")
    key = f"cache/crypto/{cache_key}.json"
    now = datetime.now(timezone.utc)
    expires_at = datetime.fromtimestamp(now.timestamp() + ttl_seconds, tz=timezone.utc)
    payload = {
        "cached_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "ttl_seconds": ttl_seconds,
        "data": data,
    }
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload))
        logger.info("Saved to cache key %s (TTL %ds)", cache_key, ttl_seconds)
        return True
    except Exception as exc:
        logger.warning("Cache write failed for %s: %s", cache_key, exc)
        return False


def invalidate_cache(cache_key: str, bucket: str) -> bool:
    """Delete a cache entry from S3; return True on success."""
    s3 = boto3.client("s3")
    key = f"cache/crypto/{cache_key}.json"
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info("Invalidated cache key %s", cache_key)
        return True
    except Exception as exc:
        logger.warning("Cache invalidation failed for %s: %s", cache_key, exc)
        return False


def clear_expired_cache(bucket: str) -> int:
    """Delete all expired cache entries from S3 and return the count removed."""
    s3 = boto3.client("s3")
    prefix = "cache/crypto/"
    deleted = 0
    now = datetime.now(timezone.utc)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                try:
                    resp = s3.get_object(Bucket=bucket, Key=obj["Key"])
                    cached = json.loads(resp["Body"].read().decode("utf-8"))
                    expires_at = datetime.fromisoformat(cached["expires_at"])
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)
                    if now > expires_at:
                        s3.delete_object(Bucket=bucket, Key=obj["Key"])
                        deleted += 1
                except Exception as exc:
                    logger.warning("Skipping cache entry %s: %s", obj["Key"], exc)
                    continue
    except Exception as exc:
        logger.warning("clear_expired_cache failed: %s", exc)
    logger.info("Cleared %d expired cache entries", deleted)
    return deleted
