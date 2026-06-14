import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def send_to_dlq(event: dict, error: str, step: str, bucket: str) -> bool:
    if not bucket:
        logger.warning("AWS_BUCKET_NAME not set, skipping DLQ save")
        return False
    try:
        now = datetime.now(timezone.utc)
        crypto_id = event.get("crypto_id", "unknown")
        key = (
            f"errors/crypto/{now.strftime('%Y/%m/%d')}/{step}"
            f"/{crypto_id}_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        )
        payload = {
            "event": event,
            "error": error,
            "step": step,
            "failed_at": now.isoformat(),
            "crypto_id": crypto_id,
        }
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
        )
        logger.info("DLQ event saved: s3://%s/%s", bucket, key)
        return True
    except Exception as e:
        logger.error("Failed to send event to DLQ: %s", e)
        return False


def get_dlq_events(bucket: str, date: str, step: Optional[str] = None) -> list:
    if not bucket:
        return []
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        prefix = f"errors/crypto/{date}/"
        if step:
            prefix += f"{step}/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        events = []
        for obj in contents:
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])
            events.append(json.loads(body["Body"].read()))
        logger.info("Found %d DLQ events for date=%s step=%s", len(events), date, step)
        return events
    except Exception as e:
        logger.error("Failed to retrieve DLQ events: %s", e)
        return []


def replay_dlq_event(event: dict, step: str) -> bool:
    from consumer import data_validator, slack_alerter
    from consumer.crypto_consumer import save_to_postgres
    from storage import s3_storage

    original = event.get("event", event)
    logger.info("Replaying DLQ event for step=%s crypto_id=%s", step, original.get("crypto_id"))
    try:
        if step == "validate":
            result = data_validator.validate_price_event(original)
            return result.get("valid", False)
        elif step == "postgres":
            import psycopg2

            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "crypto_user"),
                password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
                dbname=os.getenv("POSTGRES_DB", "crypto_db"),
            )
            try:
                return save_to_postgres(original, conn)
            finally:
                conn.close()
        elif step == "s3":
            return s3_storage.save_price_event_to_s3(original, AWS_BUCKET_NAME)
        elif step == "alert":
            change = original.get("change_24h_pct", 0)
            price = original.get("price_usd", 0)
            crypto_id = original.get("crypto_id", "unknown")
            if change > 0:
                return slack_alerter.alert_price_pump(crypto_id, change, price)
            else:
                return slack_alerter.alert_price_dump(crypto_id, change, price)
        else:
            logger.warning("Unknown replay step: %s", step)
            return False
    except Exception as e:
        logger.error("Replay failed for step=%s: %s", step, e)
        return False


def run_dlq_replay(bucket: str, date: str) -> dict:
    events = get_dlq_events(bucket, date)
    total = len(events)
    replayed = 0
    still_failing = 0

    for item in events:
        step = item.get("step", "unknown")
        if replay_dlq_event(item, step):
            replayed += 1
        else:
            still_failing += 1

    logger.info(
        "DLQ replay complete: %d total, %d replayed, %d still failing",
        total,
        replayed,
        still_failing,
    )
    return {"total": total, "replayed": replayed, "still_failing": still_failing}


if __name__ == "__main__":
    pass
