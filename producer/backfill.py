import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")


def fetch_historical_prices(crypto_id: str, days: int = 30) -> list:
    try:
        url = f"{COINGECKO_BASE_URL}/coins/{crypto_id}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "daily"}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", [])
        logger.info("Fetched %d historical records for %s (%d days)", len(prices), crypto_id, days)
        return prices
    except Exception as e:
        logger.error("Failed to fetch historical prices for %s: %s", crypto_id, e)
        return []


def format_historical_event(crypto_id: str, timestamp_ms: int, price: float) -> dict:
    ts = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return {
        "crypto_id": crypto_id,
        "price_usd": price,
        "market_cap_usd": None,
        "change_24h_pct": None,
        "timestamp": ts.isoformat(),
        "source": "coingecko_historical",
    }


def save_historical_to_s3(events: list, crypto_id: str, bucket: str) -> int:
    if not bucket or not events:
        return 0
    s3 = boto3.client("s3", region_name=AWS_REGION)
    saved = 0
    for i, event in enumerate(events):
        try:
            ts_str = event["timestamp"].replace(":", "-").replace("+", "")
            date_part = datetime.fromisoformat(event["timestamp"]).strftime("%Y/%m/%d")
            key = f"raw/crypto/{date_part}/{crypto_id}/historical_{ts_str}.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(event, default=str),
                ContentType="application/json",
            )
            saved += 1
            if (i + 1) % 10 == 0:
                logger.info(
                    "S3 backfill progress: %d/%d events saved for %s",
                    i + 1,
                    len(events),
                    crypto_id,
                )
        except Exception as e:
            logger.error("Failed to save historical event to S3: %s", e)
    logger.info("Saved %d/%d historical events to S3 for %s", saved, len(events), crypto_id)
    return saved


def save_historical_to_postgres(events: list, conn) -> int:
    if not events:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for event in events:
            try:
                cur.execute(
                    """
                    INSERT INTO crypto_prices
                        (crypto_id, price_usd, market_cap_usd, change_24h_pct,
                         event_timestamp, source)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (crypto_id, event_timestamp) DO NOTHING
                    """,
                    (
                        event.get("crypto_id"),
                        event.get("price_usd"),
                        event.get("market_cap_usd"),
                        event.get("change_24h_pct"),
                        event.get("timestamp"),
                        event.get("source"),
                    ),
                )
                inserted += cur.rowcount
            except Exception as e:
                logger.error("Failed to insert historical event: %s", e)
                conn.rollback()
    conn.commit()
    logger.info("Inserted %d historical rows into PostgreSQL", inserted)
    return inserted


def run_backfill(crypto_ids: list, days: int = 30, bucket: str = None) -> dict:
    total_events = 0
    saved_s3 = 0
    saved_postgres = 0

    pg_conn = None
    if True:
        try:
            pg_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
            )
        except Exception as e:
            logger.error("PostgreSQL connection failed, skipping DB backfill: %s", e)

    for crypto_id in crypto_ids:
        price_pairs = fetch_historical_prices(crypto_id, days)
        events = [format_historical_event(crypto_id, ts, price) for ts, price in price_pairs]
        total_events += len(events)

        if bucket:
            saved_s3 += save_historical_to_s3(events, crypto_id, bucket)

        if pg_conn:
            saved_postgres += save_historical_to_postgres(events, pg_conn)

        time.sleep(1)

    if pg_conn:
        pg_conn.close()

    logger.info(
        "Backfill complete: %d total events, %d saved to S3, %d saved to PostgreSQL",
        total_events,
        saved_s3,
        saved_postgres,
    )
    return {"total_events": total_events, "saved_s3": saved_s3, "saved_postgres": saved_postgres}


if __name__ == "__main__":
    from producer.config import CRYPTO_IDS

    result = run_backfill(CRYPTO_IDS, days=30, bucket=os.getenv("AWS_BUCKET_NAME"))
    print(result)
