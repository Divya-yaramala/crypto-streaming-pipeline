import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2

from storage.snowflake_connector import get_snowflake_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")


def sync_prices_to_snowflake(conn, events: list) -> int:
    if not events:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for event in events:
            cur.execute(
                """
                INSERT INTO CRYPTO_PIPELINE_DB.RAW.CRYPTO_PRICES
                    (crypto_id, price_usd, market_cap_usd, change_24h_pct, event_timestamp, source)
                SELECT %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM CRYPTO_PIPELINE_DB.RAW.CRYPTO_PRICES
                    WHERE crypto_id = %s AND event_timestamp = %s
                )
                """,
                (
                    event.get("crypto_id"),
                    event.get("price_usd"),
                    event.get("market_cap_usd"),
                    event.get("change_24h_pct"),
                    event.get("event_timestamp") or event.get("timestamp"),
                    event.get("source", "coingecko"),
                    event.get("crypto_id"),
                    event.get("event_timestamp") or event.get("timestamp"),
                ),
            )
            inserted += cur.rowcount
    logger.info("Synced %d price events to Snowflake RAW.CRYPTO_PRICES", inserted)
    return inserted


def sync_alerts_to_snowflake(conn, alerts: list) -> int:
    if not alerts:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for alert in alerts:
            cur.execute(
                """
                INSERT INTO CRYPTO_PIPELINE_DB.RAW.CRYPTO_ALERTS
                    (crypto_id, alert_type, message, price_usd)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    alert.get("crypto_id"),
                    alert.get("alert_type"),
                    alert.get("message"),
                    alert.get("price_usd"),
                ),
            )
            inserted += cur.rowcount
    logger.info("Synced %d alerts to Snowflake RAW.CRYPTO_ALERTS", inserted)
    return inserted


def run_daily_mart_refresh(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO CRYPTO_PIPELINE_DB.MARTS.CRYPTO_DAILY_SUMMARY
                    (crypto_id, trade_date, avg_price, min_price, max_price,
                     total_volume, alert_count)
                SELECT
                    p.crypto_id,
                    DATE(p.event_timestamp)          AS trade_date,
                    AVG(p.price_usd)                 AS avg_price,
                    MIN(p.price_usd)                 AS min_price,
                    MAX(p.price_usd)                 AS max_price,
                    COUNT(*)                         AS total_volume,
                    COUNT(a.crypto_id)               AS alert_count
                FROM CRYPTO_PIPELINE_DB.RAW.CRYPTO_PRICES p
                LEFT JOIN CRYPTO_PIPELINE_DB.RAW.CRYPTO_ALERTS a
                    ON p.crypto_id = a.crypto_id
                    AND DATE(p.event_timestamp) = DATE(a.created_at)
                GROUP BY p.crypto_id, DATE(p.event_timestamp)
                ON CONFLICT (crypto_id, trade_date) DO UPDATE SET
                    avg_price    = EXCLUDED.avg_price,
                    min_price    = EXCLUDED.min_price,
                    max_price    = EXCLUDED.max_price,
                    total_volume = EXCLUDED.total_volume,
                    alert_count  = EXCLUDED.alert_count
                """)
        logger.info("Daily mart refresh complete for MARTS.CRYPTO_DAILY_SUMMARY")
        return True
    except Exception as e:
        logger.error("Daily mart refresh failed: %s", e)
        return False


def _fetch_recent_prices_from_postgres(pg_conn, since: datetime) -> list:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT crypto_id, price_usd, market_cap_usd, change_24h_pct,
                   event_timestamp, source
            FROM crypto_prices
            WHERE event_timestamp >= %s
            ORDER BY event_timestamp
            """,
            (since,),
        )
        rows = cur.fetchall()
    return [
        {
            "crypto_id": r[0],
            "price_usd": r[1],
            "market_cap_usd": r[2],
            "change_24h_pct": r[3],
            "event_timestamp": r[4],
            "source": r[5],
        }
        for r in rows
    ]


def _fetch_recent_alerts_from_postgres(pg_conn, since: datetime) -> list:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT crypto_id, alert_type, message, price_usd
            FROM crypto_alerts
            WHERE created_at >= %s
            ORDER BY created_at
            """,
            (since,),
        )
        rows = cur.fetchall()
    return [
        {
            "crypto_id": r[0],
            "alert_type": r[1],
            "message": r[2],
            "price_usd": r[3],
        }
        for r in rows
    ]


def run_snowflake_sync() -> None:
    since = datetime.now(timezone.utc) - timedelta(hours=24)

    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    sf_conn = get_snowflake_connection()

    try:
        prices = _fetch_recent_prices_from_postgres(pg_conn, since)
        alerts = _fetch_recent_alerts_from_postgres(pg_conn, since)

        prices_synced = sync_prices_to_snowflake(sf_conn, prices)
        alerts_synced = sync_alerts_to_snowflake(sf_conn, alerts)
        mart_ok = run_daily_mart_refresh(sf_conn)

        logger.info(
            "Snowflake sync complete: %d prices, %d alerts synced, mart refresh=%s",
            prices_synced,
            alerts_synced,
            "OK" if mart_ok else "FAILED",
        )
    finally:
        pg_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    run_snowflake_sync()
