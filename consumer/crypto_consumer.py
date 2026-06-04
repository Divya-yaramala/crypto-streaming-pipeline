import json
import logging
import os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CRYPTO_PRICES = os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "crypto-consumer-group")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")


def create_kafka_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        KAFKA_TOPIC_CRYPTO_PRICES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    logger.info("Kafka consumer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    return consumer


def save_to_postgres(event: dict, conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crypto_prices
                    (crypto_id, price_usd, market_cap_usd, change_24h_pct, event_timestamp, source)
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
        conn.commit()
        logger.info(
            "Saved event for %s at $%s",
            event.get("crypto_id"),
            event.get("price_usd"),
        )
        return True
    except Exception as e:
        logger.error("Failed to save event to PostgreSQL: %s", e)
        conn.rollback()
        return False


def check_price_alert(event: dict, conn) -> None:
    change = event.get("change_24h_pct")
    if change is None:
        return

    alert_type = None
    message = None

    if change > 10.0:
        alert_type = "PUMP"
        message = "Price up more than 10% in 24h"
    elif change < -10.0:
        alert_type = "DUMP"
        message = "Price down more than 10% in 24h"

    if alert_type:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO crypto_alerts (crypto_id, alert_type, message, price_usd)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        event.get("crypto_id"),
                        alert_type,
                        message,
                        event.get("price_usd"),
                    ),
                )
            conn.commit()
            logger.info(
                "Alert triggered: %s for %s (%.2f%%)",
                alert_type,
                event.get("crypto_id"),
                change,
            )
        except Exception as e:
            logger.error("Failed to insert alert: %s", e)
            conn.rollback()


def run_consumer() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    consumer = create_kafka_consumer()
    logger.info("Consumer started. Listening for messages...")
    try:
        for message in consumer:
            event = message.value
            saved = save_to_postgres(event, conn)
            if saved:
                check_price_alert(event, conn)
            logger.info(
                "Processed message: %s | partition=%d offset=%d",
                event.get("crypto_id"),
                message.partition,
                message.offset,
            )
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
