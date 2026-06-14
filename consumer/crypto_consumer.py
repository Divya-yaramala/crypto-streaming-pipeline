import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer

from consumer import data_validator, dead_letter_queue, pipeline_monitor, slack_alerter
from storage import s3_storage

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

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")


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


def check_price_alert(event: dict, conn) -> bool:
    change = event.get("change_24h_pct")
    if change is None:
        return False

    alert_type = None
    alert_message = None

    if change > 10.0:
        alert_type = "PUMP"
        alert_message = "Price up more than 10% in 24h"
    elif change < -10.0:
        alert_type = "DUMP"
        alert_message = "Price down more than 10% in 24h"

    if alert_type:
        crypto_id = event.get("crypto_id", "unknown")
        price = event.get("price_usd") or 0.0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO crypto_alerts (crypto_id, alert_type, message, price_usd)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (crypto_id, alert_type, alert_message, price),
                )
            conn.commit()
            logger.info("Alert triggered: %s for %s (%.2f%%)", alert_type, crypto_id, change)
            if alert_type == "PUMP":
                slack_alerter.alert_price_pump(crypto_id, change, price)
            else:
                slack_alerter.alert_price_dump(crypto_id, change, price)
        except Exception as e:
            logger.error("Failed to insert alert: %s", e)
            conn.rollback()
            slack_alerter.alert_pipeline_error("check_price_alert", str(e))

        alert_dict = {
            "crypto_id": crypto_id,
            "alert_type": alert_type,
            "message": alert_message,
            "price_usd": price,
        }
        s3_result = s3_storage.save_alert_to_s3(alert_dict, AWS_BUCKET_NAME)
        logger.info("S3 alert save: %s", "OK" if s3_result else "FAILED")
        return True

    return False


def _save_invalid_event_to_s3(event: dict, errors: list) -> None:
    if not AWS_BUCKET_NAME:
        return
    try:
        s3 = s3_storage.get_s3_client()
        now = datetime.now(timezone.utc)
        crypto_id = event.get("crypto_id", "unknown")
        key = (
            f"errors/{now.strftime('%Y/%m/%d')}"
            f"/{crypto_id}/{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        )
        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=key,
            Body=json.dumps({"event": event, "errors": errors}, default=str),
            ContentType="application/json",
        )
        logger.info("Saved invalid event to S3: %s", key)
    except Exception as e:
        logger.error("Failed to save invalid event to S3: %s", e)


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

    total_count = 0
    valid_count = 0
    invalid_count = 0
    alerts_triggered = 0
    dlq_count = 0

    try:
        for message in consumer:
            event = message.value
            total_count += 1
            crypto_id = event.get("crypto_id", "unknown")

            consume_start = time.time()
            validation = data_validator.validate_price_event(event)
            validate_duration = time.time() - consume_start

            if not validation["valid"]:
                invalid_count += 1
                logger.warning(
                    "Invalid event for %s: %s",
                    crypto_id,
                    validation["errors"],
                )
                _save_invalid_event_to_s3(event, validation["errors"])
                if dead_letter_queue.send_to_dlq(
                    event, str(validation["errors"]), "validate", AWS_BUCKET_NAME
                ):
                    dlq_count += 1
                pipeline_monitor.record_event_metric(
                    "validate", crypto_id, "failure", validate_duration, AWS_BUCKET_NAME
                )
            else:
                pipeline_monitor.record_event_metric(
                    "validate", crypto_id, "success", validate_duration, AWS_BUCKET_NAME
                )
                valid_count += 1

                pg_start = time.time()
                saved = save_to_postgres(event, conn)
                pg_duration = time.time() - pg_start
                if not saved:
                    pipeline_monitor.record_event_metric(
                        "postgres", crypto_id, "failure", pg_duration, AWS_BUCKET_NAME
                    )
                    if dead_letter_queue.send_to_dlq(
                        event, "postgres insert failed", "postgres", AWS_BUCKET_NAME
                    ):
                        dlq_count += 1
                else:
                    pipeline_monitor.record_event_metric(
                        "postgres", crypto_id, "success", pg_duration, AWS_BUCKET_NAME
                    )

                    s3_start = time.time()
                    s3_result = s3_storage.save_price_event_to_s3(event, AWS_BUCKET_NAME)
                    s3_duration = time.time() - s3_start
                    logger.info("S3 price event save: %s", "OK" if s3_result else "FAILED")
                    pipeline_monitor.record_event_metric(
                        "s3",
                        crypto_id,
                        "success" if s3_result else "failure",
                        s3_duration,
                        AWS_BUCKET_NAME,
                    )
                    if not s3_result:
                        if dead_letter_queue.send_to_dlq(
                            event, "s3 save failed", "s3", AWS_BUCKET_NAME
                        ):
                            dlq_count += 1

                    try:
                        alert_start = time.time()
                        if check_price_alert(event, conn):
                            alerts_triggered += 1
                        alert_duration = time.time() - alert_start
                        pipeline_monitor.record_event_metric(
                            "alert", crypto_id, "success", alert_duration, AWS_BUCKET_NAME
                        )
                    except Exception as alert_err:
                        logger.error("Alert step failed: %s", alert_err)
                        pipeline_monitor.record_event_metric(
                            "alert", crypto_id, "failure", 0.0, AWS_BUCKET_NAME
                        )
                        if dead_letter_queue.send_to_dlq(
                            event, str(alert_err), "alert", AWS_BUCKET_NAME
                        ):
                            dlq_count += 1

            pipeline_monitor.record_event_metric(
                "consume", crypto_id, "success", time.time() - consume_start, AWS_BUCKET_NAME
            )

            logger.info(
                "Processed message: %s | partition=%d offset=%d",
                event.get("crypto_id"),
                message.partition,
                message.offset,
            )

            if total_count % 10 == 0:
                logger.info(
                    "Validation metrics: %d valid, %d invalid out of %d total events",
                    valid_count,
                    invalid_count,
                    total_count,
                )

    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        quality_score = (valid_count / total_count * 100) if total_count > 0 else 0.0
        logger.info(
            "Final metrics: %d valid, %d invalid, %d DLQ out of %d total events",
            valid_count,
            invalid_count,
            dlq_count,
            total_count,
        )
        logger.info("%d events sent to DLQ today", dlq_count)
        slack_alerter.send_daily_summary(total_count, valid_count, alerts_triggered, quality_score)
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        pipeline_monitor.generate_daily_report(AWS_BUCKET_NAME, today)
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
