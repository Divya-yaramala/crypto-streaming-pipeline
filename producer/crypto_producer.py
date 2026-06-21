import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CRYPTO_PRICES = os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices")
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]


def fetch_crypto_prices(crypto_ids: list) -> dict:
    """Fetch current USD prices for the given crypto IDs from CoinGecko."""
    ids_param = ",".join(crypto_ids)
    url = (
        f"{COINGECKO_BASE_URL}/simple/price"
        f"?ids={ids_param}&vs_currencies=usd"
        f"&include_24hr_change=true&include_market_cap=true"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched prices for %d cryptos", len(data))
        return data
    except requests.exceptions.Timeout:
        logger.error("Request to CoinGecko timed out")
        return {}
    except (requests.exceptions.ConnectionError, ConnectionError) as e:
        logger.error("Connection error fetching crypto prices: %s", e)
        return {}
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch crypto prices: %s", e)
        return {}


def create_kafka_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer instance."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    return producer


def publish_price_event(producer: KafkaProducer, crypto_id: str, price_data: dict) -> bool:
    """Build and publish a price event for one crypto to the Kafka topic."""
    try:
        event = {
            "crypto_id": crypto_id,
            "price_usd": price_data.get("usd"),
            "market_cap_usd": price_data.get("usd_market_cap"),
            "change_24h_pct": price_data.get("usd_24h_change"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "coingecko",
        }
        producer.send(KAFKA_TOPIC_CRYPTO_PRICES, value=event)
        producer.flush()
        logger.info("Published event for %s at $%.2f", crypto_id, event["price_usd"] or 0)
        return True
    except KafkaError as e:
        logger.error("Kafka error publishing event for %s: %s", crypto_id, e)
        return False
    except Exception as e:
        logger.error("Unexpected error publishing event for %s: %s", crypto_id, e)
        return False


def run_producer(interval_seconds: int = 60) -> None:
    """Run the polling loop, fetching and publishing crypto prices at regular intervals."""
    producer = create_kafka_producer()
    logger.info("Producer started. Polling every %d seconds.", interval_seconds)
    try:
        while True:
            prices = fetch_crypto_prices(CRYPTO_IDS)
            published = 0
            for crypto_id in CRYPTO_IDS:
                if crypto_id in prices:
                    success = publish_price_event(producer, crypto_id, prices[crypto_id])
                    if success:
                        published += 1
            logger.info("Summary: %d/%d prices published", published, len(CRYPTO_IDS))
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
