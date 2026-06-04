import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CRYPTO_PRICES = os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "crypto-consumer-group")
PRICE_ALERT_THRESHOLD_PCT = float(os.getenv("PRICE_ALERT_THRESHOLD_PCT", "10.0"))

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
