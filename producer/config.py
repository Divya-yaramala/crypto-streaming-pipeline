import os

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CRYPTO_PRICES = os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices")
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
