import logging
import os
from dataclasses import dataclass
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic_crypto_prices: str
    topic_crypto_alerts: str
    consumer_group: str


@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    region: str = "us-east-1"


@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str


@dataclass
class CryptoConfig:
    crypto_ids: List[str]
    coingecko_base_url: str
    producer_interval_seconds: int = 60
    price_alert_threshold_pct: float = 10.0


def load_kafka_config() -> KafkaConfig:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required but not set")
    config = KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        topic_crypto_prices=os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices"),
        topic_crypto_alerts=os.getenv("KAFKA_TOPIC_CRYPTO_ALERTS", "crypto-alerts"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "crypto-consumer-group"),
    )
    logger.info("Kafka config loaded: bootstrap_servers=%s", config.bootstrap_servers)
    return config


def load_aws_config() -> AWSConfig:
    bucket_name = os.getenv("AWS_BUCKET_NAME", "")
    if not bucket_name:
        raise ValueError("AWS_BUCKET_NAME is required but not set")
    config = AWSConfig(
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        bucket_name=bucket_name,
        region=os.getenv("AWS_REGION", "us-east-1"),
    )
    logger.info("AWS config loaded: bucket=%s region=%s", config.bucket_name, config.region)
    return config


def load_postgres_config() -> PostgresConfig:
    host = os.getenv("POSTGRES_HOST", "")
    if not host:
        raise ValueError("POSTGRES_HOST is required but not set")
    config = PostgresConfig(
        host=host,
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "crypto_user"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
        database=os.getenv("POSTGRES_DB", "crypto_db"),
    )
    logger.info("Postgres config loaded: host=%s db=%s", config.host, config.database)
    return config


def load_snowflake_config() -> SnowflakeConfig:
    account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    if not account:
        raise ValueError("SNOWFLAKE_ACCOUNT is required but not set")
    config = SnowflakeConfig(
        account=account,
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "CRYPTO_PIPELINE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_PIPELINE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "MARTS"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    logger.info(
        "Snowflake config loaded: account=%s warehouse=%s", config.account, config.warehouse
    )
    return config


def load_crypto_config() -> CryptoConfig:
    raw_ids = os.getenv("CRYPTO_IDS", "bitcoin,ethereum,solana,cardano,dogecoin")
    config = CryptoConfig(
        crypto_ids=[c.strip() for c in raw_ids.split(",")],
        coingecko_base_url=os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
        producer_interval_seconds=int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60")),
        price_alert_threshold_pct=float(os.getenv("PRICE_ALERT_THRESHOLD_PCT", "10.0")),
    )
    logger.info(
        "Crypto config loaded: %d cryptos, interval=%ds",
        len(config.crypto_ids),
        config.producer_interval_seconds,
    )
    return config


def validate_all_configs() -> bool:
    loaders = {
        "kafka": load_kafka_config,
        "aws": load_aws_config,
        "postgres": load_postgres_config,
        "snowflake": load_snowflake_config,
        "crypto": load_crypto_config,
    }
    all_ok = True
    for name, loader in loaders.items():
        try:
            loader()
            logger.info("Config OK: %s", name)
        except Exception as e:
            logger.error("Config FAILED: %s — %s", name, e)
            all_ok = False
    return all_ok
