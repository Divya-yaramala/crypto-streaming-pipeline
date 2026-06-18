import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.config_manager import (
    AWSConfig,
    CryptoConfig,
    KafkaConfig,
    PostgresConfig,
    load_aws_config,
    load_crypto_config,
    load_kafka_config,
    load_postgres_config,
    validate_all_configs,
)

KAFKA_VARS = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_TOPIC_CRYPTO_PRICES": "crypto-prices",
    "KAFKA_TOPIC_CRYPTO_ALERTS": "crypto-alerts",
    "KAFKA_CONSUMER_GROUP": "test-group",
}

AWS_VARS = {
    "AWS_ACCESS_KEY_ID": "test-key",
    "AWS_SECRET_ACCESS_KEY": "test-secret",
    "AWS_BUCKET_NAME": "test-bucket",
    "AWS_REGION": "us-east-1",
}

POSTGRES_VARS = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": "user",
    "POSTGRES_PASSWORD": "pass",
    "POSTGRES_DB": "testdb",
}


def test_load_kafka_config_success():
    with patch.dict(os.environ, KAFKA_VARS):
        config = load_kafka_config()
    assert isinstance(config, KafkaConfig)
    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic_crypto_prices == "crypto-prices"


def test_load_kafka_config_missing():
    env = {k: v for k, v in KAFKA_VARS.items() if k != "KAFKA_BOOTSTRAP_SERVERS"}
    with patch.dict(os.environ, env, clear=False):
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": ""}):
            try:
                load_kafka_config()
                assert False, "Expected ValueError"
            except ValueError as e:
                assert "KAFKA_BOOTSTRAP_SERVERS" in str(e)


def test_load_aws_config_success():
    with patch.dict(os.environ, AWS_VARS):
        config = load_aws_config()
    assert isinstance(config, AWSConfig)
    assert config.bucket_name == "test-bucket"
    assert config.region == "us-east-1"


def test_load_postgres_config_success():
    with patch.dict(os.environ, POSTGRES_VARS):
        config = load_postgres_config()
    assert isinstance(config, PostgresConfig)
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.database == "testdb"


def test_load_crypto_config_defaults():
    with patch.dict(os.environ, {}, clear=False):
        config = load_crypto_config()
    assert isinstance(config, CryptoConfig)
    assert config.producer_interval_seconds == 60
    assert config.price_alert_threshold_pct == 10.0
    assert "bitcoin" in config.crypto_ids


def test_validate_all_configs_success():
    all_vars = {
        **KAFKA_VARS,
        **AWS_VARS,
        **POSTGRES_VARS,
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_pass",
    }
    with patch.dict(os.environ, all_vars):
        result = validate_all_configs()
    assert result is True
