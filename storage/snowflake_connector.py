import logging
import os

import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "CRYPTO_PIPELINE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_PIPELINE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "MARTS")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")


def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        login_timeout=30,
    )
    logger.info("Connected to Snowflake account '%s'", SNOWFLAKE_ACCOUNT)
    return conn


def create_snowflake_objects(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS CRYPTO_PIPELINE_DB")
        logger.info("Database CRYPTO_PIPELINE_DB ready")

        cur.execute("CREATE SCHEMA IF NOT EXISTS CRYPTO_PIPELINE_DB.RAW")
        logger.info("Schema RAW ready")

        cur.execute("CREATE SCHEMA IF NOT EXISTS CRYPTO_PIPELINE_DB.MARTS")
        logger.info("Schema MARTS ready")

        cur.execute(
            """
            CREATE WAREHOUSE IF NOT EXISTS CRYPTO_PIPELINE_WH
            WITH WAREHOUSE_SIZE = 'X-SMALL'
                 AUTO_SUSPEND = 60
                 AUTO_RESUME = TRUE
            """
        )
        logger.info("Warehouse CRYPTO_PIPELINE_WH ready")


def create_snowflake_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS CRYPTO_PIPELINE_DB.RAW.CRYPTO_PRICES (
                crypto_id       VARCHAR(50),
                price_usd       NUMERIC(20, 8),
                market_cap_usd  NUMERIC(30, 2),
                change_24h_pct  NUMERIC(10, 4),
                event_timestamp TIMESTAMP,
                source          VARCHAR(50),
                ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UNIQUE (crypto_id, event_timestamp)
            )
            """
        )
        logger.info("Table RAW.CRYPTO_PRICES ready")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS CRYPTO_PIPELINE_DB.RAW.CRYPTO_ALERTS (
                crypto_id  VARCHAR(50),
                alert_type VARCHAR(50),
                message    TEXT,
                price_usd  NUMERIC(20, 8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        logger.info("Table RAW.CRYPTO_ALERTS ready")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS CRYPTO_PIPELINE_DB.MARTS.CRYPTO_DAILY_SUMMARY (
                crypto_id    VARCHAR(50),
                trade_date   DATE,
                avg_price    NUMERIC(20, 8),
                min_price    NUMERIC(20, 8),
                max_price    NUMERIC(20, 8),
                total_volume BIGINT,
                alert_count  INTEGER,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UNIQUE (crypto_id, trade_date)
            )
            """
        )
        logger.info("Table MARTS.CRYPTO_DAILY_SUMMARY ready")


def setup_snowflake() -> None:
    conn = get_snowflake_connection()
    try:
        create_snowflake_objects(conn)
        create_snowflake_tables(conn)
        logger.info("Snowflake setup complete")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_snowflake()
