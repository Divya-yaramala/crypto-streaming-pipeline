import logging
import os

import psycopg2

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


def get_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    logger.info("Connected to PostgreSQL database '%s'", POSTGRES_DB)
    return conn


def create_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id                SERIAL PRIMARY KEY,
                crypto_id         VARCHAR(50) NOT NULL,
                price_usd         NUMERIC(20, 8),
                market_cap_usd    NUMERIC(30, 2),
                change_24h_pct    NUMERIC(10, 4),
                event_timestamp   TIMESTAMP NOT NULL,
                source            VARCHAR(50),
                ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (crypto_id, event_timestamp)
            )
            """
        )
        logger.info("Table 'crypto_prices' ready")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_alerts (
                id          SERIAL PRIMARY KEY,
                crypto_id   VARCHAR(50),
                alert_type  VARCHAR(50),
                message     TEXT,
                price_usd   NUMERIC(20, 8),
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        logger.info("Table 'crypto_alerts' ready")

    conn.commit()


def setup_database() -> None:
    conn = get_connection()
    try:
        create_tables(conn)
        logger.info("Database setup complete")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()
