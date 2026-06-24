import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TOTAL_INVESTMENT = 10_000.0

DEFAULT_PORTFOLIO: dict = {
    "bitcoin": 0.5,
    "ethereum": 0.3,
    "solana": 0.1,
    "cardano": 0.05,
    "dogecoin": 0.05,
}


def load_portfolio(bucket: str) -> dict:
    """Load portfolio allocations from S3; return the default portfolio on any error."""
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.get_object(Bucket=bucket, Key="portfolio/crypto/config.json")
        portfolio = json.loads(response["Body"].read().decode("utf-8"))
        logger.info("Portfolio loaded from S3: %d cryptos", len(portfolio))
        return portfolio
    except Exception as e:
        logger.warning("Could not load portfolio from S3: %s. Using default.", e)
        return dict(DEFAULT_PORTFOLIO)


def calculate_portfolio_value(portfolio: dict, current_prices: dict) -> dict:
    """Calculate current portfolio value and per-crypto weights given a $10k investment."""
    individual_values: dict = {
        crypto_id: allocation_pct * TOTAL_INVESTMENT
        for crypto_id, allocation_pct in portfolio.items()
    }
    total_value = sum(individual_values.values())
    weights: dict = {
        k: round(v / total_value * 100, 4) if total_value > 0 else 0.0
        for k, v in individual_values.items()
    }
    top_holding = (
        max(individual_values, key=lambda k: individual_values[k]) if individual_values else None
    )
    metrics = {
        "total_value": round(total_value, 2),
        "individual_values": {k: round(v, 2) for k, v in individual_values.items()},
        "weights": weights,
        "top_holding": top_holding,
        "current_prices": current_prices,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "Portfolio value: $%.2f | Top holding: %s",
        total_value,
        top_holding,
    )
    return metrics


def calculate_portfolio_returns(
    portfolio: dict,
    current_prices: dict,
    previous_prices: dict,
) -> dict:
    """Calculate per-crypto daily returns and the weighted portfolio return percentage."""
    individual_returns: dict = {}
    for crypto_id in portfolio:
        current = current_prices.get(crypto_id, 0.0)
        previous = previous_prices.get(crypto_id, 0.0)
        if previous and previous > 0:
            daily_return_pct = (current - previous) / previous * 100
        else:
            daily_return_pct = 0.0
        individual_returns[crypto_id] = round(daily_return_pct, 4)

    weighted_return = sum(portfolio.get(cid, 0.0) * ret for cid, ret in individual_returns.items())
    result = {
        "individual_returns": individual_returns,
        "weighted_portfolio_return_pct": round(weighted_return, 4),
    }
    logger.info("Portfolio weighted return: %.4f%%", weighted_return)
    return result


def save_portfolio_snapshot(metrics: dict, bucket: str, date: str) -> bool:
    """Save a portfolio metrics snapshot to S3 under portfolio/crypto/snapshots/YYYY/MM/DD/."""
    if not bucket:
        logger.warning("AWS_BUCKET_NAME not set; skipping portfolio snapshot save")
        return False
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"portfolio/crypto/snapshots/{date}/snapshot.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(metrics, default=str),
            ContentType="application/json",
        )
        logger.info("Portfolio snapshot saved to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save portfolio snapshot to S3: %s", e)
        return False


def run_portfolio_tracking() -> None:
    """Load portfolio, compute current value and daily returns, and save a snapshot."""
    bucket = os.getenv("AWS_BUCKET_NAME", "")
    portfolio = load_portfolio(bucket)

    current_prices: dict[str, Any] = {}
    previous_prices: dict[str, Any] = {}

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "crypto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
            dbname=os.getenv("POSTGRES_DB", "crypto_db"),
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (crypto_id) crypto_id, price_usd
                FROM crypto_prices
                ORDER BY crypto_id, event_timestamp DESC
                """)
            current_prices = {row[0]: float(row[1]) for row in cur.fetchall()}

            since_25h = datetime.now(timezone.utc) - timedelta(hours=25)
            until_23h = datetime.now(timezone.utc) - timedelta(hours=23)
            cur.execute(
                """
                SELECT DISTINCT ON (crypto_id) crypto_id, price_usd
                FROM crypto_prices
                WHERE event_timestamp BETWEEN %s AND %s
                ORDER BY crypto_id, event_timestamp DESC
                """,
                (since_25h, until_23h),
            )
            previous_prices = {row[0]: float(row[1]) for row in cur.fetchall()}
        conn.close()
    except Exception as e:
        logger.error("Failed to load prices from PostgreSQL: %s", e)

    metrics = calculate_portfolio_value(portfolio, current_prices)

    if previous_prices:
        returns = calculate_portfolio_returns(portfolio, current_prices, previous_prices)
        metrics["returns"] = returns

    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    save_portfolio_snapshot(metrics, bucket, today)

    logger.info(
        "Portfolio tracking complete. Total value: $%.2f | Top holding: %s",
        metrics["total_value"],
        metrics.get("top_holding"),
    )


if __name__ == "__main__":
    run_portfolio_tracking()
