import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def load_price_series(
    crypto_ids: list,
    bucket: str,
    days: int = 30,
) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    today = datetime.now(timezone.utc)
    records: dict = {}

    for i in range(days):
        day = today - timedelta(days=i)
        date_str = day.strftime("%Y/%m/%d")
        date_key = day.strftime("%Y-%m-%d")
        records[date_key] = {}

        for crypto_id in crypto_ids:
            try:
                key = f"processed/prices/crypto/{date_str}/{crypto_id}.json"
                response = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))
                records[date_key][crypto_id] = float(data.get("price_usd", float("nan")))
            except Exception:
                records[date_key][crypto_id] = float("nan")

    df = pd.DataFrame.from_dict(records, orient="index")
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    logger.info("Loaded price series: %d days x %d cryptos", len(df), len(crypto_ids))
    return df


def calculate_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    corr_matrix = df.corr(method="pearson")

    pairs = [
        (corr_matrix.columns[i], corr_matrix.columns[j], corr_matrix.iloc[i, j])
        for i in range(len(corr_matrix.columns))
        for j in range(i + 1, len(corr_matrix.columns))
    ]
    if pairs:
        pairs_sorted = sorted(pairs, key=lambda x: x[2])
        low_pair = pairs_sorted[0]
        high_pair = pairs_sorted[-1]
        logger.info(
            "Highest correlation: %s/%s=%.4f | Lowest: %s/%s=%.4f",
            high_pair[0],
            high_pair[1],
            high_pair[2],
            low_pair[0],
            low_pair[1],
            low_pair[2],
        )

    return corr_matrix


def find_highly_correlated(
    corr_matrix: pd.DataFrame,
    threshold: float = 0.8,
) -> list:
    pairs = []
    cols = corr_matrix.columns.tolist()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            corr_val = corr_matrix.iloc[i, j]
            if corr_val > threshold:
                pairs.append(
                    {
                        "crypto1": cols[i],
                        "crypto2": cols[j],
                        "correlation": round(float(corr_val), 4),
                    }
                )
    return pairs


def calculate_beta(
    crypto_id: str,
    market_proxy: str,
    prices: pd.DataFrame,
) -> float:
    if crypto_id not in prices.columns or market_proxy not in prices.columns:
        logger.warning("Missing columns for beta calculation: %s vs %s", crypto_id, market_proxy)
        return 0.0

    crypto_returns = prices[crypto_id].pct_change().dropna()
    market_returns = prices[market_proxy].pct_change().dropna()

    aligned = pd.concat([crypto_returns, market_returns], axis=1).dropna()
    if len(aligned) < 2:
        logger.warning("Insufficient data to calculate beta for %s", crypto_id)
        return 0.0

    cov = np.cov(aligned.iloc[:, 0].values, aligned.iloc[:, 1].values)
    market_var = cov[1, 1]
    if market_var == 0:
        return 0.0

    beta = float(cov[0, 1] / market_var)
    logger.info("Beta for %s vs %s: %.4f", crypto_id, market_proxy, beta)
    return round(beta, 4)


def run_correlation_analysis(bucket: str) -> dict:
    crypto_ids = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

    prices = load_price_series(crypto_ids, bucket, days=30)
    corr_matrix = calculate_correlation_matrix(prices)
    high_corr_pairs = find_highly_correlated(corr_matrix, threshold=0.8)

    betas: dict = {}
    for crypto_id in crypto_ids:
        if crypto_id != "bitcoin":
            betas[crypto_id] = calculate_beta(crypto_id, "bitcoin", prices)

    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    results = {
        "date": today,
        "correlation_matrix": corr_matrix.round(4).to_dict(),
        "highly_correlated_pairs": high_corr_pairs,
        "betas_vs_bitcoin": betas,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }

    if bucket:
        try:
            s3 = boto3.client("s3", region_name=AWS_REGION)
            key = f"processed/correlation/crypto/{today}/correlation.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(results, default=str),
                ContentType="application/json",
            )
            logger.info("Correlation results saved to S3: %s", key)
        except Exception as e:
            logger.error("Failed to save correlation results to S3: %s", e)

    return results


if __name__ == "__main__":
    pass
