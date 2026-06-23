import json
import logging
import os
from datetime import datetime, timezone

import boto3
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def calculate_sma(prices: list, window: int) -> list:
    """Calculate Simple Moving Average for the given window size."""
    if len(prices) < window:
        logger.warning("Not enough data for SMA: need %d, got %d", window, len(prices))
        return []
    arr = np.array(prices, dtype=float)
    sma = [round(float(np.mean(arr[i : i + window])), 6) for i in range(len(arr) - window + 1)]
    logger.info("SMA(%d) calculated: %d values", window, len(sma))
    return sma


def calculate_rsi(prices: list, period: int = 14) -> list:
    """Calculate Relative Strength Index over the given period."""
    if len(prices) < period + 1:
        logger.warning("Not enough data for RSI: need %d, got %d", period + 1, len(prices))
        return []
    arr = np.array(prices, dtype=float)
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    rsi_values: list = []
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_values.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_values.append(round(100.0 - 100.0 / (1.0 + rs), 4))

    logger.info("RSI(%d) calculated: %d values", period, len(rsi_values))
    return rsi_values


def calculate_bollinger_bands(prices: list, window: int = 20, num_std: float = 2.0) -> dict:
    """Calculate Bollinger Bands (upper, middle, lower) for the given window and std multiplier."""
    if len(prices) < window:
        logger.warning("Not enough data for Bollinger Bands: need %d, got %d", window, len(prices))
        return {"upper": [], "middle": [], "lower": []}
    arr = np.array(prices, dtype=float)
    upper, middle, lower = [], [], []
    for i in range(window - 1, len(arr)):
        window_slice = arr[i - window + 1 : i + 1]
        mean = float(np.mean(window_slice))
        std = float(np.std(window_slice, ddof=1))
        middle.append(round(mean, 6))
        upper.append(round(mean + num_std * std, 6))
        lower.append(round(mean - num_std * std, 6))
    logger.info("Bollinger Bands(%d, %.1f) calculated: %d bands", window, num_std, len(middle))
    return {"upper": upper, "middle": middle, "lower": lower}


def calculate_macd(
    prices: list,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict:
    """Calculate MACD line, signal line, and histogram."""
    if len(prices) < slow + signal:
        logger.warning("Not enough data for MACD: need %d, got %d", slow + signal, len(prices))
        return {"macd": [], "signal": [], "histogram": []}
    arr = np.array(prices, dtype=float)

    def _ema(data: np.ndarray, span: int) -> np.ndarray:
        alpha = 2.0 / (span + 1)
        ema = np.zeros(len(data))
        ema[0] = data[0]
        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        return ema

    fast_ema = _ema(arr, fast)
    slow_ema = _ema(arr, slow)
    macd_line = fast_ema - slow_ema
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line

    result = {
        "macd": [round(float(v), 6) for v in macd_line],
        "signal": [round(float(v), 6) for v in signal_line],
        "histogram": [round(float(v), 6) for v in histogram],
    }
    logger.info("MACD(%d,%d,%d) calculated: %d values", fast, slow, signal, len(macd_line))
    return result


def run_technical_analysis(
    crypto_id: str,
    prices: list,
    bucket: str,
) -> dict:
    """Compute SMA-20, RSI-14, Bollinger Bands-20, and MACD, then save results to S3."""
    results: dict = {
        "crypto_id": crypto_id,
        "price_count": len(prices),
        "calculated_at": datetime.now(timezone.utc).isoformat(),
        "sma_20": calculate_sma(prices, window=20),
        "rsi_14": calculate_rsi(prices, period=14),
        "bollinger_bands_20": calculate_bollinger_bands(prices, window=20),
        "macd": calculate_macd(prices),
    }

    if bucket:
        try:
            s3 = boto3.client("s3", region_name=AWS_REGION)
            today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
            key = f"analytics/crypto/{today}/{crypto_id}/technical_indicators.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(results, default=str),
                ContentType="application/json",
            )
            logger.info("Technical analysis saved to S3: %s", key)
        except Exception as e:
            logger.error("Failed to save technical analysis to S3: %s", e)

    logger.info(
        "Technical analysis complete for %s: SMA=%d, RSI=%d values",
        crypto_id,
        len(results["sma_20"]),
        len(results["rsi_14"]),
    )
    return results


if __name__ == "__main__":
    import psycopg2

    crypto_id = os.getenv("CRYPTO_ID", "bitcoin")
    bucket = os.getenv("AWS_BUCKET_NAME", "")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "crypto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
            dbname=os.getenv("POSTGRES_DB", "crypto_db"),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT price_usd FROM crypto_prices
                WHERE crypto_id = %s
                ORDER BY event_timestamp DESC
                LIMIT 200
                """,
                (crypto_id,),
            )
            prices = [float(row[0]) for row in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.error("Failed to load prices: %s", e)
        prices = []

    if prices:
        run_technical_analysis(crypto_id, list(reversed(prices)), bucket)
    else:
        logger.warning("No prices found for %s", crypto_id)
