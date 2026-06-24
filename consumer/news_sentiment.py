import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

CRYPTO_COMPANIES = {
    "bitcoin": "Bitcoin BTC cryptocurrency",
    "ethereum": "Ethereum ETH cryptocurrency",
    "solana": "Solana SOL cryptocurrency",
    "cardano": "Cardano ADA cryptocurrency",
    "dogecoin": "Dogecoin DOGE cryptocurrency",
}

POSITIVE_KEYWORDS = {"bullish", "surge", "rise", "gain", "up", "record", "buy", "pump", "moon"}
NEGATIVE_KEYWORDS = {"bearish", "crash", "fall", "drop", "down", "sell", "dump", "ban", "hack"}


def fetch_crypto_news(crypto_id: str) -> list:
    try:
        if NEWS_API_KEY:
            query = CRYPTO_COMPANIES.get(crypto_id, crypto_id)
            url = "https://newsapi.org/v2/everything"
            params = {"q": query, "apiKey": NEWS_API_KEY, "language": "en", "pageSize": 20}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            headlines = [
                article["title"]
                for article in data.get("articles", [])
                if article.get("title")
            ]
        else:
            url = "https://api.coingecko.com/api/v3/news"
            params = {"category": "crypto"}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            items = data if isinstance(data, list) else data.get("data", [])
            headlines = [item.get("title", "") for item in items if item.get("title")]

        logger.info("Fetched %d headlines for %s", len(headlines), crypto_id)
        return headlines
    except Exception as e:
        logger.error("Failed to fetch news for %s: %s", crypto_id, e)
        return []


def analyze_sentiment(headlines: list, crypto_id: str) -> dict:
    positive_count = 0
    negative_count = 0
    neutral_count = 0

    for headline in headlines:
        text = headline.lower()
        has_positive = any(kw in text for kw in POSITIVE_KEYWORDS)
        has_negative = any(kw in text for kw in NEGATIVE_KEYWORDS)
        if has_positive and not has_negative:
            positive_count += 1
        elif has_negative and not has_positive:
            negative_count += 1
        else:
            neutral_count += 1

    total = len(headlines)
    sentiment_score = (positive_count - negative_count) / max(total, 1) * 100

    if sentiment_score > 20:
        sentiment_label = "BULLISH"
    elif sentiment_score < -20:
        sentiment_label = "BEARISH"
    else:
        sentiment_label = "NEUTRAL"

    logger.info("%s sentiment: %s (score=%.1f)", crypto_id, sentiment_label, sentiment_score)

    return {
        "crypto_id": crypto_id,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "total_headlines": total,
        "sentiment_score": round(sentiment_score, 2),
        "sentiment_label": sentiment_label,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


def save_sentiment_to_s3(
    sentiment: dict,
    crypto_id: str,
    bucket: str,
    date: str,
) -> bool:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"processed/sentiment/crypto/{date}/{crypto_id}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(sentiment, default=str),
            ContentType="application/json",
        )
        logger.info("Sentiment saved to S3: %s", key)
        return True
    except Exception as e:
        logger.error("Failed to save sentiment to S3: %s", e)
        return False


def run_crypto_sentiment() -> None:
    bucket = os.getenv("AWS_BUCKET_NAME", "")
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    counts = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}

    for crypto_id in CRYPTO_COMPANIES:
        headlines = fetch_crypto_news(crypto_id)
        sentiment = analyze_sentiment(headlines, crypto_id)
        label = sentiment["sentiment_label"]
        counts[label] += 1
        if bucket:
            save_sentiment_to_s3(sentiment, crypto_id, bucket, today)

    logger.info(
        "Sentiment summary: %d BULLISH, %d BEARISH, %d NEUTRAL",
        counts["BULLISH"],
        counts["BEARISH"],
        counts["NEUTRAL"],
    )


if __name__ == "__main__":
    run_crypto_sentiment()
