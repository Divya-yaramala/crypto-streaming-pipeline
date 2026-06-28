import json
import logging
import os
from datetime import datetime, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
CRYPTOS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]


def check_data_freshness(bucket: str, crypto_id: str) -> dict:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"raw/crypto/latest/{crypto_id}.json"
        body = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(body["Body"].read())
        ts_str = data.get("timestamp", "")
        last_update = datetime.fromisoformat(ts_str)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        hours = (now - last_update).total_seconds() / 3600
        is_fresh = hours < 2
        result = {
            "crypto_id": crypto_id,
            "hours_since_update": round(hours, 3),
            "is_fresh": is_fresh,
        }
        logger.info("Freshness check for %s: %.2f hours (fresh=%s)", crypto_id, hours, is_fresh)
        return result
    except Exception as e:
        logger.error("Failed freshness check for %s: %s", crypto_id, e)
        return {"crypto_id": crypto_id, "hours_since_update": 999.0, "is_fresh": False}


def check_data_completeness(bucket: str, crypto_id: str, date: str) -> dict:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paths = [
        f"raw/crypto/{date}/{crypto_id}/",
        f"processed/aggregations/{date}/{crypto_id}.json",
        f"processed/sentiment/crypto/{date}/{crypto_id}.json",
        f"processed/technical/crypto/{date}/{crypto_id}.json",
        f"monitoring/crypto/{date}/",
    ]
    missing = []
    for prefix in paths:
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if not response.get("Contents"):
                missing.append(prefix)
        except Exception:
            missing.append(prefix)
    total = len(paths)
    found = total - len(missing)
    completeness_pct = round(found / total * 100, 1)
    result = {
        "crypto_id": crypto_id,
        "completeness_pct": completeness_pct,
        "missing": missing,
    }
    logger.info(
        "Completeness for %s: %.1f%% (%d/%d paths present)",
        crypto_id,
        completeness_pct,
        found,
        total,
    )
    return result


def check_price_anomaly(prices: list, crypto_id: str) -> dict:
    if len(prices) < 3:
        return {"crypto_id": crypto_id, "anomaly_count": 0, "anomaly_pct": 0.0}
    mean = sum(prices) / len(prices)
    variance = sum((p - mean) ** 2 for p in prices) / len(prices)
    std = variance**0.5
    if std == 0:
        anomaly_count = 0
    else:
        anomaly_count = sum(1 for p in prices if abs(p - mean) > 3 * std)
    anomaly_pct = round(anomaly_count / len(prices) * 100, 2)
    result = {
        "crypto_id": crypto_id,
        "anomaly_count": anomaly_count,
        "anomaly_pct": anomaly_pct,
    }
    logger.info(
        "Anomaly check for %s: %d anomalies (%.1f%%)", crypto_id, anomaly_count, anomaly_pct
    )
    return result


def calculate_observatory_score(freshness: dict, completeness: dict, anomaly: dict) -> float:
    f_score = 1.0 if freshness.get("is_fresh") else 0.0
    c_score = completeness.get("completeness_pct", 0.0) / 100
    a_score = 1.0 if anomaly.get("anomaly_pct", 100.0) < 5 else 0.5
    score = round(f_score * 40 + c_score * 40 + a_score * 20, 2)
    logger.info("Observatory score: %.1f/100", score)
    return score


def run_observatory_check(bucket: str) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    by_crypto: dict = {}
    scores = []
    for crypto_id in CRYPTOS:
        freshness = check_data_freshness(bucket, crypto_id)
        completeness = check_data_completeness(bucket, crypto_id, today)
        anomaly = check_price_anomaly([], crypto_id)
        score = calculate_observatory_score(freshness, completeness, anomaly)
        by_crypto[crypto_id] = {
            "freshness": freshness,
            "completeness": completeness,
            "anomaly": anomaly,
            "score": score,
        }
        scores.append(score)
    overall_score = round(sum(scores) / len(scores), 2) if scores else 0.0
    result = {"overall_score": overall_score, "by_crypto": by_crypto}
    logger.info("Observatory check complete: overall_score=%.1f", overall_score)
    return result


if __name__ == "__main__":
    pass
