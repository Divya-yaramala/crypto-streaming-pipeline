import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def _load_alert_history(s3, bucket: str, date: str) -> list:
    key = f"monitoring/alerts/history/{date}/alerts.json"
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def aggregate_alerts_by_hour(bucket: str, date: str) -> dict:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        alerts = _load_alert_history(s3, bucket, date)

        hourly: dict = {}
        for alert in alerts:
            triggered_at = alert.get("triggered_at", "")
            try:
                dt = datetime.fromisoformat(triggered_at)
                hour = dt.strftime("%H")
                hourly[hour] = hourly.get(hour, 0) + 1
            except Exception:
                pass

        if hourly:
            peak_hour = max(hourly, key=lambda h: hourly[h])
            logger.info("Peak alert hour: %s (%d alerts)", peak_hour, hourly[peak_hour])

        return hourly
    except Exception as e:
        logger.error("Failed to aggregate alerts by hour: %s", e)
        return {}


def find_alert_patterns(bucket: str, days: int = 7) -> dict:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    today = datetime.now(timezone.utc)
    all_alerts: list = []
    daily_counts: dict = {}

    for i in range(days):
        day = today - timedelta(days=i)
        date_str = day.strftime("%Y/%m/%d")
        date_key = day.strftime("%Y-%m-%d")
        try:
            alerts = _load_alert_history(s3, bucket, date_str)
            all_alerts.extend(alerts)
            daily_counts[date_key] = len(alerts)
        except Exception:
            daily_counts[date_key] = 0

    crypto_counts: dict = {}
    rule_counts: dict = {}
    for alert in all_alerts:
        crypto_id = alert.get("crypto_id", "unknown")
        rule_id = alert.get("rule_id", "unknown")
        crypto_counts[crypto_id] = crypto_counts.get(crypto_id, 0) + 1
        rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1

    most_alerted_crypto = (
        max(crypto_counts, key=lambda k: crypto_counts[k]) if crypto_counts else None
    )
    most_common_rule = max(rule_counts, key=lambda k: rule_counts[k]) if rule_counts else None
    peak_alert_day = max(daily_counts, key=lambda k: daily_counts[k]) if daily_counts else None

    logger.info(
        "Alert patterns: top crypto=%s, top rule=%s, peak day=%s",
        most_alerted_crypto,
        most_common_rule,
        peak_alert_day,
    )

    return {
        "most_alerted_crypto": most_alerted_crypto,
        "most_common_rule": most_common_rule,
        "peak_alert_day": peak_alert_day,
        "crypto_counts": crypto_counts,
        "rule_counts": rule_counts,
        "daily_counts": daily_counts,
    }


def generate_alert_digest(bucket: str, date: str) -> str:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        alerts = _load_alert_history(s3, bucket, date)

        total = len(alerts)
        severity_counts: dict = {}
        for alert in alerts:
            severity = alert.get("severity", "UNKNOWN")
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        critical = severity_counts.get("CRITICAL", 0)
        high = severity_counts.get("HIGH", 0)
        digest = f"Today: {total} alerts ({critical} CRITICAL, {high} HIGH)"
        logger.info("Alert digest generated: %s", digest)
        return digest
    except Exception as e:
        logger.error("Failed to generate alert digest: %s", e)
        return "Today: 0 alerts (0 CRITICAL, 0 HIGH)"


def send_daily_alert_digest(bucket: str, date: str, webhook_url: str) -> bool:
    digest = generate_alert_digest(bucket, date)
    if not webhook_url:
        logger.warning("No webhook URL set, skipping digest send")
        return False
    try:
        payload = {
            "attachments": [
                {
                    "color": "good",
                    "text": f"Daily Alert Digest\n{digest}",
                    "ts": datetime.now(timezone.utc).timestamp(),
                }
            ]
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Daily alert digest sent")
        return True
    except Exception as e:
        logger.error("Failed to send daily alert digest: %s", e)
        return False


def suppress_duplicate_alerts(
    alert: dict,
    bucket: str,
    suppress_minutes: int = 30,
) -> bool:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        history = _load_alert_history(s3, bucket, today)

        cutoff = datetime.now(timezone.utc).timestamp() - suppress_minutes * 60
        for past in history:
            if past.get("rule_id") == alert.get("rule_id") and past.get("crypto_id") == alert.get(
                "crypto_id"
            ):
                try:
                    past_ts = datetime.fromisoformat(past["triggered_at"]).timestamp()
                    if past_ts > cutoff:
                        logger.info(
                            "Suppressing duplicate %s alert for %s",
                            alert.get("rule_id"),
                            alert.get("crypto_id"),
                        )
                        return True
                except Exception:
                    pass
        return False
    except Exception:
        return False


if __name__ == "__main__":
    pass
