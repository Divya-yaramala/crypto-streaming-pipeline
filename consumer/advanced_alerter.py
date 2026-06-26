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

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

ALERT_RULES = [
    {
        "rule_id": "A001",
        "name": "extreme_pump",
        "metric": "change_24h_pct",
        "operator": ">",
        "threshold": 20.0,
        "severity": "CRITICAL",
    },
    {
        "rule_id": "A002",
        "name": "extreme_dump",
        "metric": "change_24h_pct",
        "operator": "<",
        "threshold": -20.0,
        "severity": "CRITICAL",
    },
    {
        "rule_id": "A003",
        "name": "moderate_pump",
        "metric": "change_24h_pct",
        "operator": ">",
        "threshold": 10.0,
        "severity": "HIGH",
    },
    {
        "rule_id": "A004",
        "name": "moderate_dump",
        "metric": "change_24h_pct",
        "operator": "<",
        "threshold": -10.0,
        "severity": "HIGH",
    },
    {
        "rule_id": "A005",
        "name": "high_volume",
        "metric": "volume_ratio",
        "operator": ">",
        "threshold": 2.0,
        "severity": "MEDIUM",
    },
    {
        "rule_id": "A006",
        "name": "price_above_sma",
        "metric": "price_vs_sma",
        "operator": ">",
        "threshold": 5.0,
        "severity": "LOW",
    },
]

_OPERATORS = {
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
}

_SEVERITY_COLORS = {
    "CRITICAL": "danger",
    "HIGH": "warning",
    "MEDIUM": "#439FE0",
    "LOW": "good",
}


def evaluate_alert_rules(event: dict) -> list:
    triggered = []
    for rule in ALERT_RULES:
        metric = rule["metric"]
        value = event.get(metric)
        if value is None:
            continue
        op_fn = _OPERATORS.get(rule["operator"])
        if op_fn and op_fn(float(value), rule["threshold"]):
            alert = {
                **rule,
                "crypto_id": event.get("crypto_id", "unknown"),
                "triggered_value": value,
                "triggered_at": datetime.now(timezone.utc).isoformat(),
            }
            triggered.append(alert)
            logger.info(
                "Alert triggered: %s (%s) for %s — %s=%s",
                rule["name"],
                rule["severity"],
                event.get("crypto_id"),
                metric,
                value,
            )
    return triggered


def send_tiered_alert(rule: dict, event: dict, webhook_url: str) -> bool:
    if not webhook_url:
        logger.warning("No webhook URL set, skipping tiered alert")
        return False
    severity = rule.get("severity", "LOW")
    color = _SEVERITY_COLORS.get(severity, "good")
    crypto_id = event.get("crypto_id", "unknown")
    price = event.get("price_usd", 0.0)
    change = event.get("change_24h_pct", 0.0)
    message = (
        f"[{severity}] {rule.get('name', 'alert')} — "
        f"{crypto_id} | price=${price:,.2f} | change={change:+.2f}%"
    )
    try:
        payload = {
            "attachments": [
                {
                    "color": color,
                    "text": message,
                    "ts": datetime.now(timezone.utc).timestamp(),
                }
            ]
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Tiered alert sent: %s", message[:100])
        return True
    except Exception as e:
        logger.error("Failed to send tiered alert: %s", e)
        return False


def save_alert_history(alerts: list, bucket: str, date: str) -> bool:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"monitoring/alerts/history/{date}/alerts.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(alerts, default=str),
            ContentType="application/json",
        )
        logger.info("Alert history saved to S3: %s (%d alerts)", key, len(alerts))
        return True
    except Exception as e:
        logger.error("Failed to save alert history: %s", e)
        return False


def get_alert_statistics(bucket: str, date: str) -> dict:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"monitoring/alerts/history/{date}/alerts.json"
        response = s3.get_object(Bucket=bucket, Key=key)
        alerts = json.loads(response["Body"].read().decode("utf-8"))

        by_severity: dict = {}
        by_crypto: dict = {}
        for alert in alerts:
            severity = alert.get("severity", "UNKNOWN")
            crypto_id = alert.get("crypto_id", "unknown")
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_crypto[crypto_id] = by_crypto.get(crypto_id, 0) + 1

        return {
            "total_alerts": len(alerts),
            "by_severity": by_severity,
            "by_crypto": by_crypto,
        }
    except Exception as e:
        logger.error("Failed to get alert statistics: %s", e)
        return {"total_alerts": 0, "by_severity": {}, "by_crypto": {}}


def run_advanced_alerting(event: dict, bucket: str) -> list:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    triggered = evaluate_alert_rules(event)

    for alert in triggered:
        send_tiered_alert(alert, event, webhook_url)

    if triggered and bucket:
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        save_alert_history(triggered, bucket, today)

    return triggered


if __name__ == "__main__":
    pass
