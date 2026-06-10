import logging
import os
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack_message(message: str, color: str = "good") -> bool:
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return False
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
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack message sent: %s", message[:80])
        return True
    except Exception as e:
        logger.error("Failed to send Slack message: %s", e)
        return False


def alert_price_pump(crypto_id: str, change_pct: float, price: float) -> bool:
    message = (
        f"🚀 PUMP ALERT: {crypto_id} up {change_pct:.2f}% "
        f"— Current price: ${price:,.2f}"
    )
    return send_slack_message(message, color="danger")


def alert_price_dump(crypto_id: str, change_pct: float, price: float) -> bool:
    message = (
        f"📉 DUMP ALERT: {crypto_id} down {abs(change_pct):.2f}% "
        f"— Current price: ${price:,.2f}"
    )
    return send_slack_message(message, color="danger")


def alert_pipeline_error(step: str, error: str) -> bool:
    message = f"⚠️ Pipeline Error in {step}: {error}"
    return send_slack_message(message, color="warning")


def send_daily_summary(
    total_events: int,
    valid_events: int,
    alerts_triggered: int,
    quality_score: float,
) -> bool:
    message = (
        f"📊 Daily Summary:\n"
        f"• Total events: {total_events}\n"
        f"• Valid events: {valid_events} ({quality_score:.1f}%)\n"
        f"• Alerts triggered: {alerts_triggered}\n"
        f"• Quality score: {quality_score:.1f}%"
    )
    return send_slack_message(message, color="good")
