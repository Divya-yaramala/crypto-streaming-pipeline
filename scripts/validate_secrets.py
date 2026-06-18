import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

REQUIRED_VARS = {
    "Kafka": ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_CRYPTO_PRICES"],
    "AWS": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_BUCKET_NAME"],
    "Postgres": ["POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"],
    "Snowflake": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
}

OPTIONAL_VARS = {
    "Slack": ["SLACK_WEBHOOK_URL"],
    "NewsAPI": ["NEWS_API_KEY"],
}


def check_required_secrets() -> dict:
    report = {}
    for service, vars_ in REQUIRED_VARS.items():
        missing = [v for v in vars_ if not os.getenv(v)]
        report[service] = {
            "status": "missing" if missing else "ok",
            "missing_vars": missing,
            "required": True,
        }
    for service, vars_ in OPTIONAL_VARS.items():
        missing = [v for v in vars_ if not os.getenv(v)]
        report[service] = {
            "status": "missing" if missing else "ok",
            "missing_vars": missing,
            "required": False,
        }
    return report


def print_secrets_report(report: dict) -> None:
    print(f"\n{'Service':<12} | {'Status':<12} | Missing vars")
    print("-" * 55)
    any_required_missing = False
    for service, info in report.items():
        required = info["required"]
        if info["status"] == "ok":
            status_str = "ok"
        else:
            status_str = "MISSING" if required else "not set"
        missing_str = ", ".join(info["missing_vars"]) if info["missing_vars"] else ""
        print(f"{service:<12} | {status_str:<12} | {missing_str}")
        if info["status"] == "missing" and required:
            any_required_missing = True
    print()
    if any_required_missing:
        logger.error("Required secrets missing — pipeline cannot start")
        sys.exit(1)
    else:
        logger.info("All required secrets present")


if __name__ == "__main__":
    report = check_required_secrets()
    print_secrets_report(report)
