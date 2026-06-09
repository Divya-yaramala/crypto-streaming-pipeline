import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

VALID_CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]


def validate_price_event(event: dict) -> dict:
    checks = {}
    errors = []

    # 1. required_fields
    required = ["crypto_id", "price_usd", "timestamp"]
    missing = [f for f in required if event.get(f) is None]
    checks["required_fields"] = not missing
    if missing:
        errors.append(f"Missing required fields: {missing}")

    # 2. price_positive
    price = event.get("price_usd")
    if price is not None:
        checks["price_positive"] = price > 0
        if not checks["price_positive"]:
            errors.append(f"price_usd must be positive, got {price}")
    else:
        checks["price_positive"] = False

    # 3. valid_crypto
    crypto_id = event.get("crypto_id")
    checks["valid_crypto"] = crypto_id in VALID_CRYPTO_IDS
    if not checks["valid_crypto"]:
        errors.append(f"Unknown crypto_id: {crypto_id}")

    # 4. timestamp_format
    ts = event.get("timestamp")
    if ts is not None:
        try:
            datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            checks["timestamp_format"] = True
        except ValueError:
            checks["timestamp_format"] = False
            errors.append(f"Invalid timestamp format: {ts}")
    else:
        checks["timestamp_format"] = False

    # 5. price_range
    if price is not None:
        checks["price_range"] = 0.0001 <= price <= 10_000_000
        if not checks["price_range"]:
            errors.append(f"price_usd {price} out of range [0.0001, 10000000]")
    else:
        checks["price_range"] = False

    # 6. change_range (optional field — passes when absent)
    change = event.get("change_24h_pct")
    if change is not None:
        checks["change_range"] = -99 <= change <= 1000
        if not checks["change_range"]:
            errors.append(f"change_24h_pct {change} out of range [-99, 1000]")
    else:
        checks["change_range"] = True

    valid = all(checks.values())
    result = {"valid": valid, "checks": checks, "errors": errors}

    if valid:
        logger.info("Event valid: %s at $%s", event.get("crypto_id"), event.get("price_usd"))
    else:
        logger.warning("Event invalid: %s | errors: %s", event.get("crypto_id"), errors)

    return result


def validate_aggregation(agg: dict) -> dict:
    checks = {}
    errors = []

    # 1. avg_price > 0
    avg_price = agg.get("avg_price")
    checks["avg_price_positive"] = avg_price is not None and avg_price > 0
    if not checks["avg_price_positive"]:
        errors.append(f"avg_price must be positive, got {avg_price}")

    # 2. min_price <= avg_price <= max_price
    min_price = agg.get("min_price")
    max_price = agg.get("max_price")
    if all(v is not None for v in [min_price, avg_price, max_price]):
        checks["price_order"] = min_price <= avg_price <= max_price
        if not checks["price_order"]:
            errors.append(f"Price order invalid: min={min_price} avg={avg_price} max={max_price}")
    else:
        checks["price_order"] = False
        errors.append("min_price, avg_price, or max_price is None")

    # 3. record_count > 0
    record_count = agg.get("record_count")
    checks["record_count_positive"] = record_count is not None and record_count > 0
    if not checks["record_count_positive"]:
        errors.append(f"record_count must be positive, got {record_count}")

    # 4. window_start before window_end (optional fields)
    window_start = agg.get("window_start")
    window_end = agg.get("window_end")
    if window_start is not None and window_end is not None:
        checks["window_order"] = str(window_start) < str(window_end)
        if not checks["window_order"]:
            errors.append(f"window_start {window_start} is not before window_end {window_end}")
    else:
        checks["window_order"] = True

    valid = all(checks.values())
    result = {"valid": valid, "checks": checks, "errors": errors}
    logger.info("Aggregation validation for %s: valid=%s", agg.get("crypto_id"), valid)
    return result


def calculate_stream_quality_score(events: list) -> float:
    if not events:
        return 0.0
    valid_count = sum(1 for e in events if e.get("valid", False))
    score = (valid_count / len(events)) * 100
    logger.info("Stream quality score: %.1f%% (%d/%d valid)", score, valid_count, len(events))
    return score


def run_stream_validation(events: list) -> dict:
    results = [validate_price_event(e) for e in events]
    valid_count = sum(1 for r in results if r["valid"])
    invalid_count = len(results) - valid_count
    all_errors = [err for r in results for err in r["errors"]]
    quality_score = calculate_stream_quality_score(results)
    return {
        "total": len(results),
        "valid": valid_count,
        "invalid": invalid_count,
        "quality_score": quality_score,
        "errors": all_errors,
    }
