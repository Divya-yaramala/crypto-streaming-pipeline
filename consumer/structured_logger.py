import json
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_structured_log(
    level: str,
    message: str,
    module: str,
    crypto_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    env = os.getenv("ENV", "development")
    tags: List[str] = [module]
    if crypto_id:
        tags.append(crypto_id)
    log_dict: Dict[str, Any] = {
        "timestamp": ts,
        "level": level,
        "message": message,
        "module": module,
        "crypto_id": crypto_id,
        "tags": tags,
        "extra": extra or {},
        "environment": env,
    }
    return log_dict


def log_info(
    message: str,
    module: str,
    crypto_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    log_dict = create_structured_log("INFO", message, module, crypto_id, extra)
    logger.info(json.dumps(log_dict))


def log_error(
    message: str,
    module: str,
    error: Optional[Exception] = None,
    crypto_id: Optional[str] = None,
) -> None:
    extra: Dict[str, Any] = {}
    if error is not None:
        extra["error_type"] = type(error).__name__
        extra["error_message"] = str(error)
        extra["traceback"] = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
    log_dict = create_structured_log("ERROR", message, module, crypto_id, extra or None)
    logger.error(json.dumps(log_dict))


def log_metric(
    metric_name: str,
    value: float,
    module: str,
    crypto_id: Optional[str] = None,
    unit: str = "count",
) -> None:
    extra: Dict[str, Any] = {
        "metric_name": metric_name,
        "value": value,
        "unit": unit,
    }
    msg = f"metric:{metric_name}={value}"
    log_dict = create_structured_log("METRIC", msg, module, crypto_id, extra)
    logger.info(json.dumps(log_dict))


def log_pipeline_start(crypto_id: str, step: str) -> None:
    log_info(
        f"Pipeline step started: {step}",
        module="pipeline",
        crypto_id=crypto_id,
        extra={"step": step, "phase": "start"},
    )


def log_pipeline_end(
    crypto_id: str,
    step: str,
    duration_seconds: float,
    success: bool,
) -> None:
    status = "completed" if success else "failed"
    message = f"Pipeline step {status}: {step} ({duration_seconds:.3f}s)"
    extra: Dict[str, Any] = {
        "step": step,
        "phase": "end",
        "duration_seconds": duration_seconds,
        "success": success,
    }
    if success:
        log_info(message, module="pipeline", crypto_id=crypto_id, extra=extra)
    else:
        log_error(message, module="pipeline", crypto_id=crypto_id)


if __name__ == "__main__":
    pass
