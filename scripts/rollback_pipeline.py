import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consumer.data_versioner import rollback_to_version

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

VALID_CRYPTOS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]
VALID_STEPS = ["consume", "validate", "aggregate", "s3", "snowflake"]


def validate_rollback_args(args) -> bool:
    if args.crypto not in VALID_CRYPTOS:
        logger.error("Invalid crypto '%s'. Choose from: %s", args.crypto, VALID_CRYPTOS)
        return False
    if args.step not in VALID_STEPS:
        logger.error("Invalid step '%s'. Choose from: %s", args.step, VALID_STEPS)
        return False
    if len(args.version_id) != 8:
        logger.error("--version-id must be exactly 8 characters (got %d)", len(args.version_id))
        return False
    return True


def execute_rollback(
    crypto: str,
    step: str,
    version_id: str,
    date: str,
    dry_run: bool,
) -> None:
    bucket = os.getenv("AWS_BUCKET_NAME", "")
    if dry_run:
        logger.info(
            "DRY RUN — would rollback %s/%s to version %s on %s (bucket=%s)",
            crypto,
            step,
            version_id,
            date,
            bucket,
        )
        print("\nDry run rollback preview:")
        print(f"  Crypto    : {crypto}")
        print(f"  Step      : {step}")
        print(f"  Version ID: {version_id}")
        print(f"  Date      : {date}")
        print(f"  Bucket    : {bucket or '(not set)'}")
        return
    result = rollback_to_version(crypto, step, version_id, bucket, date)
    if result:
        logger.info("Rollback succeeded: %s/%s → version %s", crypto, step, version_id)
    else:
        logger.error("Rollback failed: no data returned for version %s", version_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rollback a pipeline step to a previous version")
    parser.add_argument(
        "--crypto",
        required=True,
        choices=VALID_CRYPTOS,
        help="Crypto to rollback",
    )
    parser.add_argument(
        "--step",
        required=True,
        choices=VALID_STEPS,
        help="Pipeline step to rollback",
    )
    parser.add_argument(
        "--version-id",
        required=True,
        dest="version_id",
        help="Specific version ID to rollback to (8 characters)",
    )
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview rollback without executing",
    )
    args = parser.parse_args()
    if not validate_rollback_args(args):
        sys.exit(1)
    execute_rollback(args.crypto, args.step, args.version_id, args.date, args.dry_run)
    print("\nRollback summary:")
    print(f"  Crypto    : {args.crypto}")
    print(f"  Step      : {args.step}")
    print(f"  Version ID: {args.version_id}")
    print(f"  Date      : {args.date}")
    print(f"  Mode      : {'dry-run' if args.dry_run else 'executed'}")


if __name__ == "__main__":
    main()
