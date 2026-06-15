import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer import backfill
from producer.config import CRYPTO_IDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def validate_args(args) -> bool:
    if not (1 <= args.days <= 365):
        logger.error("--days must be between 1 and 365 (got %d)", args.days)
        return False
    if args.crypto and args.crypto not in CRYPTO_IDS:
        logger.error("--crypto '%s' is not valid. Choose from: %s", args.crypto, CRYPTO_IDS)
        return False
    return True


def run_backfill_cli(args) -> None:
    crypto_ids = [args.crypto] if args.crypto else CRYPTO_IDS
    bucket = None if args.postgres_only else os.getenv("AWS_BUCKET_NAME", "")

    if args.dry_run:
        logger.info("DRY RUN — no data will be loaded")
        logger.info("Would backfill %d days for: %s", args.days, crypto_ids)
        logger.info(
            "S3 enabled: %s | PostgreSQL enabled: %s", not args.postgres_only, not args.s3_only
        )
        print("\nDry run summary:")
        print(f"  Cryptos : {', '.join(crypto_ids)}")
        print(f"  Days    : {args.days}")
        print(f"  S3      : {'skip' if args.postgres_only else 'enabled'}")
        print(f"  Postgres: {'skip' if args.s3_only else 'enabled'}")
        return

    logger.info("Starting backfill: %d days for %s", args.days, crypto_ids)

    if args.s3_only:
        result = backfill.run_backfill(crypto_ids, days=args.days, bucket=bucket)
        result["saved_postgres"] = "skipped"
    elif args.postgres_only:
        result = backfill.run_backfill(crypto_ids, days=args.days, bucket=None)
        result["saved_s3"] = "skipped"
    else:
        result = backfill.run_backfill(crypto_ids, days=args.days, bucket=bucket)

    print("\nBackfill summary:")
    print(f"{'Crypto':<12} | {'Events':>6} | {'S3':>8} | {'Postgres':>8}")
    print("-" * 42)
    events_per_crypto = result["total_events"] // max(len(crypto_ids), 1)
    for cid in crypto_ids:
        s3_status = "skipped" if args.postgres_only else str(events_per_crypto)
        pg_status = "skipped" if args.s3_only else str(events_per_crypto)
        print(f"{cid:<12} | {events_per_crypto:>6} | {s3_status:>8} | {pg_status:>8}")
    print("-" * 42)
    s3_total = str(result["saved_s3"])
    pg_total = str(result["saved_postgres"])
    print(f"{'TOTAL':<12} | {result['total_events']:>6} | {s3_total:>8} | {pg_total:>8}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical crypto price data")
    parser.add_argument(
        "--crypto",
        type=str,
        default=None,
        help=f"Specific crypto to backfill. Choices: {CRYPTO_IDS}",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (1-365, default 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be loaded without actually loading data",
    )
    parser.add_argument(
        "--s3-only",
        action="store_true",
        help="Save to S3 only, skip PostgreSQL",
    )
    parser.add_argument(
        "--postgres-only",
        action="store_true",
        help="Save to PostgreSQL only, skip S3",
    )
    args = parser.parse_args()
    if not validate_args(args):
        sys.exit(1)
    run_backfill_cli(args)


if __name__ == "__main__":
    main()
