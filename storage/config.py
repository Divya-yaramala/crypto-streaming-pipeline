import os

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_RAW_PREFIX = "raw/crypto"
S3_PROCESSED_PREFIX = "processed"
S3_ARCHIVE_PREFIX = "archive/crypto"
S3_DAYS_TO_KEEP = int(os.getenv("S3_DAYS_TO_KEEP", "7"))

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
