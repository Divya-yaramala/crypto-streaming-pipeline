import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
