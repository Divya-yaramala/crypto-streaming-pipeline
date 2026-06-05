import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

WINDOW_DURATION = "1 minute"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CRYPTO_PRICES = os.getenv("KAFKA_TOPIC_CRYPTO_PRICES", "crypto-prices")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")

_SCHEMA = StructType(
    [
        StructField("crypto_id", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("market_cap_usd", DoubleType(), True),
        StructField("change_24h_pct", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("CryptoStreamProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        .getOrCreate()
    )
    logger.info("SparkSession created")
    return spark


def read_kafka_stream(spark: SparkSession):
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_CRYPTO_PRICES)
        .load()
    )
    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), _SCHEMA).alias("data")
    ).select("data.*")
    logger.info("Kafka stream reader configured for topic '%s'", KAFKA_TOPIC_CRYPTO_PRICES)
    return parsed_df


def calculate_aggregations(df):
    return df.groupBy(
        "crypto_id",
        F.window(F.col("timestamp"), WINDOW_DURATION),
    ).agg(
        F.avg("price_usd").alias("avg_price"),
        F.min("price_usd").alias("min_price"),
        F.max("price_usd").alias("max_price"),
        (F.max("price_usd") - F.min("price_usd")).alias("price_range"),
        F.avg("change_24h_pct").alias("avg_change_24h"),
        F.count("*").alias("record_count"),
    )


def write_to_postgres(df, epoch_id: int) -> None:
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    row_count = df.count()
    df.write.jdbc(
        url=jdbc_url,
        table="crypto_price_aggregates",
        mode="append",
        properties=properties,
    )
    logger.info("Batch %d written: %d rows", epoch_id, row_count)


def run_stream_processor() -> None:
    spark = create_spark_session()
    logger.info("Stream processor started")
    raw_stream = read_kafka_stream(spark)
    agg_stream = calculate_aggregations(raw_stream)
    query = (
        agg_stream.writeStream.outputMode("update")
        .foreachBatch(write_to_postgres)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_stream_processor()
