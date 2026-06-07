import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import MagicMock

# Mock pyspark before importing spark_processor to avoid requiring Java/JVM
for _mod in ["pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"]:
    sys.modules[_mod] = MagicMock()

import stream_processor.spark_processor as sp
from stream_processor.spark_processor import (
    WINDOW_DURATION,
    calculate_aggregations,
    create_spark_session,
    read_kafka_stream,
    write_to_postgres,
)


def test_create_spark_session_success():
    mock_session = MagicMock()
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = mock_session

    sp.SparkSession.builder = builder

    result = create_spark_session()

    assert result is mock_session
    builder.appName.assert_called_with("CryptoStreamProcessor")


def test_calculate_aggregations_columns():
    mock_df = MagicMock()
    mock_grouped = MagicMock()
    mock_result = MagicMock()
    mock_result.columns = [
        "crypto_id",
        "window",
        "avg_price",
        "min_price",
        "max_price",
        "price_range",
        "avg_change_24h",
        "record_count",
    ]
    mock_df.groupBy.return_value = mock_grouped
    mock_grouped.agg.return_value = mock_result

    result = calculate_aggregations(mock_df)

    assert "avg_price" in result.columns
    assert "min_price" in result.columns
    assert "max_price" in result.columns


def test_write_to_postgres_called():
    mock_df = MagicMock()
    mock_df.count.return_value = 5

    write_to_postgres(mock_df, epoch_id=0)

    mock_df.write.jdbc.assert_called_once()
    call_kwargs = mock_df.write.jdbc.call_args[1]
    assert call_kwargs.get("table") == "crypto_price_aggregates"


def test_read_kafka_stream_format():
    mock_spark = MagicMock()
    read_stream = MagicMock()
    mock_spark.readStream = read_stream
    read_stream.format.return_value = read_stream
    read_stream.option.return_value = read_stream
    read_stream.load.return_value = MagicMock()

    read_kafka_stream(mock_spark)

    read_stream.format.assert_called_with("kafka")


def test_aggregation_window_size():
    assert WINDOW_DURATION == "1 minute"
