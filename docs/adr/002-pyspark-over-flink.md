# ADR 002 - PySpark over Apache Flink

**Status:** Accepted

## Context

The pipeline needed a stream processing framework to compute 1-minute OHLC aggregations over the Kafka price stream.

## Decision

Chose PySpark Structured Streaming over Apache Flink.

## Reasons

- **Python-native API:** PySpark exposes a full Python API — no Java or Scala required to write transformation logic.
- **Unified batch and streaming:** The same DataFrame API works for both batch backfills and live streaming jobs, reducing code duplication.
- **AWS ecosystem integration:** PySpark integrates natively with S3, Glue, and EMR, aligning with the project's AWS storage layer.
- **Community and documentation:** Larger Python community with more examples, tutorials, and Stack Overflow answers than Flink's Python API (PyFlink).

## Consequences

- Higher processing latency than Flink due to micro-batch execution model (not true event-time streaming).
- Requires a JVM on the host machine even when writing pure Python logic.
