# ADR 001 - Apache Kafka over RabbitMQ

**Status:** Accepted

## Context

The pipeline needed a message broker to decouple the CoinGecko price fetcher from the downstream consumers and stream processor. The two primary candidates were Apache Kafka and RabbitMQ.

## Decision

Chose Apache Kafka over RabbitMQ.

## Reasons

- **Message retention for replay:** Kafka retains messages on disk, enabling the DLQ replay pattern to recover failed events without data loss.
- **High-throughput streaming:** Kafka is purpose-built for high-volume, ordered event streams — better suited to financial tick data than RabbitMQ's queue model.
- **Native PySpark connector:** PySpark Structured Streaming has a first-class Kafka source/sink, making the Spark integration straightforward.
- **Industry standard:** Kafka is the de-facto standard for financial data streaming pipelines.

## Consequences

- Requires Zookeeper as a coordination layer, increasing setup complexity.
- Higher memory footprint than RabbitMQ for a small-scale deployment.
