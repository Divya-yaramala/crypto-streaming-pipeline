# Setup Guide

## Prerequisites
- Docker Desktop
- Python 3.11+
- AWS Account with S3 bucket

## Quick Start

```bash
# Clone repo
git clone https://github.com/Divya-yaramala/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline

# Setup environment
cp .env.example .env
# Fill in AWS credentials and Postgres settings

# Start Kafka and Postgres
make up

# Verify Kafka is running
# Open http://localhost:8080 for Kafka UI

# Run producer (terminal 1)
python producer/crypto_producer.py

# Run consumer (terminal 2)
python consumer/crypto_consumer.py

# Run PySpark processor (terminal 3)
python stream_processor/spark_processor.py
```
