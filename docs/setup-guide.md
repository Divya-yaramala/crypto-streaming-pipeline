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

## Performance Tips
- Use parallel_process_events for high-volume event batches
- Set batch_size=100 for PostgreSQL batch inserts
- Cache CoinGecko API responses with ttl_seconds=300
- Clear expired cache weekly: `python -c "from consumer.cache_manager import clear_expired_cache; import os; clear_expired_cache(os.getenv('AWS_BUCKET_NAME'))"`
- Run benchmark to measure speedup: `python -c "from consumer.performance_optimizer import run_benchmark; print(run_benchmark([], lambda x: x))"`

## Cost Optimization Commands
```bash
# Run full cost optimization check
python -c "from consumer.s3_optimizer import run_cost_optimization; import os; print(run_cost_optimization(os.getenv('AWS_BUCKET_NAME')))"

# Check system resources
python -c "from consumer.resource_manager import run_resource_check; import os; print(run_resource_check(os.getenv('AWS_BUCKET_NAME')))"

# Estimate Kafka throughput
python -c "from consumer.resource_manager import estimate_kafka_throughput; print(estimate_kafka_throughput(10.0, 2.5))"
```

## SLA Monitoring Commands
```bash
# Run SLA check for today
python -c "from consumer.sla_monitor import run_sla_check; import os; print(run_sla_check(os.getenv('AWS_BUCKET_NAME')))"

# Generate quality report
python -c "from consumer.quality_reporter import run_quality_reporting; import os; print(run_quality_reporting(os.getenv('AWS_BUCKET_NAME')))"

# Check quality trend (last 7 days)
python -c "from consumer.quality_reporter import compare_quality_trends; import os; print(compare_quality_trends(os.getenv('AWS_BUCKET_NAME')))"
```

## Pro Tips
- Run `python scripts/validate_secrets.py` before starting pipeline
- Use `--dry-run` flag on backfill to preview data volume
- Check Kafka UI at http://localhost:8080 to monitor topics
- Use `make dashboard` to start Streamlit on port 8501
- Run `pytest tests/` before every commit
- Check `monitoring/reports/` in S3 for daily pipeline health
