# Operations Runbook

## 1. Starting the Pipeline

### Full stack with Docker
```bash
# Copy environment variables
cp .env.example .env
# Edit .env with real credentials

# Validate secrets before starting
python scripts/validate_secrets.py

# Start all services (Kafka, Zookeeper, PostgreSQL, dashboard, API)
make up

# Verify services are running
docker-compose ps
```

### Individual components (without Docker)
```bash
# 1. Start Kafka and PostgreSQL (Docker only)
docker-compose up -d kafka zookeeper postgres

# 2. Set up database tables
python storage/postgres_setup.py

# 3. Start the Kafka producer
python -m producer.crypto_producer

# 4. Start the Kafka consumer (separate terminal)
python -m consumer.crypto_consumer

# 5. Start the PySpark stream processor (separate terminal)
python -m stream_processor.spark_processor

# 6. Start the Streamlit dashboard (separate terminal)
make dashboard

# 7. Start the REST API (separate terminal)
make api
```

## 2. Common Issues and Fixes

### Kafka not connecting
```
Error: NoBrokersAvailable: ...localhost:9092
```
**Fix:**
```bash
docker-compose ps kafka           # Check Kafka container status
docker-compose logs kafka         # Check for startup errors
docker-compose restart kafka      # Restart if unhealthy
```

### PostgreSQL connection refused
```
Error: could not connect to server: Connection refused (port 5432)
```
**Fix:**
```bash
docker-compose ps postgres
docker-compose logs postgres
# Confirm env vars match docker-compose.yml
echo $POSTGRES_HOST $POSTGRES_PORT $POSTGRES_DB
```

### S3 access denied
```
Error: ClientError: An error occurred (AccessDenied)
```
**Fix:**
```bash
# Check credentials are set
echo $AWS_ACCESS_KEY_ID
echo $AWS_BUCKET_NAME
# Verify IAM permissions: s3:PutObject, s3:GetObject, s3:ListBucket
aws s3 ls s3://$AWS_BUCKET_NAME
```

### Snowflake authentication failure
```
Error: 250001: Failed to connect to DB
```
**Fix:**
```bash
# Verify account identifier format: orgname-accountname
echo $SNOWFLAKE_ACCOUNT
python storage/snowflake_connector.py   # Test connection
```

## 3. Monitoring Checks

### Check pipeline health via API
```bash
curl http://localhost:8000/health
curl http://localhost:8000/dashboard
```

### Read daily monitoring reports
```bash
# List today's reports in S3
aws s3 ls s3://$AWS_BUCKET_NAME/monitoring/reports/crypto/$(date +%Y/%m/%d)/

# Download and inspect the daily report
aws s3 cp s3://$AWS_BUCKET_NAME/monitoring/reports/crypto/$(date +%Y/%m/%d)/daily_report.json .
cat daily_report.json | python -m json.tool
```

### Check DLQ event count
```bash
# List failed events for today
aws s3 ls s3://$AWS_BUCKET_NAME/errors/crypto/$(date +%Y/%m/%d)/ --recursive | wc -l
```

### Run tests to verify integrity
```bash
make test
```

## 4. Backfill Procedure

Run backfill when:
- Pipeline was down for multiple days
- A new crypto was added to the tracking list
- Data corruption was detected and corrected

```bash
# Preview what would be loaded (no writes)
python scripts/run_backfill.py --days 30 --dry-run

# Backfill last 30 days for all cryptos
python scripts/run_backfill.py --days 30

# Backfill specific crypto only
python scripts/run_backfill.py --crypto bitcoin --days 90

# Full year backfill (takes several minutes due to API rate limits)
python scripts/run_backfill.py --days 365

# S3 only (if PostgreSQL is already populated)
python scripts/run_backfill.py --days 30 --s3-only
```

See [Backfill Guide](backfill-guide.md) for full details.

## 5. Emergency Procedures

### Stop the pipeline safely
```bash
# Stop all Docker services gracefully
make down

# Or stop individual containers
docker-compose stop consumer
docker-compose stop producer
```

### Replay DLQ events after an outage
```python
from consumer.dead_letter_queue import run_dlq_replay
import os

bucket = os.getenv("AWS_BUCKET_NAME")
date = "2026/01/15"   # date of the outage

result = run_dlq_replay(bucket, date)
print(result)
# {"total": 42, "replayed": 40, "still_failing": 2}
```

### Reset Kafka consumer offset (re-process all messages)
```bash
# Stop the consumer first, then reset offset
docker-compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group crypto-consumer-group \
  --topic crypto-prices \
  --reset-offsets --to-earliest --execute
```

### Force refresh Snowflake MARTS
```bash
cd dbt_project && dbt run --full-refresh
```
