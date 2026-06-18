# Real-Time Crypto Price Streaming Pipeline

[![CI Pipeline](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml)
[![Code Quality](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Tests](https://img.shields.io/badge/tests-72%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

> 🚀 **Live Dashboard:** Run `streamlit run dashboard/app.py` and open http://localhost:8501

A production-grade real-time cryptocurrency price streaming pipeline built with Apache Kafka, PySpark Structured Streaming, PostgreSQL, AWS S3, and Snowflake.

## Key Features

- Real-time Kafka streaming at 60-second intervals
- PySpark micro-batch aggregations (1-minute windows)
- Dual storage: PostgreSQL (hot) + AWS S3 (cold)
- Snowflake data warehouse with dbt transformations
- Live Streamlit dashboard with auto-refresh
- Slack alerts for PUMP/DUMP price movements
- Dead letter queue for zero data loss
- 6-point data validation per event
- Historical backfill up to 365 days
- REST API with Swagger UI

## Architecture

```
CoinGecko API --> Kafka Producer --> Kafka Topic
                                          |
                                   Kafka Consumer --> PostgreSQL + AWS S3
                                          |
                                    PySpark Processor --> PostgreSQL (aggregates)
                                          |
                                    Snowflake Sync --> Snowflake RAW
                                                             |
                                                    dbt --> Snowflake MARTS
                                                            staging views +
                                                            mart tables
                                                             |
                                                    Streamlit Dashboard
```

## Tech Stack

| Layer          | Technology                  |
|----------------|-----------------------------|
| Streaming      | Apache Kafka                |
| Processing     | PySpark Structured Streaming|
| Source API     | CoinGecko (free, no API key)|
| Storage        | PostgreSQL + AWS S3         |
| Data Warehouse | Snowflake                   |
| Transformation | dbt Core                    |
| Orchestration  | Docker Compose              |
| Testing        | pytest                      |
| CI/CD          | GitHub Actions              |

## 📊 Live Dashboard

Real-time crypto price dashboard built with Streamlit:

| Feature | Details |
|---|---|
| Live prices | CoinGecko API (updates every 60s) |
| Price charts | 24-hour Plotly line charts |
| Alerts | PUMP/DUMP detection display |
| Aggregations | 1-minute OHLC windows |

Start the dashboard:

```bash
# With Docker
docker-compose up -d streamlit-dashboard
# Open http://localhost:8501

# Without Docker
streamlit run dashboard/app.py
```

## Documentation

- [Pipeline Overview](docs/pipeline-overview.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Backfill Guide](docs/backfill-guide.md)
- [Data Flow](docs/data-flow.md)
- [Setup Guide](docs/setup-guide.md)
- [Operations Runbook](docs/runbook.md)
- [Project Statistics](docs/project-stats.md)

## Getting Started

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Validate all environment variables
python scripts/validate_secrets.py

# 3. Start all services
make up

# 4. Run tests
make test
```

## REST API

The pipeline exposes a REST API:

| Endpoint | Description |
|---|---|
| GET /health | Health check |
| GET /cryptos | List tracked cryptos |
| GET /prices/{crypto_id} | Recent prices |
| GET /alerts/{crypto_id} | Recent alerts |
| GET /aggregations/{crypto_id} | 1-min aggregations |
| GET /summary/{crypto_id} | Combined summary |
| GET /dashboard | All cryptos summary |

Start the API:
```bash
uvicorn api.main:app --reload --port 8000
# Swagger UI: http://localhost:8000/docs
```

## Backfilling Historical Data

```bash
# Backfill last 30 days for all cryptos
python scripts/run_backfill.py --days 30

# Backfill specific crypto
python scripts/run_backfill.py --crypto bitcoin --days 90

# Preview without loading
python scripts/run_backfill.py --days 30 --dry-run

# S3 only
python scripts/run_backfill.py --days 30 --s3-only
```

## Services

| Service    | Port | Description              |
|------------|------|--------------------------|
| Kafka      | 9092 | Message broker           |
| Zookeeper  | 2181 | Kafka coordination       |
| Kafka UI   | 8080 | Web UI for Kafka topics  |
| PostgreSQL | 5432 | Relational storage       |

## Project Structure

```
crypto-streaming-pipeline/
├── producer/          # Kafka producer — fetches from CoinGecko
├── consumer/          # Kafka consumer — writes to PostgreSQL/S3
├── stream_processor/  # PySpark streaming jobs
├── storage/           # DB schema and S3 utilities
├── tests/             # pytest test suite
├── docs/              # Architecture and API docs
├── docker-compose.yml # Local dev environment
├── .env.example       # Environment variable template
├── requirements.txt   # Python dependencies
└── Makefile           # Common dev commands
```

## Progress Log

### Day 1 — Project Scaffold
- Docker Compose with Kafka, Zookeeper, Kafka UI, PostgreSQL
- Project folder structure created
- README with architecture diagram

### Day 2 — Kafka Producer
- CoinGecko API integration fetching prices for 5 cryptos
- Kafka producer publishing price events every 60 seconds
- Event format: crypto_id, price_usd, market_cap, 24hr_change, timestamp
- 6 unit tests passing green

### Day 3 — Kafka Consumer + PostgreSQL Storage
- Built Kafka consumer reading crypto price events
- PostgreSQL storage with idempotent inserts
- Price alert detection: PUMP (>10%) and DUMP (<-10%)
- Database setup script with crypto_prices and crypto_alerts tables
- 6 unit tests passing green — 12/12 total

### Day 4 — PySpark Stream Processor
- Built PySpark Structured Streaming processor
- Reads from Kafka topic in real-time
- Calculates 1-minute OHLC aggregations per crypto
- Writes micro-batch results to PostgreSQL
- Added crypto_price_aggregates table
- 5 unit tests passing green — 17/17 total

### Day 5 — AWS S3 Cold Storage
- Built S3 storage module saving raw events, aggregations, alerts
- S3 partitioned by date: raw/crypto/YYYY/MM/DD/crypto_id/
- Archive function moves old data after 7 days
- Consumer and PySpark processor wired to save to S3
- 6 unit tests passing green — 23/23 total

### ✅ Day 6 — CI/CD with GitHub Actions
- Created CI pipeline running all 23 tests on every push
- Created code quality workflow: black, isort, flake8, mypy
- Added CI and Code Quality badges to README
- Added MIT License
- Pipeline runs automatically on every commit

### ✅ Day 7 — Real-Time Streamlit Dashboard
- Built live crypto price dashboard with Streamlit
- KPI metrics for all 5 cryptos with 24h change
- Plotly line charts for BTC and ETH price history
- PUMP/DUMP alerts display and aggregations table
- Auto-refresh every 60 seconds
- Containerized with Docker

### ✅ Day 8 — Stream Data Quality + Validation
- Built stream data validator with 6-point validation checks
- Invalid events logged and saved to S3 errors/ prefix
- Validation wired into Kafka consumer and PySpark processor
- Stream quality score calculated per batch
- 7 unit tests passing green — 30/30 total

### ✅ Day 9 — Slack Alerting + Notifications
- Built Slack alerter with PUMP/DUMP price alerts
- Pipeline error notifications with color-coded severity
- Daily summary with quality score and event counts
- Slack wired into consumer and data validator
- 6 unit tests passing green — 36/36 total

### ✅ Day 10 — Snowflake Data Warehouse Integration
- Built Snowflake connector with warehouse, database, and schema setup
- Sync module pushes last 24h of prices and alerts from PostgreSQL to Snowflake
- RAW layer: CRYPTO_PRICES and CRYPTO_ALERTS tables with dedup logic
- MARTS layer: CRYPTO_DAILY_SUMMARY aggregated by crypto and date
- Snowflake sync service added to Docker Compose
- 6 unit tests passing green — 42/42 total

### ✅ Day 11 — dbt Transformations
- Set up dbt project connecting to Snowflake
- Staging models: stg_crypto_prices, stg_crypto_alerts
- Mart models: fct_crypto_daily, fct_crypto_alerts_summary, dim_cryptos
- schema.yml with not_null tests on key columns
- dbt added to Docker Compose and Makefile

### ✅ Day 12 — Dead Letter Queue Pattern
- Built DLQ capturing failed events to S3 errors/crypto/YYYY/MM/DD/
- Failed events categorized by step: validate, postgres, s3, alert
- DLQ replay routes events back through correct pipeline step
- Wired into consumer and PySpark processor
- 6 unit tests passing green — 48/48 total

### ✅ Day 13 — Pipeline Monitoring + Metrics
- Built pipeline monitor tracking per-step metrics and durations
- Hourly and daily report generation saved to S3
- Success rates, avg/min/max durations per step
- Monitoring wired into consumer and PySpark processor
- 6 unit tests passing green — 54/54 total

### ✅ Day 14 — Historical Backfill Capability
- Built backfill module fetching historical prices from CoinGecko
- Saves to both S3 and PostgreSQL with idempotent inserts
- CLI script with --days, --crypto, --dry-run, --s3-only flags
- Handles up to 365 days of historical data
- 6 unit tests passing green — 60/60 total

### ✅ Day 15 — REST API
- Built 7 REST endpoints exposing pipeline results
- Swagger UI auto-generated at /docs
- Endpoints for prices, alerts, aggregations, summary
- Dashboard endpoint returns all 5 cryptos at once
- 6 unit tests passing green — 66/66 total

### ✅ Day 16 — Configuration Management
- Built typed configuration manager using Python dataclasses
- 5 config classes: Kafka, AWS, Postgres, Snowflake, Crypto
- CLI secrets validator checking all required env vars
- Fail-fast validation at pipeline startup
- 6 unit tests passing green — 72/72 total

### ✅ Day 17 — Documentation + ADRs
- Created 7 Architecture Decision Records
- Comprehensive pipeline overview document
- Operations runbook for day-to-day management
- README updated with docs links and key features
- Portfolio-ready documentation
