# Real-Time Crypto Price Streaming Pipeline

[![CI Pipeline](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml)
[![Code Quality](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Tests](https://img.shields.io/badge/tests-42%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

> 🚀 **Live Dashboard:** Run `streamlit run dashboard/app.py` and open http://localhost:8501

A production-grade real-time cryptocurrency price streaming pipeline built with Apache Kafka, PySpark Structured Streaming, PostgreSQL, AWS S3, and Snowflake.

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

- [Data Flow](docs/data-flow.md)
- [Setup Guide](docs/setup-guide.md)

## Getting Started

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Start all services
make up

# 3. Run tests
make test
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
