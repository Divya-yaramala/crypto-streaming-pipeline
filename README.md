# 🚀 Real-Time Crypto Price Streaming Pipeline

[![CI Pipeline](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml)
[![Code Quality](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Tests](https://img.shields.io/badge/tests-119%2B%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

---
> 🎉 **Project 2 Complete!** 119+ tests · Kafka · PySpark · Snowflake · dbt · Streamlit · REST API · 9 ADRs · 20 production patterns · 25 days
---

> A production-grade real-time crypto price streaming pipeline that ingests live prices every 60 seconds, processes with PySpark, stores in PostgreSQL and AWS S3, transforms with dbt, warehouses in Snowflake, and displays on a live Streamlit dashboard.

## 📐 Architecture

```
CoinGecko API ──► Kafka Producer ──► Kafka Topic
                                          │
                                          ▼
                                   Kafka Consumer ──► PostgreSQL + AWS S3
                                          │
                                          ▼
                                  PySpark Processor ──► PostgreSQL (aggregates)
                                          │
                                          ▼
                                  Snowflake Sync ──► Snowflake RAW + MARTS
                                          │
                                          ▼
                                  dbt ──► Snowflake MARTS
                                          │
                              ┌───────────┴───────────┐
                              ▼                       ▼
                     Streamlit Dashboard          REST API
```

## ✨ Key Features

- 🔄 Real-time Kafka streaming at 60-second intervals
- ⚡ PySpark micro-batch aggregations (1-minute windows)
- 🗄️ Dual storage: PostgreSQL (hot) + AWS S3 (cold)
- ❄️ Snowflake data warehouse with dbt transformations
- 📊 Live Streamlit dashboard with auto-refresh
- 🔔 Slack PUMP/DUMP alerts (>10% price change)
- 🛡️ Dead letter queue for zero data loss
- ✅ 6-point stream data validation per event
- 📈 Historical backfill up to 365 days
- 🌐 REST API with 7 endpoints + Swagger UI
- 🔁 Retry logic with tenacity for resilience
- 📋 9 Architecture Decision Records

## 📊 Project Stats
| Metric | Value |
|---|---|
| Total tests | 119+ passing |
| Production patterns | 20 |
| ADRs | 9 |
| Docker services | 6 |
| Days to build | 25 |
| CI/CD workflows | 2 |

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka |
| Processing | PySpark Structured Streaming |
| Source API | CoinGecko (free, no key needed) |
| Hot Storage | PostgreSQL |
| Cold Storage | AWS S3 |
| Transformation | dbt Core |
| Data Warehouse | Snowflake |
| Dashboard | Streamlit + Plotly |
| REST API | FastAPI |
| Alerting | Slack Webhooks |
| CI/CD | GitHub Actions |
| Testing | pytest (80+ tests) |
| Code Quality | black, isort, flake8, mypy |

## 🏗️ Pipeline Components

| Component | File | Responsibility |
|---|---|---|
| Producer | producer/crypto_producer.py | Fetch + publish prices |
| Consumer | consumer/crypto_consumer.py | Process + store events |
| Validator | consumer/data_validator.py | 6-point validation |
| DLQ | consumer/dead_letter_queue.py | Capture failed events |
| Monitor | consumer/pipeline_monitor.py | Track metrics |
| Spark | stream_processor/spark_processor.py | Aggregations |
| S3 Storage | storage/s3_storage.py | Cold storage |
| Snowflake | storage/snowflake_connector.py | Warehouse sync |
| API | api/main.py | REST endpoints |
| Dashboard | dashboard/app.py | Live visualization |

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Docker Desktop
- AWS Account with S3 bucket
- Snowflake Account (free trial)
- Slack Webhook URL (optional)

### Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/Divya-yaramala/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
cp .env.example .env
# Fill in credentials

# 2. Validate environment
python scripts/validate_secrets.py

# 3. Start Kafka and PostgreSQL
make up

# 4. Backfill historical data (optional)
python scripts/run_backfill.py --days 30

# 5. Start producer (terminal 1)
python producer/crypto_producer.py

# 6. Start consumer (terminal 2)
python consumer/crypto_consumer.py

# 7. Start dashboard
make dashboard
# Open http://localhost:8501

# 8. Start REST API
make api
# Open http://localhost:8000/docs
```

### Running Tests

```bash
pytest tests/ -v
```

### Rollback a Pipeline Step

```bash
# Preview rollback without executing
python scripts/rollback_pipeline.py --crypto bitcoin --step consume --version-id abc12345 --dry-run

# Execute rollback
python scripts/rollback_pipeline.py --crypto bitcoin --step consume --version-id abc12345
```

## 🌐 REST API

| Endpoint | Description |
|---|---|
| GET /health | Health check |
| GET /cryptos | List tracked cryptos |
| GET /prices/{crypto_id} | Recent prices |
| GET /alerts/{crypto_id} | Recent alerts |
| GET /aggregations/{crypto_id} | 1-min aggregations |
| GET /summary/{crypto_id} | Combined summary |
| GET /dashboard | All cryptos summary |

```bash
uvicorn api.main:app --reload --port 8000
# Swagger UI: http://localhost:8000/docs
```

## 📚 Documentation

- [Pipeline Overview](docs/pipeline-overview.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Backfill Guide](docs/backfill-guide.md)
- [Data Flow](docs/data-flow.md)
- [Setup Guide](docs/setup-guide.md)
- [Operations Runbook](docs/runbook.md)
- [Project Statistics](docs/project-stats.md)
- [Project Completion](docs/project-completion.md)

## 🏛️ Architecture Decisions

| ADR | Decision | Status |
|---|---|---|
| 001 | Apache Kafka over RabbitMQ | ✅ Accepted |
| 002 | PySpark over Apache Flink | ✅ Accepted |
| 003 | CoinGecko over Binance API | ✅ Accepted |
| 004 | PostgreSQL over MongoDB | ✅ Accepted |
| 005 | Snowflake over Redshift | ✅ Accepted |
| 006 | Streamlit over Plotly Dash | ✅ Accepted |
| 007 | S3 Cold Storage Strategy | ✅ Accepted |
| 008 | S3 Caching Strategy | ✅ Accepted |
| 009 | Retry Logic with Tenacity | ✅ Accepted |

## 📈 Progress Log

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

### ✅ Day 18 — Performance Optimization + Caching
- Built S3-backed cache manager with TTL, hit/miss, and expiry logic
- MD5 cache key generator for consistent, reproducible lookups
- Performance optimizer with timer decorator, parallel ThreadPoolExecutor, and batch Postgres inserts
- Benchmark utility comparing sequential vs parallel throughput
- 11 unit tests passing green — 83/83 total

### ✅ Day 19 — Final Code Review + Production Hardening
- Full code review across all modules with docstrings and type hints
- Added edge case tests for boundary conditions
- Retry logic with tenacity for API and database calls
- All linters passing: black, isort, flake8, mypy
- Production-ready codebase — Project 2 complete!

### ✅ Day 20 — World-Class README + Portfolio Polish
- Rewrote README with badges, architecture, features, tech stack
- Complete component table with file references
- All 9 ADRs documented in summary table
- Portfolio-ready presentation

### ✅ Day 21 — Advanced Analytics + Portfolio Tracking
- Built portfolio tracker: load allocations from S3, compute $10k portfolio value and daily returns
- Built technical indicators: SMA, RSI, Bollinger Bands, MACD using numpy
- Results saved to S3 under analytics/crypto/ and portfolio/crypto/snapshots/
- 11 new unit tests passing green — 100/100 total

### ✅ Day 22 — News Sentiment + Market Correlation
- Built crypto news sentiment analyzer (BULLISH/BEARISH/NEUTRAL)
- Market correlation matrix for all 5 cryptos
- Beta calculation vs Bitcoin as market proxy
- Sentiment and correlation results saved to S3
- 10 unit tests passing green — 110/110 total

### ✅ Day 23 — Advanced Alerting + Alert Aggregation
- Built tiered alerting with 6 rules and 4 severity levels
- Alert aggregation with hourly bucketing and pattern detection
- Duplicate alert suppression (30-minute window)
- Daily alert digest sent to Slack
- 10 unit tests passing green — 120/120 total

### ✅ Day 24 — Data Versioning + Pipeline Rollback
- Built data versioner with MD5-based version IDs
- Snapshots saved to S3 under versions/crypto/YYYY/MM/DD/
- CLI rollback script with --dry-run preview mode
- Can rollback any pipeline step to any previous version
- 6 unit tests passing green — 126/126 total

### ✅ Day 25 — Final Portfolio Polish
- Created final project statistics document
- Updated README with project stats table
- Portfolio-ready with 119+ tests and 20 production patterns
- Project 2 complete after 25 days of building!
