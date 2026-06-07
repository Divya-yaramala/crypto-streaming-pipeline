# Real-Time Crypto Price Streaming Pipeline

[![CI Pipeline](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/ci.yml)
[![Code Quality](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml/badge.svg)](https://github.com/Divya-yaramala/crypto-streaming-pipeline/actions/workflows/code-quality.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Tests](https://img.shields.io/badge/tests-23%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

A production-grade real-time cryptocurrency price streaming pipeline built with Apache Kafka, PySpark Structured Streaming, PostgreSQL, and AWS S3.

## Architecture

```
CoinGecko API --> Kafka Producer --> Kafka Topic
                                          |
                                   Kafka Consumer --> PostgreSQL (raw prices + alerts)
                                          |                  |
                                    PySpark            AWS S3 (cold storage)
                                    Processor -->      raw/crypto/YYYY/MM/DD/
                                    PostgreSQL         processed/aggregations/
                                    (aggregates)       processed/alerts/
```

## Tech Stack

| Layer          | Technology                  |
|----------------|-----------------------------|
| Streaming      | Apache Kafka                |
| Processing     | PySpark Structured Streaming|
| Source API     | CoinGecko (free, no API key)|
| Storage        | PostgreSQL + AWS S3         |
| Orchestration  | Docker Compose              |
| Testing        | pytest                      |
| CI/CD          | GitHub Actions              |

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
