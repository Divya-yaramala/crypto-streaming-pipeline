# Real-Time Crypto Price Streaming Pipeline

A production-grade real-time cryptocurrency price streaming pipeline built with Apache Kafka, PySpark Structured Streaming, PostgreSQL, and AWS S3.

## Architecture

```
CoinGecko API --> Kafka Producer --> Kafka Topic --> PySpark Consumer --> PostgreSQL
                                                                     \-> AWS S3
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
