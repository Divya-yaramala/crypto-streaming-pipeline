# Architecture — Real-Time Crypto Streaming Pipeline

## Data Flow
CoinGecko API → Kafka Producer → Kafka Topic → Kafka Consumer → PostgreSQL

## Components
| Component | File | Responsibility |
|---|---|---|
| Producer | producer/crypto_producer.py | Fetches crypto prices, publishes to Kafka |
| Consumer | consumer/crypto_consumer.py | Reads from Kafka, saves to PostgreSQL |
| DB Setup | storage/setup_db.py | Creates tables |
| Docker | docker-compose.yml | Kafka + Zookeeper + PostgreSQL |

## Kafka Topics
| Topic | Purpose |
|---|---|
| crypto-prices | Raw price events from CoinGecko |
| crypto-alerts | PUMP/DUMP alerts |

## Database Tables
| Table | Purpose |
|---|---|
| crypto_prices | All price events with idempotent inserts |
| crypto_alerts | PUMP/DUMP alerts when price changes > 10% |
