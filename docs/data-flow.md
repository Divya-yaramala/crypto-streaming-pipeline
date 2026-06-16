# Data Flow Documentation

## Real-Time Flow (< 1 second latency)
CoinGecko API → Kafka Producer → Kafka Topic → Kafka Consumer → PostgreSQL

## Stream Processing Flow (1-minute windows)
Kafka Topic → PySpark Processor → PostgreSQL (aggregates)

## Cold Storage Flow (async)
Kafka Consumer → AWS S3 (raw events)
PySpark Processor → AWS S3 (aggregations)
Alert Detection → AWS S3 (alerts)

## S3 Structure
```
raw/crypto/YYYY/MM/DD/crypto_id/timestamp.json
processed/aggregations/YYYY/MM/DD/crypto_id/window.json
processed/alerts/YYYY/MM/DD/crypto_id/alert.json
archive/crypto/ (data older than 7 days)
```

## PostgreSQL Tables
| Table | Purpose |
|---|---|
| crypto_prices | Raw price events |
| crypto_alerts | PUMP/DUMP alerts |
| crypto_price_aggregates | 1-minute OHLC windows |

## Historical Backfill Flow
CoinGecko Historical API → backfill.py → S3 (raw/crypto/YYYY/MM/DD/)
                                       → PostgreSQL (crypto_prices)

## Backfill vs Real-Time
| Feature | Real-Time | Backfill |
|---|---|---|
| Source | CoinGecko live API | CoinGecko history API |
| Frequency | Every 60 seconds | On-demand |
| Data | Current prices | Historical OHLC |
| Storage | S3 + PostgreSQL | S3 + PostgreSQL |
| Deduplication | ON CONFLICT | ON CONFLICT |
