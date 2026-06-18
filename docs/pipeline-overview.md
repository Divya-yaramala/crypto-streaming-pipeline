# Pipeline Overview

## Architecture

```
CoinGecko API
      |
      v
producer/crypto_producer.py  ──► Kafka Topic: crypto-prices
                                        |
              ┌─────────────────────────┼──────────────────────┐
              v                         v                      v
consumer/crypto_consumer.py   stream_processor/           producer/backfill.py
  |  |  |  |                  spark_processor.py          (on-demand)
  |  |  |  |                       |       |
  |  |  |  └─► PostgreSQL          |       └─► S3 processed/
  |  |  └────► S3 raw/             └─► PostgreSQL (aggregates)
  |  └───────► S3 errors/ (DLQ)
  └──────────► Slack alerts
                                        |
                                  storage/snowflake_sync.py
                                        |
                                  Snowflake RAW layer
                                        |
                                  dbt_project/ (transformations)
                                        |
                                  Snowflake MARTS layer
                                        |
                              ┌─────────┴──────────┐
                              v                    v
                     dashboard/app.py          api/main.py
                     (Streamlit :8501)       (FastAPI :8000)
```

## Components

| Component | File | Responsibility |
|---|---|---|
| Kafka Producer | `producer/crypto_producer.py` | Fetch prices from CoinGecko every 60s, publish to Kafka |
| Producer Config | `producer/config.py` | Kafka and CoinGecko settings |
| Historical Backfill | `producer/backfill.py` | Fetch up to 365 days of historical prices |
| Kafka Consumer | `consumer/crypto_consumer.py` | Consume price events, write to PostgreSQL and S3 |
| Data Validator | `consumer/data_validator.py` | 6-point validation on each price event |
| Slack Alerter | `consumer/slack_alerter.py` | PUMP/DUMP alerts and pipeline error notifications |
| Dead Letter Queue | `consumer/dead_letter_queue.py` | Capture failed events to S3, support replay |
| Pipeline Monitor | `consumer/pipeline_monitor.py` | Per-step metrics, hourly/daily reports to S3 |
| Config Manager | `consumer/config_manager.py` | Typed dataclass configs for all services |
| Spark Processor | `stream_processor/spark_processor.py` | 1-minute OHLC aggregations via PySpark Structured Streaming |
| S3 Storage | `storage/s3_storage.py` | Save events, aggregations, alerts to S3 |
| Snowflake Connector | `storage/snowflake_connector.py` | Create Snowflake objects and tables |
| Snowflake Sync | `storage/snowflake_sync.py` | Sync last 24h from PostgreSQL to Snowflake |
| PostgreSQL Setup | `storage/postgres_setup.py` | Create database tables |
| REST API | `api/main.py` | 7 FastAPI endpoints exposing pipeline results |
| Streamlit Dashboard | `dashboard/app.py` | Live price charts, KPI metrics, alerts display |
| Dashboard Config | `dashboard/config.py` | Crypto IDs, colours, refresh interval |
| dbt Models | `dbt_project/models/` | Staging views and mart tables in Snowflake |
| Backfill CLI | `scripts/run_backfill.py` | CLI wrapper for historical backfill |
| Secrets Validator | `scripts/validate_secrets.py` | Check all required env vars at startup |

## Data Models

### PostgreSQL Tables

**crypto_prices**
| Column | Type | Description |
|---|---|---|
| crypto_id | VARCHAR(50) | Cryptocurrency identifier (e.g. bitcoin) |
| price_usd | NUMERIC(20,8) | Price in USD |
| market_cap_usd | NUMERIC(30,2) | Market capitalisation in USD |
| change_24h_pct | NUMERIC(10,4) | 24-hour percentage change |
| event_timestamp | TIMESTAMP | When the price was recorded |
| source | VARCHAR(50) | Data source (coingecko / coingecko_historical) |
| ingested_at | TIMESTAMP | When the row was inserted |

**crypto_alerts**
| Column | Type | Description |
|---|---|---|
| crypto_id | VARCHAR(50) | Cryptocurrency identifier |
| alert_type | VARCHAR(50) | PUMP or DUMP |
| message | TEXT | Human-readable description |
| price_usd | NUMERIC(20,8) | Price at alert time |
| created_at | TIMESTAMP | When the alert was created |

**crypto_price_aggregates**
| Column | Type | Description |
|---|---|---|
| crypto_id | VARCHAR(50) | Cryptocurrency identifier |
| window_start | TIMESTAMP | Start of the 1-minute window |
| window_end | TIMESTAMP | End of the 1-minute window |
| avg_price | NUMERIC(20,8) | Average price in window |
| min_price | NUMERIC(20,8) | Minimum price in window |
| max_price | NUMERIC(20,8) | Maximum price in window |
| price_range | NUMERIC(20,8) | max_price - min_price |
| avg_change_24h | NUMERIC(10,4) | Average 24h change in window |
| record_count | INTEGER | Number of events in window |

### Snowflake Tables

| Table | Layer | Description |
|---|---|---|
| RAW.CRYPTO_PRICES | RAW | Raw price events synced from PostgreSQL |
| RAW.CRYPTO_ALERTS | RAW | Alert events synced from PostgreSQL |
| MARTS.CRYPTO_DAILY_SUMMARY | MARTS | Daily aggregates per crypto |

### dbt Models

| Model | Type | Description |
|---|---|---|
| stg_crypto_prices | View | Staged prices with price_change_usd and is_price_increase |
| stg_crypto_alerts | View | Staged alerts with severity classification |
| fct_crypto_daily | Table | Daily OHLC + volatility per crypto |
| fct_crypto_alerts_summary | Table | Daily alert counts and rates per crypto |
| dim_cryptos | Table | Static dimension: symbol, category, consensus mechanism |

## Kafka Topics

| Topic | Key | Value | Description |
|---|---|---|---|
| crypto-prices | crypto_id | JSON price event | Raw price events from CoinGecko |
| crypto-alerts | crypto_id | JSON alert event | PUMP/DUMP alerts generated by consumer |

## S3 Structure

```
s3://bucket/
├── raw/crypto/YYYY/MM/DD/crypto_id/         # Raw price events (real-time)
├── processed/aggregations/YYYY/MM/DD/       # 1-minute OHLC aggregations
├── processed/alerts/YYYY/MM/DD/             # PUMP/DUMP alert records
├── errors/crypto/YYYY/MM/DD/step/           # DLQ failed events by pipeline step
├── monitoring/crypto/YYYY/MM/DD/step/       # Per-step duration and status metrics
├── monitoring/reports/crypto/YYYY/MM/DD/    # Daily monitoring reports
└── archive/crypto/                          # Events older than 7 days
```

## Error Handling

### Dead Letter Queue (DLQ)
Failed events at each pipeline step are written to `errors/crypto/YYYY/MM/DD/{step}/` in S3. Steps captured: `validate`, `postgres`, `s3`, `alert`, `spark_postgres`, `spark_s3`. The `run_dlq_replay()` function retrieves and re-routes events back through the correct step.

### Retry Logic
- CoinGecko API: single attempt per polling cycle; failures logged and skipped until next interval.
- PostgreSQL inserts: single attempt with rollback on failure; event routed to DLQ.
- S3 puts: single attempt; failure logged and routed to DLQ.

### Alert Routing
- Change > +10%: PUMP alert → PostgreSQL + S3 + Slack
- Change < -10%: DUMP alert → PostgreSQL + S3 + Slack
- Pipeline errors: `alert_pipeline_error()` → Slack warning
- Quality score < 80%: `alert_pipeline_error()` → Slack warning
