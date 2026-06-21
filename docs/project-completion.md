# Project 2 Complete — Crypto Streaming Pipeline

## Summary
Real-time crypto price streaming pipeline built over 19 days.

## What Was Built
- Kafka producer fetching prices every 60 seconds
- Kafka consumer storing to PostgreSQL + S3
- PySpark micro-batch aggregations (1-minute windows)
- Snowflake data warehouse with dbt transformations
- Streamlit live dashboard with auto-refresh
- REST API with 7 endpoints
- Dead letter queue for zero data loss
- Slack alerting for PUMP/DUMP events
- Historical backfill up to 365 days
- CI/CD with GitHub Actions

## Stats
- 19 days of development
- 89 tests passing
- 9 ADRs documented
- CI/CD green badges
- 17 production patterns

## Tech Stack
Kafka · PySpark · PostgreSQL · Snowflake · dbt · AWS S3 · Streamlit · FastAPI · Docker
