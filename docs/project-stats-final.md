# Final Project Statistics — Crypto Streaming Pipeline

## Code Statistics
| Metric | Count |
|---|---|
| Python modules | 25+ |
| Test files | 15+ test files |
| Total tests | 119+ passing |
| Kafka topics | 2 |
| PostgreSQL tables | 3 |
| Snowflake tables | 3 |
| dbt models | 5 |
| ADRs | 9 |
| REST API endpoints | 7 |
| Docker services | 6 |

## Pipeline Statistics
| Component | Details |
|---|---|
| Cryptos tracked | 5 (BTC, ETH, SOL, ADA, DOGE) |
| Update interval | Every 60 seconds |
| S3 prefixes | raw/, processed/, errors/, monitoring/, archive/, versions/, cache/ |
| Alert rules | 6 tiered rules |
| Backfill capacity | Up to 365 days |

## Production Patterns Implemented
1. Kafka streaming with producer/consumer pattern
2. PySpark micro-batch processing (1-minute windows)
3. Idempotent PostgreSQL inserts (ON CONFLICT DO NOTHING)
4. AWS S3 cold storage with date partitioning
5. Stream data validation (6 checks per event)
6. Slack tiered alerting (4 severity levels)
7. Dead letter queue with automatic replay
8. Pipeline monitoring and metrics tracking
9. Historical backfill up to 365 days
10. CI/CD with GitHub Actions (black, isort, flake8, mypy)
11. S3 caching with TTL expiry
12. Parallel processing with ThreadPoolExecutor
13. Retry logic with tenacity
14. Data versioning with rollback capability
15. Alert deduplication (30-minute suppression)
16. News sentiment analysis (BULLISH/BEARISH/NEUTRAL)
17. Market correlation matrix + Beta calculation
18. Crypto portfolio tracking
19. Technical indicators (SMA, RSI, Bollinger Bands, MACD)
20. Advanced tiered alerting with 6 rules

## Development Statistics
| Metric | Value |
|---|---|
| Days of development | 25 days |
| Total commits | 100+ |
| CI/CD workflows | 2 |
| Documentation files | 10+ |
| Architecture Decision Records | 9 |
