# Project Statistics — Crypto Streaming Pipeline

## Code Statistics
| Metric | Count |
|---|---|
| Python files | 15+ modules |
| Test files | 10 test files |
| Total tests | 173 passing |
| dbt models | 5 models |
| Kafka topics | 2 topics |
| ADRs | 12 architecture decisions |
| API endpoints | 7 REST endpoints |

## Pipeline Statistics
| Component | Details |
|---|---|
| Cryptos tracked | 5 (BTC, ETH, SOL, ADA, DOGE) |
| Update interval | Every 60 seconds |
| S3 prefixes | raw/, processed/, errors/, monitoring/, archive/ |
| PostgreSQL tables | 3 tables |
| Snowflake schemas | RAW, MARTS |
| dbt models | 5 (2 staging + 3 marts) |

## Production Patterns
1. Kafka streaming with DLQ pattern
2. PySpark micro-batch processing
3. Idempotent PostgreSQL inserts
4. AWS S3 cold storage with archiving
5. Stream data validation (6 checks)
6. Slack alerting for PUMP/DUMP
7. Dead letter queue with replay
8. Pipeline monitoring and metrics
9. Historical backfill up to 365 days
10. CI/CD with GitHub Actions
11. S3 Caching with TTL expiry
12. Parallel Processing with ThreadPoolExecutor
13. Batch PostgreSQL inserts
14. Retry Logic with Tenacity (3 attempts, exponential backoff)
15. Edge Case Test Coverage (boundary conditions)
16. Full Type Hints across all modules
17. Docstrings on every function
18. Data Observatory (freshness, completeness, anomaly detection)
19. Pipeline Health Scoring (0–100 score, A–F grade, HEALTHY/DEGRADED/CRITICAL status)
20. SLA Monitoring (6 thresholds, compliance tracking)
21. Quality Reporting (hourly + daily + trend analysis)
22. Quality Alerts (threshold-based alerting)
23. S3 Cost Optimization (Glacier archival)
24. Resource Monitoring (CPU, memory, disk)
25. Consumer Lag Estimation
