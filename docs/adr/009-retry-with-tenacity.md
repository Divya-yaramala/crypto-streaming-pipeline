# ADR 009 - Retry Logic with Tenacity

## Status
Accepted

## Context
API calls to CoinGecko and database connections to PostgreSQL and Snowflake can fail transiently due to network hiccups, rate limits, or cold starts. A consistent retry strategy is needed across all external calls.

## Decision
Used the tenacity library for retry logic, applied via decorators on functions that call external services.

## Reasons
- Declarative retry with decorators keeps business logic clean
- Supports fixed, exponential, and random wait strategies
- Stop conditions configurable: max attempts, max elapsed time
- Already proven in the stock-pipeline project

## Consequences
- Adds latency on failure — each retry waits before the next attempt
- Must tune retry parameters per use case (API rate limits vs DB timeouts differ)
- Tenacity is already in requirements.txt; no new dependency added
