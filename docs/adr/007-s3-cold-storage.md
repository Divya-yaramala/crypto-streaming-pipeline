# ADR 007 - S3 Cold Storage Strategy

**Status:** Accepted

## Context

The pipeline generates a continuous stream of price events, alerts, and aggregations. Keeping all data in PostgreSQL indefinitely would grow costs and degrade query performance over time.

## Decision

Archive raw events to S3 after 7 days using date-partitioned prefixes.

## Reasons

- **Cost efficiency:** S3 Standard storage costs a fraction of PostgreSQL disk, especially for data older than 7 days that is rarely queried.
- **DLQ replay support:** S3-backed storage enables the dead letter queue pattern — failed events are written to `errors/crypto/YYYY/MM/DD/` and can be replayed without re-hitting the API.
- **Efficient querying:** Date partitioning (`raw/crypto/YYYY/MM/DD/crypto_id/`) allows targeted retrieval by date range without scanning the full bucket.
- **Minimal dependencies:** boto3 was already in the project for AWS integration, so no new dependency was introduced.

## Consequences

- Query latency for archived data is higher than PostgreSQL (S3 GET vs index scan).
- Custom archiving logic must be maintained — S3 Lifecycle rules could automate tiering but were not implemented to keep the solution self-contained.
