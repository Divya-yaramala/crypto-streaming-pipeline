# ADR 004 - PostgreSQL over MongoDB

**Status:** Accepted

## Context

The pipeline needed a primary operational database for storing raw crypto price events and alerts, with support for idempotent inserts and aggregation queries.

## Decision

Chose PostgreSQL over MongoDB.

## Reasons

- **ACID compliance:** Financial data requires strong consistency guarantees — PostgreSQL's transactional model prevents partial writes and data corruption.
- **Idempotent inserts:** `ON CONFLICT (crypto_id, event_timestamp) DO NOTHING` gives built-in deduplication with no application-level logic required.
- **Strong typing:** Column-level types (`NUMERIC(20,8)`, `TIMESTAMP`) enforce data quality at the database layer, catching upstream errors early.
- **JOIN and aggregation support:** Rich SQL support for the dashboard's price history queries and the daily summary aggregations.

## Consequences

- Schema changes require migrations (ALTER TABLE), adding friction when event shapes evolve.
- Less flexible than a document store for storing variable-structure events (e.g., different fields per crypto source).
