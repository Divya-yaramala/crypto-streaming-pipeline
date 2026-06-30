# ADR 011 - SLA Monitoring Pattern

## Status
Accepted

## Context
As the pipeline grew to cover all critical steps from ingestion to dashboard refresh, there was no systematic way to track whether each step was meeting its performance targets. Without defined thresholds and compliance tracking, SLA breaches could go undetected until users noticed stale data or missing alerts.

## Decision
Built a custom SLA monitor (`consumer/sla_monitor.py`) with 6 predefined thresholds covering every critical pipeline step, metrics saved to S3 for audit trail, and compliance percentage calculated per SLA and in aggregate.

## Reasons
- 6 SLAs cover all critical pipeline steps (producer, consumer, S3, Postgres, dashboard, alerting)
- S3-based storage provides a durable audit trail without additional infrastructure
- Compliance percentage gives a single, clear metric to track over time
- No additional monitoring infrastructure needed — uses only boto3 and the standard library
- Thresholds are configurable per step, reflecting the different performance expectations of each component

## Consequences
- Must manually instrument each pipeline step to call `record_sla_metric` at the right moment
- No real-time SLA alerting — reporting is batch-based, checked on demand or on a schedule
- Future: add Slack alert when SLA is breached to enable real-time response
