# ADR 013 - Structured Logging Pattern

## Status
Accepted

## Context
As the pipeline grew to 25+ modules, plain-text log lines became hard to filter and correlate across pipeline steps. Debugging an issue required manually scanning logs from multiple modules, and there was no consistent format for shipping logs to external observability tools like CloudWatch or Datadog.

## Decision
Built a two-layer logging system: a structured JSON logger (`consumer/structured_logger.py`) for real-time log output, and an audit logger (`consumer/audit_logger.py`) that persists every pipeline event to S3 as an immutable audit trail.

## Reasons
- JSON logs are directly parseable by CloudWatch Logs Insights, Datadog, and the ELK stack without custom parsers
- Structured format with `level`, `module`, `crypto_id`, and `extra` fields enables precise filtering and aggregation
- The `crypto_id` field allows per-crypto log correlation across all pipeline steps
- Metric logs provide a drop-in integration point for custom CloudWatch metrics or Datadog gauges
- S3-based audit trail provides tamper-evident compliance evidence with no additional infrastructure

## Consequences
- JSON log lines are more verbose than plain text, increasing log volume slightly
- Effective querying requires a log aggregation tool — raw terminal output is less readable
- Future: ship structured logs to AWS CloudWatch Logs for production monitoring and alerting
