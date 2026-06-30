# ADR 012 - S3 Cost Optimization Strategy

## Status
Accepted

## Context
The pipeline writes raw price events to S3 every 60 seconds, generating hundreds of thousands of objects per month. Standard S3 storage costs grow continuously as data accumulates, and raw historical data is rarely accessed after the first 30 days once it has been processed and loaded into Snowflake.

## Decision
Built an automated S3 cost optimizer (`consumer/s3_optimizer.py`) that identifies objects older than 30 days across all major prefixes and moves them to Glacier storage class, with a savings calculator to justify the archival decision.

## Reasons
- Glacier storage is ~83% cheaper than Standard ($0.004/GB vs $0.023/GB)
- Raw crypto data is rarely accessed after 30 days — it exists in Snowflake MARTS for querying
- Automated candidate identification removes the need for manual S3 review
- Estimated savings calculation provides a clear cost justification before running archival
- No additional tooling needed — uses only the S3 CopyObject API with StorageClass override

## Consequences
- Glacier retrieval has a delay of minutes to hours depending on retrieval tier
- Teams must plan ahead if archived data is needed urgently (e.g., incident investigation)
- S3 Lifecycle Policies could replace this custom logic in future, but would reduce visibility into the decision
