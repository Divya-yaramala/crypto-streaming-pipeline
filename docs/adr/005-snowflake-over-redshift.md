# ADR 005 - Snowflake over AWS Redshift

**Status:** Accepted

## Context

The pipeline needed a cloud data warehouse to serve as the analytical layer on top of the S3 raw data, powering dbt transformations and MARTS tables.

## Decision

Chose Snowflake over AWS Redshift.

## Reasons

- **Compute and storage separation:** Snowflake's architecture scales compute independently of storage, avoiding over-provisioning costs at low query volumes.
- **First-class dbt integration:** dbt's Snowflake adapter is the most mature and best-documented adapter in the ecosystem.
- **Lower setup barrier:** Snowflake's free trial and web UI require no existing AWS infrastructure, making it accessible without pre-configured IAM roles or VPCs.
- **Ad-hoc query performance:** Snowflake's columnar storage and automatic clustering deliver strong performance for the exploratory queries used by the dashboard.

## Consequences

- Introduces a vendor outside the AWS ecosystem, adding a separate billing relationship and credential set.
- Data must transit from S3 to Snowflake via the sync job, adding latency compared to Redshift Spectrum querying S3 directly.
