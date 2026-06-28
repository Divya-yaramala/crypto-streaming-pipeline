# ADR 010 - Data Observatory and Health Scoring

## Status
Accepted

## Context
As the pipeline grew to 20+ production patterns, a systematic way to observe data quality and quantify pipeline health was needed. Ad-hoc manual checks across freshness, completeness, and anomaly metrics were not scalable and provided no single view of overall pipeline state.

## Decision
Built a two-layer observability system:
1. **Data Observatory** (`consumer/data_observatory.py`) — measures data freshness (hours since last update), S3 path completeness (% of expected paths present), and price anomaly rate (% of prices beyond 3 standard deviations).
2. **Health Scorer** (`consumer/health_scorer.py`) — converts observatory metrics into a 0–100 score, letter grade (A–F), health status (HEALTHY / DEGRADED / CRITICAL), actionable recommendations, and a time-series trend (improving / stable / degrading).

## Reasons
- Weighted scoring (freshness 40%, completeness 40%, anomaly 20%) reflects real impact on downstream consumers
- Letter grades give an instantly readable signal without needing to interpret raw numbers
- Recommendations are generated from the same data, closing the gap between observability and action
- Health history saved to S3 enables trend analysis without a dedicated time-series database

## Consequences
- Observatory score is a lagging indicator — it reflects the last known state, not real-time stream health
- Anomaly detection uses a simple 3-sigma rule; correlated market moves (e.g., broad crypto crashes) may generate false positives
- No new dependencies added; uses only boto3 and the standard library
