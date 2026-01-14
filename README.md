# Job Matching Data Pipe Line

An event-driven analytics pipeline built on BigQuery and dbt.
Raw job-matching user events are ingested into a raw layer, transformed into
canonical deduplicated events, and further aggregated into business-facing
metrics such as funnels, ranking quality, and A/B test results.

The project demonstrates warehouse-centric data pipeline design,
incremental transformations, and data quality testing.

## High Level pipeline diagram

```
┌──────────────────────────┐
│ Job Matching Product │
│ (Web / Mobile / Backend) │
└─────────────┬────────────┘
              │
              │ user events
              │ (search, impression, click, apply)
              ▼
┌──────────────────────────┐
│ Batch Event Generator │
│ (Python / Backend Logs) │
└─────────────┬────────────┘
              │
              │ NDJSON files
              │
              ▼
┌────────────────────────────────────────┐
│ 🟤 BigQuery – Raw Events (Bronze) │
│ job_matching_bronze.events_raw │
│ - append-only │
│ - duplicates allowed │
│ - ingestion-time partitioned │
└─────────────┬──────────────────────────┘
              │
              │ dbt (silver models)
              ▼
┌────────────────────────────────────────┐
│ ⚪ BigQuery – Canonical Events (Silver) │
│ job_matching_silver.events │
│ - deduplicated │
│ - validated schema │
│ - event-time partitioned │
└─────────────┬──────────────────────────┘
              │
              │ dbt (gold models)
              ▼
┌────────────────────────────────────────┐
│ 🟡 BigQuery – Analytics Tables (Gold) │
│ - funnel_daily │
│ - ranking_metrics_daily │
│ - ab_metrics_daily │
└─────────────┬──────────────────────────┘
              │
              │
      ┌───────┴─────────┐
      │                 │
      ▼                 ▼
┌──────────────┐ ┌─────────────────┐
│ Dashboards │ │ Analysis / ML │
│ (PMs) │ │ (Analysts, Eng) │
└──────────────┘ └─────────────────┘
```

## Time based workflow

```
T0 (all day)
└─ Users interact with job search & ranking

T1 (nightly)
└─ Batch job writes raw events
   → job_matching_bronze.events_raw

T2 (after ingestion)
└─ dbt run --select silver
   → job_matching_silver.events

T3 (after silver)
└─ dbt run --select gold
   → job_matching_gold.*

T4 (morning)
└─ PMs / engineers read dashboards & metrics
```

## Challenges & Key Learnings

1. BigQuery Streaming Insert Restriction (Free Tier)

Problem

Initial ingestion used BigQuery streaming inserts via the Python client. This failed due to free-tier restrictions that disallow streaming inserts.

Solution

Switched to a batch-oriented ingestion approach:

- Generated NDJSON files locally
- Loaded data using BigQuery load jobs programmatically
- Preserved replayability and cost efficiency

Learning
Batch load jobs are often preferable for reliability, cost control, and reprocessing, and are widely used in production systems.

2. dbt Profile and Schema Configuration Pitfalls

Problem

dbt generated unexpected dataset names (e.g. concatenated schemas) due to interaction between dataset in profiles.yml and +schema model configurations.

Solution

- Set a neutral dataset (dbt) in profiles.yml
- Controlled output datasets explicitly using +schema per model group (silver, gold)
- Cleaned dbt artifacts and recompiled models

Learning

In dbt-bigquery, the profile dataset and model schema configuration interact closely; clear separation avoids naming ambiguity.
