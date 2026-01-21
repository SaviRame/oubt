# Data Governance (Demo)

## 1) Purpose and scope
- Covers NYC taxi pipeline data in this demo: bronze, master, silver, gold, Redshift, QuickSight.
- Applies to dev/demo environments used in this repo.

## 2) Ownership and stewardship
- Data owner: Business stakeholder for taxi analytics.
- Data steward: Responsible for data definitions and quality rules.
- Data engineering: Responsible for ingestion, transformations, and monitoring.

## 3) Data classification and access
- Classification: Internal.
- Access controls:
  - S3 access via IAM roles and bucket policies.
  - Redshift access via admin user and IAM roles.
  - QuickSight access via principal permissions.

## 4) Data lineage
- Source → Bronze (S3) → Master (S3 Delta) → Silver (S3 Delta) → Gold (S3 Delta)
- Gold → Redshift Serverless → QuickSight dashboards
- Orchestration: Step Functions + Glue

## 5) Data quality rules
- Null counts and negative values for staging data:
  - `quality.stg_taxi_trips_nulls`
  - `quality.stg_taxi_trips_negative_values`
- Fact freshness: latest date within 7 days (if using gold metrics).
- Referential integrity checks (fact to dims) as needed.

## 6) Retention and lifecycle
- S3 lifecycle policies for bronze/master/silver/gold as required.
- Redshift retention driven by business needs and cost.

## 7) Change management
- CI/CD via GitHub Actions (OIDC).
- All schema changes captured in SQL under `demo/sql/`.
- Rollback: re-run prior scripts or restore from S3/Redshift snapshots.

## 8) Monitoring and alerts
- CloudWatch dashboard: `demo-oubt-pipeline`.
- Alarms:
  - Step Functions failures
  - Lambda errors (Redshift copy, QuickSight refresh)
- SNS notifications to `demo-oubt-alerts`.

## 9) Compliance and audit
- Glue and Step Functions logs in CloudWatch.
- Redshift audit via system tables and logs.
- QuickSight access tracked via IAM/QuickSight permissions.

## 10) Data dictionary (high level)
- Master dimensions: vendor, zone, payment_type, rate_code
- Gold fact: trip aggregates by date and dimension keys
