# Glue Job: vendor-master

This README captures the AWS CLI commands used to package, deploy, and run the Glue ETL job.

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/vendor_master.py s3://week-4-oubt/code/glue/jobs/vendor_master.py
aws s3 cp demo/src/zone_master.py s3://week-4-oubt/code/glue/jobs/zone_master.py
aws s3 cp demo/src/rate_code_ref.py s3://week-4-oubt/code/glue/jobs/rate_code_ref.py
aws s3 cp demo/src/payment_type_ref.py s3://week-4-oubt/code/glue/jobs/payment_type_ref.py
aws s3 cp demo/src/trips_processed.py s3://week-4-oubt/code/glue/jobs/trips_processed.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job

```bash
aws glue create-job \
  --name vendor-master \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/vendor_master.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--additional-python-modules": "recordlinkage",
    "--bronze_db": "bronze",
    "--master_db": "master",
    "--debug_db": "debug",
    "--output_path": "s3://week-4-oubt/master/",
    "--debug_output_path": "s3://week-4-oubt/debug/"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name vendor-master \
  --arguments '{
    "--ingestion_date": "2026-01-18"
  }'
```

# Glue Job: zone-master

## Create the Glue job

```bash
aws glue create-job \
  --name zone-master \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/zone_master.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--bronze_db": "bronze",
    "--master_db": "master",
    "--output_path": "s3://week-4-oubt/master/"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name zone-master \
  --arguments '{
    "--ingestion_date": "2026-01-18"
  }'
```

# Glue Job: rate-code-ref

## Create the Glue job

```bash
aws glue create-job \
  --name rate-code-ref \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/rate_code_ref.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--bronze_db": "bronze",
    "--master_db": "master",
    "--output_path": "s3://week-4-oubt/master/"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name rate-code-ref \
  --arguments '{
    "--ingestion_date": "2026-01-18"
  }'
```

# Glue Job: payment-type-ref

## Create the Glue job

```bash
aws glue create-job \
  --name payment-type-ref \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/payment_type_ref.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--bronze_db": "bronze",
    "--master_db": "master",
    "--output_path": "s3://week-4-oubt/master/"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name payment-type-ref \
  --arguments '{
    "--ingestion_date": "2026-01-18"
  }'
```

# Glue Job: trips-processed

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/trips_processed.py s3://week-4-oubt/code/glue/jobs/trips_processed.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job

```bash
aws glue create-job \
  --name trips-processed \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/trips_processed.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--bronze_db": "bronze",
    "--master_db": "master",
    "--silver_db": "silver",
    "--output_path": "s3://week-4-oubt/silver/"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name trips-processed \
  --arguments '{
    "--ingestion_date": "2026-01-18"
  }'
```

# Glue Job: fact-trips

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/fact_trips.py s3://week-4-oubt/code/glue/jobs/fact_trips.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Delete current table (delta)

```bash
aws glue delete-table --database-name gold --name fact_trips
aws s3 rm s3://week-4-oubt/gold/fact_trips --recursive
```

## Create the Glue job

```bash
aws glue create-job \
  --name fact-trips \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/fact_trips.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--silver_db": "silver",
    "--gold_db": "gold",
    "--output_path": "s3://week-4-oubt/gold/",
    "--start_date": "2026-01-01",
    "--end_date": "2026-01-31"
  }'
```

## Start a job run

```bash
aws glue start-job-run \
  --job-name fact-trips \
  --arguments '{  
    "--start_date": "2025-08-01",
    "--end_date": "2025-08-31"
  }'
```

# Glue Job: export-master-parquet

Exports master tables to Parquet for Redshift COPY.

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/export_master_parquet.py s3://week-4-oubt/code/glue/jobs/export_master_parquet.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job

```bash
aws glue create-job \
  --name export-master-parquet \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/export_master_parquet.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--master_db": "master",
    "--output_path": "s3://week-4-oubt/staging/redshift/master",
    "--current_only": "true"
  }'
```

# Glue Job: export-fact-parquet

Exports fact_trips to Parquet for Redshift COPY.

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/export_fact_parquet.py s3://week-4-oubt/code/glue/jobs/export_fact_parquet.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job

```bash
aws glue create-job \
  --name export-fact-parquet \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/export_fact_parquet.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip",
    "--gold_db": "gold",
    "--output_path": "s3://week-4-oubt/staging/redshift/gold"
  }'
```

# Redshift tables (DDL)

Run this once in the Redshift query editor:

```sql
-- demo/sql/redshift-ddl.sql
```
