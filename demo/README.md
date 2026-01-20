# Glue Job: vendor-master

This README captures the AWS CLI commands used to package, deploy, and run the Glue ETL job.

## Upload job script and dependency

```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/vendor_master.py s3://week-4-oubt/code/glue/jobs/vendor_master.py
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
