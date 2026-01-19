# Glue Job: vendor-bronze-silver

This README captures the AWS CLI commands used to package, deploy, and run the Glue ETL job.

## Upload job script and dependency
```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/vendor-bronze-silver.py s3://week-4-oubt/code/glue/jobs/vendor-bronze-silver.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job
```bash
aws glue create-job \
  --name vendor-bronze-silver \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/vendor-bronze-silver.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip"
  }'
```

## Start a job run
```bash
aws glue start-job-run \
  --job-name vendor-bronze-silver \
  --arguments '{
    "--bronze_db":"bronze",
    "--silver_db":"silver",
    "--ingestion_date":"2026-01-18",
    "--silver_table_path": "s3://week-4-oubt/silver/mdm/vendor/"
  }'
```

## Check job status and logs
```bash
aws glue get-job-run \
  --job-name vendor-bronze-silver \
  --run-id <JOB_RUN_ID> \
  --predecessors-included

aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/output \
  --filter-pattern <JOB_RUN_ID>

aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/error \
  --filter-pattern <JOB_RUN_ID>
```


# Glue Job: vendor-silver-gold

## Upload job script and dependency
```bash
zip glue_utils.zip demo/src/glue_utils.py
aws s3 cp demo/src/vendor-silver-gold.py s3://week-4-oubt/code/glue/jobs/vendor-silver-gold.py
aws s3 cp glue_utils.zip s3://week-4-oubt/code/glue/libs/glue_utils.zip
```

## Create the Glue job
```bash
aws glue create-job \
  --name vendor-silver-gold \
  --role arn:aws:iam::765017559809:role/GlueServiceRole \
  --command Name=glueetl,ScriptLocation=s3://week-4-oubt/code/glue/jobs/vendor-silver-gold.py \
  --glue-version 5.1 \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "delta",
    "--extra-py-files": "s3://week-4-oubt/code/glue/libs/glue_utils.zip"
  }'
```

## Start a job run
```bash
aws glue start-job-run \
  --job-name vendor-silver-gold \
  --arguments '{
    "--gold_db":"gold",
    "--silver_db":"silver",
    "--ingestion_date":"2026-01-19"
  }'
```
