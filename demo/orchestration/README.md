# Glue orchestration with Step Functions + EventBridge

This demo wiring creates a Step Functions state machine that runs the Glue jobs in
order, then an EventBridge rule that triggers the state machine when new data
arrives under `s3://week-4-oubt/bronze/`.

## 1) Create the state machine

Create an IAM role for Step Functions with permission to run Glue jobs:

```bash
aws iam create-role \
  --role-name demo-stepfunctions-glue-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": { "Service": "states.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam attach-role-policy \
  --role-name demo-stepfunctions-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

Add Redshift Data API + Secrets Manager permissions for COPY:

```bash
aws iam put-role-policy \
  --role-name demo-stepfunctions-glue-role \
  --policy-name stepfunctions-redshift-data \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": "secretsmanager:GetSecretValue",
        "Resource": "arn:aws:secretsmanager:us-east-1:765017559809:secret:redshift-admin-demo-ipFq13"
      }
    ]
  }'
```

## Lambda helper for Redshift Data API

Create an IAM role for Lambda:

```bash
aws iam create-role \
  --role-name lambda-redshift-data-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": { "Service": "lambda.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam put-role-policy \
  --role-name lambda-redshift-data-role \
  --policy-name lambda-redshift-data \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": "secretsmanager:GetSecretValue",
        "Resource": "arn:aws:secretsmanager:us-east-1:765017559809:secret:redshift-admin-demo-ipFq13"
      },
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "*"
      }
    ]
  }'
```

Create the Lambda function:

```bash
zip redshift_data_api.zip demo/lambda/redshift_data_api.py
aws lambda create-function \
  --function-name redshift-data-api-runner \
  --runtime python3.11 \
  --handler redshift_data_api.lambda_handler \
  --role arn:aws:iam::765017559809:role/lambda-redshift-data-role \
  --zip-file fileb://redshift_data_api.zip \
  --timeout 900
```

Allow Step Functions to invoke the Lambda:

```bash
aws iam put-role-policy \
  --role-name demo-stepfunctions-glue-role \
  --policy-name stepfunctions-lambda-invoke \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": "arn:aws:lambda:us-east-1:765017559809:function:redshift-data-api-runner"
      }
    ]
  }'
```

## QuickSight refresh Lambda

Create an IAM role for QuickSight refresh:

```bash
aws iam create-role \
  --role-name lambda-quicksight-refresh-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": { "Service": "lambda.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam put-role-policy \
  --role-name lambda-quicksight-refresh-role \
  --policy-name lambda-quicksight-refresh \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "quicksight:CreateIngestion",
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "*"
      }
    ]
  }'
```

Create the Lambda function:

```bash
zip quicksight_refresh.zip demo/lambda/quicksight_refresh.py
aws lambda create-function \
  --function-name quicksight-refresh \
  --runtime python3.11 \
  --handler quicksight_refresh.lambda_handler \
  --role arn:aws:iam::765017559809:role/lambda-quicksight-refresh-role \
  --zip-file fileb://quicksight_refresh.zip \
  --timeout 300
```

Allow Step Functions to invoke the Lambda:

```bash
aws iam put-role-policy \
  --role-name demo-stepfunctions-glue-role \
  --policy-name stepfunctions-quicksight-invoke \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": "arn:aws:lambda:us-east-1:765017559809:function:quicksight-refresh"
      }
    ]
  }'
```

Create the state machine:

```bash
aws stepfunctions create-state-machine \
  --name demo-glue-orchestration \
  --definition file://demo/orchestration/state_machine.json \
  --role-arn arn:aws:iam::<ACCOUNT_ID>:role/demo-stepfunctions-glue-role
```

## 2) Create the EventBridge rule

Create the rule that listens to the bronze prefix:

```bash
aws events put-rule \
  --name bronze-s3-to-stepfunctions \
  --event-pattern file://demo/orchestration/eventbridge_rule.json
```

Add Step Functions as the target and pass a simple input. Update dates as needed.

```bash
aws events put-targets \
  --rule bronze-s3-to-stepfunctions \
  --targets '[
    {
      "Id": "StartGlueOrchestration",
      "Arn": "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:demo-glue-orchestration",
      "RoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/demo-eventbridge-stepfunctions-role",
      "InputTransformer": {
        "InputPathsMap": {
          "bucket": "$.detail.bucket.name",
          "key": "$.detail.object.key"
        },
        "InputTemplate": "{\"bucket\":\"<bucket>\",\"object_key\":\"<key>\",\"ingestion_date\":\"2026-01-18\",\"start_date\":\"2026-01-01\",\"end_date\":\"2026-01-31\"}"
      }
    }
  ]'
```

Create an EventBridge role that can start executions:

```bash
aws iam create-role \
  --role-name demo-eventbridge-stepfunctions-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": { "Service": "events.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam put-role-policy \
  --role-name demo-eventbridge-stepfunctions-role \
  --policy-name eventbridge-start-sfn \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "states:StartExecution",
        "Resource": "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:demo-glue-orchestration"
      }
    ]
  }'
```

## 3) Manual test

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:demo-glue-orchestration \
  --input '{"ingestion_date":"2026-01-18","start_date":"2026-01-01","end_date":"2026-01-31"}'
```

## Notes

- The state machine expects `ingestion_date`, `start_date`, and `end_date` in the input.
- If your S3 keys contain a date (for example: `bronze/trips/ingestion_date=2026-01-18/...`),
  you can replace the hard-coded dates in the EventBridge input template with a parsed value.
  For demos, it is fine to keep the fixed dates above.
