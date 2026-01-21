# CloudWatch alarms

These alarms flag failures in the orchestration and Lambda helper functions.

## Create alarms

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name Demo-StepFunctions-ExecutionsFailed \
  --namespace AWS/States \
  --metric-name ExecutionsFailed \
  --dimensions Name=StateMachineArn,Value=arn:aws:states:us-east-1:765017559809:stateMachine:demo-glue-orchestration \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions arn:aws:sns:us-east-1:765017559809:demo-oubt-alerts

aws cloudwatch put-metric-alarm \
  --alarm-name Demo-Lambda-Redshift-Errors \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=redshift-data-api-runner \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions arn:aws:sns:us-east-1:765017559809:demo-oubt-alerts

aws cloudwatch put-metric-alarm \
  --alarm-name Demo-Lambda-QuickSight-Errors \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=quicksight-refresh \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions arn:aws:sns:us-east-1:765017559809:demo-oubt-alerts
```

## Delete alarms

```bash
aws cloudwatch delete-alarms \
  --alarm-names \
    Demo-StepFunctions-ExecutionsFailed \
    Demo-Lambda-Redshift-Errors \
    Demo-Lambda-QuickSight-Errors
```
