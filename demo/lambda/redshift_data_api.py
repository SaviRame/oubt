import time

import boto3


redshift_data = boto3.client("redshift-data")


def lambda_handler(event, context):
    workgroup_name = event["workgroup_name"]
    database = event["database"]
    secret_arn = event["secret_arn"]
    sql = event["sql"]
    poll_seconds = int(event.get("poll_seconds", 5))
    timeout_seconds = int(event.get("timeout_seconds", 900))

    resp = redshift_data.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement_id = resp["Id"]
    deadline = time.time() + timeout_seconds

    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            return {"statement_id": statement_id, "status": status}
        if status in ("FAILED", "ABORTED"):
            error = desc.get("Error", "Unknown error")
            raise RuntimeError(f"Redshift statement {statement_id} {status}: {error}")
        if time.time() >= deadline:
            raise TimeoutError(f"Redshift statement {statement_id} timed out.")
        time.sleep(poll_seconds)
