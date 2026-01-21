import time
import uuid

import boto3


quicksight = boto3.client("quicksight")


def lambda_handler(event, context):
    account_id = event["account_id"]
    dataset_ids = event["dataset_ids"]
    results = []

    for dataset_id in dataset_ids:
        ingestion_id = f"{int(time.time())}-{uuid.uuid4()}"
        quicksight.create_ingestion(
            AwsAccountId=account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        results.append({"dataset_id": dataset_id, "ingestion_id": ingestion_id})

    return {"ingestions": results}
