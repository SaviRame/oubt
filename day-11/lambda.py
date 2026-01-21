import boto3
from urllib.parse import urlparse

s3 = boto3.client("s3")

def parse_s3_uri(s3_uri):
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

def handler(event, context):
    zones_uri = event["zones_input"]
    transactions_uri = event["transactions_input"]

    zones_bucket, zones_key = parse_s3_uri(zones_uri)
    tx_bucket, tx_key = parse_s3_uri(transactions_uri)

    # Check zones CSV
    zones_head = s3.head_object(Bucket=zones_bucket, Key=zones_key)
    zones_size = zones_head["ContentLength"]
    if zones_size <= 0:
        raise Exception("Zones CSV file is empty")

    # Check transactions parquet
    tx_head = s3.head_object(Bucket=tx_bucket, Key=tx_key)
    tx_size = tx_head["ContentLength"]
    if tx_size <= 0:
        raise Exception("Transactions parquet file is empty")

    return {
        "ok": True,
        "zones_size": zones_size,
        "transactions_size": tx_size
    }
