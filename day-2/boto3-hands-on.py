import json

import boto3

s3_client = boto3.client("s3")


# creating an s3 bucket
def bucket_creation():
    response = s3_client.create_bucket(Bucket="savitha-labs-boto3-day2")
    print(response)


# uploading files to the s3 bucket
def file_upload():
    s3_client.upload_file(
        r"C:\Users\jmsav\oubt\day-1\yellow_tripdata_2025-08.parquet",
        "savitha-labs-boto3-day2",
        "yellow_tripdata_2025-08.parquet",
    )
    s3_client.upload_file(
        r"C:\Users\jmsav\oubt\day-1\taxi_zone_lookup.csv",
        "savitha-labs-boto3-day2",
        "taxi_zone_lookup.csv",
    )


# policy to allow all access
def policy():
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "allow specific user full access",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::765017559809:user/boto3"},
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::savitha-labs-boto3-day2",
            }
        ],
    }

    policy = json.dumps(bucket_policy)
    s3_client.put_bucket_policy(Bucket="savitha-labs-boto3-day2", Policy=policy)


# calling the functions
# bucket_creation()
# file_upload()
# policy()

resp = s3_client.get_bucket_policy(Bucket="savitha-labs-boto3-day2")
print(json.loads(resp["Policy"]))

# adding tagging to the bucket
BUCKET = "savitha-labs-boto3-day2"


def tag_existing_object(key: str, owner: str, classification: str, domain: str):
    s3_client.put_object_tagging(
        Bucket=BUCKET,
        Key=key,
        Tagging={
            "TagSet": [
                {"Key": "Owner", "Value": owner},
                {"Key": "Classification", "Value": classification},
                {"Key": "Domain", "Value": domain},
            ]
        },
    )
    print(f"Tagged: {key}")


def show_tags(key: str):
    resp = s3_client.get_object_tagging(Bucket=BUCKET, Key=key)
    print(f"Tags for {key}: {resp['TagSet']}")


# Tag your two already-uploaded objects

tag_existing_object(
    "yellow_tripdata_2023-08.parquet", "savitha", "Internal", "Transport"
)

tag_existing_object("taxi_zone_lookup.csv", "savitha", "Internal", "Transport")


# Verify
show_tags("yellow_tripdata_2023-08.parquet")
show_tags("taxi_zone_lookup.csv")


# uploading files with prefix
def upload_file_with_prefix(file_path, prefix, bucket_name):
    s3_client.upload_file(
        Filename=file_path,
        Bucket=bucket_name,
        Key=prefix + file_path.split("\\")[-1],  # splitting \ and get the last elemet
    )


# calling the file upload with prefix function
#upload_file_with_prefix(
    #r"C:\Users\jmsav\Documents\prefix.txt", "day-2\\", "savitha-labs-boto3-day2")

# setting life cycle configuration only to the added prefix file
lifecycle_config = {
    "Rules": [
        {
            "ID": "expire-day2-after-30-days",
            "Status": "Enabled",
            "Filter": {"Prefix": "day-2/"},   # apply only to this prefix
            "Expiration": {"Days": 30},
        }
    ]
}

s3_client.put_bucket_lifecycle_configuration(
    Bucket=BUCKET,
    LifecycleConfiguration=lifecycle_config
)

print("Lifecycle rule set.")

# created the metdata manifest file and uploaded into the day2 s3 bucket