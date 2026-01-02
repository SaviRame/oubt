# Terraform S3 Bucket Setup

This directory contains the Terraform configuration to create a simple S3 bucket in AWS.

## Prerequisites

- Terraform installed (version ~> 5.0 for AWS provider)
- AWS CLI configured with credentials
- AWS account with permissions to create S3 buckets

## Usage

1. Navigate to this directory: `cd day-5/terraform`

2. Initialize Terraform: `terraform init`

3. Plan the deployment: `terraform plan`

4. Apply the configuration: `terraform apply`

5. To destroy: `terraform destroy`

## Variables

- `aws_region`: The AWS region (default: us-east-1)
- `bucket_name`: The name of the S3 bucket (must be globally unique)

## Outputs

- `bucket_name`: The name of the created S3 bucket
- `bucket_arn`: The ARN of the created S3 bucket

## Future Enhancements

- Add versioning, encryption, and other S3 features
- Integrate with GitHub Actions for CI/CD
- Add RDS and Lambda resources