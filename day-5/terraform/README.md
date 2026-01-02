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

## CI/CD

This Terraform configuration is integrated with GitHub Actions for automated CI/CD.

- **Pull Requests to main**: Runs `terraform validate` and `terraform plan` to check for syntax errors and preview changes.
- **Push to main**: Runs `terraform validate`, `terraform plan`, and `terraform apply` to deploy changes automatically.

### Setup

1. In your GitHub repository settings, navigate to Secrets and Variables > Actions.
2. Add the following secrets:
   - `AWS_ACCESS_KEY_ID`: Your AWS access key ID.
   - `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key.
3. Ensure the AWS IAM user/role has permissions to create and manage S3 buckets in the `us-east-1` region.

The workflow is defined in `.github/workflows/terraform-cicd.yml` and only triggers on changes to files in this directory.

## Future Enhancements

- Add versioning, encryption, and other S3 features
- Add RDS and Lambda resources