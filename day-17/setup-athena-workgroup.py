#!/usr/bin/env python3
"""
Setup Athena Workgroup with Usage Controls for Day 17 Hands-on
This script creates an Athena workgroup with query usage controls for governance.
"""

import json
import logging

import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_athena_workgroup(athena_client, workgroup_name, result_location):
    """
    Create an Athena workgroup with usage controls.

    Args:
        athena_client: Boto3 Athena client
        workgroup_name: Name of the workgroup to create
        result_location: S3 location for query results

    Returns:
        bool: True if workgroup was created or already exists, False otherwise
    """
    try:
        # Check if workgroup already exists
        response = athena_client.get_work_group(WorkGroup=workgroup_name)
        logger.info(f"Workgroup {workgroup_name} already exists")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # Workgroup doesn't exist, create it
            try:
                response = athena_client.create_work_group(
                    Name=workgroup_name,
                    Configuration={
                        "ResultConfiguration": {"OutputLocation": result_location},
                        "EnforceWorkGroupConfiguration": True,
                        "PublishCloudWatchMetricsEnabled": True,
                        "BytesScannedCutoffPerQuery": 1073741824,  # 1 GB limit
                        "RequesterPaysEnabled": False,
                        "EngineVersion": {
                            "SelectedEngineVersion": "Athena engine version 3",
                            "EffectiveEngineVersion": "Athena engine version 3",
                        },
                    },
                    Description="Self-service analytics with usage controls for Day 17 hands-on",
                )
                logger.info(f"Successfully created workgroup {workgroup_name}")
                return True
            except ClientError as e:
                logger.error(f"Error creating workgroup {workgroup_name}: {e}")
                return False
        else:
            logger.error(f"Error checking workgroup {workgroup_name}: {e}")
            return False


def create_cloudwatch_dashboard(cloudwatch_client, dashboard_name):
    """
    Create a CloudWatch dashboard for monitoring Athena usage.

    Args:
        cloudwatch_client: Boto3 CloudWatch client
        dashboard_name: Name of the dashboard to create

    Returns:
        bool: True if dashboard was created or already exists, False otherwise
    """
    try:
        # Define dashboard body
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            [
                                "AWS/Athena",
                                "TotalExecutionTime",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [
                                "AWS/Athena",
                                "EngineExecutionTime",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [
                                "AWS/Athena",
                                "QueryPlanningTime",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [
                                "AWS/Athena",
                                "QueryQueueTime",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [
                                "AWS/Athena",
                                "ServiceProcessingTime",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-east-1",
                        "title": "Athena Query Execution Time",
                    },
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            [
                                "AWS/Athena",
                                "TotalBytesScanned",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [
                                "AWS/Athena",
                                "DataScannedInBytes",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-east-1",
                        "title": "Athena Data Scanned",
                    },
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            [
                                "AWS/Athena",
                                "QueryCount",
                                "WorkGroup",
                                "self-service-analytics",
                            ],
                            [".", "SuccessfulQueryCount", ".", "."],
                            [".", "FailedQueryCount", ".", "."],
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-east-1",
                        "title": "Athena Query Count",
                    },
                },
            ]
        }

        # Create dashboard
        response = cloudwatch_client.put_dashboard(
            DashboardName=dashboard_name, DashboardBody=json.dumps(dashboard_body)
        )
        logger.info(f"Successfully created CloudWatch dashboard {dashboard_name}")
        return True
    except ClientError as e:
        logger.error(f"Error creating CloudWatch dashboard {dashboard_name}: {e}")
        return False


def create_cloudwatch_alarms(cloudwatch_client):
    """
    Create CloudWatch alarms for monitoring Athena usage.

    Args:
        cloudwatch_client: Boto3 CloudWatch client

    Returns:
        bool: True if alarms were created successfully, False otherwise
    """
    try:
        # Alarm for high data usage
        cloudwatch_client.put_metric_alarm(
            AlarmName="athena-high-data-usage",
            AlarmDescription="Alarm when Athena data scanned exceeds threshold",
            ActionsEnabled=True,
            AlarmActions=["arn:aws:sns:us-east-1:123456789012:athena-alarms"],
            MetricName="TotalBytesScanned",
            Namespace="AWS/Athena",
            Statistic="Sum",
            Dimensions=[
                {"Name": "WorkGroup", "Value": "self-service-analytics"},
            ],
            Period=300,
            EvaluationPeriods=2,
            Threshold=5000000000,  # 5 GB
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
        )

        # Alarm for failed queries
        cloudwatch_client.put_metric_alarm(
            AlarmName="athena-failed-queries",
            AlarmDescription="Alarm when Athena queries fail",
            ActionsEnabled=True,
            AlarmActions=["arn:aws:sns:us-east-1:123456789012:athena-alarms"],
            MetricName="FailedQueryCount",
            Namespace="AWS/Athena",
            Statistic="Sum",
            Dimensions=[
                {"Name": "WorkGroup", "Value": "self-service-analytics"},
            ],
            Period=300,
            EvaluationPeriods=3,
            Threshold=1,
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
        )

        logger.info("Successfully created CloudWatch alarms for Athena monitoring")
        return True
    except ClientError as e:
        logger.error(f"Error creating CloudWatch alarms: {e}")
        return False


def main():
    """Main function to set up Athena workgroup and monitoring."""
    # Initialize clients
    athena_client = boto3.client("athena")
    cloudwatch_client = boto3.client("cloudwatch")

    # Configuration
    workgroup_name = "self-service-analytics"
    result_location = "s3://your-bucket/athena-results/"
    dashboard_name = "athena-monitoring-dashboard"

    # Create workgroup
    if create_athena_workgroup(athena_client, workgroup_name, result_location):
        logger.info(f"Workgroup {workgroup_name} is ready")
    else:
        logger.error(f"Failed to create workgroup {workgroup_name}")
        return

    # Create CloudWatch dashboard
    if create_cloudwatch_dashboard(cloudwatch_client, dashboard_name):
        logger.info(f"CloudWatch dashboard {dashboard_name} is ready")
    else:
        logger.error(f"Failed to create CloudWatch dashboard {dashboard_name}")

    # Create CloudWatch alarms
    if create_cloudwatch_alarms(cloudwatch_client):
        logger.info("CloudWatch alarms are ready")
    else:
        logger.error("Failed to create CloudWatch alarms")

    logger.info("Athena workgroup setup completed")


if __name__ == "__main__":
    main()
