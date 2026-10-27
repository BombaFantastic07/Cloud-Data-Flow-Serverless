"""S3-triggered Lambda handler that starts an AWS Glue ETL job."""

import json
import logging
import os
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]


def lambda_handler(event, context):
    """Start one Glue job run for each supported S3 object event."""
    started = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            logger.info("Skipping non-S3 record")
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        # Safety guard: process only CSV files under raw/.
        if not key.startswith("raw/") or not key.lower().endswith(".csv"):
            logger.info("Skipping object outside supported input: s3://%s/%s", bucket, key)
            continue

        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--INPUT_BUCKET": bucket,
                "--INPUT_KEY": key,
            },
        )
        job_run_id = response["JobRunId"]
        started.append({"bucket": bucket, "key": key, "job_run_id": job_run_id})
        logger.info("Started Glue job %s for s3://%s/%s; run=%s",
                    GLUE_JOB_NAME, bucket, key, job_run_id)

    return {
        "statusCode": 200,
        "body": json.dumps({"started_jobs": started}),
    }
