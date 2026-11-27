"""S3 event / Airflow entry point that starts the customer AWS Glue job."""
import json
import logging
import os
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")


def _extract_object(event):
    """Normalize an S3 notification or Airflow invocation into bucket/key/etag."""
    if "Records" in event:
        record = event["Records"][0]
        s3 = record["s3"]
        return {
            "bucket": s3["bucket"]["name"],
            "key": unquote_plus(s3["object"]["key"]),
            "etag": s3["object"].get("eTag", "").strip('"'),
        }
    return {
        "bucket": event["bucket"],
        "key": event["key"],
        "etag": event.get("etag", "").strip('"'),
    }


def lambda_handler(event, context):
    """Start Glue once per S3 object version and return the Glue run ID."""
    source = _extract_object(event)
    bucket, key, etag = source["bucket"], source["key"], source["etag"]

    if not key.startswith("raw/customers/") or not key.lower().endswith(".csv"):
        logger.info("Ignoring object outside raw/customers CSV prefix: s3://%s/%s", bucket, key)
        return {"status": "IGNORED", "reason": "not a customer raw CSV"}

    job_name = os.environ["GLUE_JOB_NAME"]
    table = dynamodb.Table(os.environ["IDEMPOTENCY_TABLE"])
    object_id = f"{bucket}/{key}#{etag or 'no-etag'}"
    now = int(time.time())

    try:
        table.put_item(
            Item={
                "object_id": object_id,
                "status": "STARTING",
                "created_at": now,
                "expires_at": now + 30 * 24 * 60 * 60,
            },
            ConditionExpression="attribute_not_exists(object_id)",
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        existing = table.get_item(Key={"object_id": object_id}).get("Item", {})
        if existing.get("job_run_id"):
            return {
                "status": existing.get("status", "STARTED"),
                "job_run_id": existing["job_run_id"],
                "duplicate": True,
            }
        raise RuntimeError("A concurrent invocation is already starting this Glue run; retry shortly")

    run_date = key.rsplit("/", 1)[-1].replace("customers_", "").replace(".csv", "")
    if len(run_date) != 8 or not run_date.isdigit():
        run_date = time.strftime("%Y%m%d", time.gmtime())

    args = {
        "--input_path": f"s3://{bucket}/{key}",
        "--curated_path": f"s3://{bucket}/curated/customers/run_date={run_date}/",
        "--rejected_path": f"s3://{bucket}/rejected/customers/run_date={run_date}/",
        "--max_rejection_percentage": os.getenv("MAX_REJECTION_PERCENTAGE", "20"),
        "--audit_bucket": bucket,
    }

    try:
        response = glue.start_job_run(JobName=job_name, Arguments=args)
        run_id = response["JobRunId"]
        table.update_item(
            Key={"object_id": object_id},
            UpdateExpression="SET #s = :started, job_run_id = :run_id",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":started": "STARTED", ":run_id": run_id},
        )
        logger.info("Started Glue job %s run %s for s3://%s/%s", job_name, run_id, bucket, key)
        return {"status": "STARTED", "job_run_id": run_id, "duplicate": False}
    except Exception:
        table.update_item(
            Key={"object_id": object_id},
            UpdateExpression="SET #s = :failed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "START_FAILED"},
        )
        raise
