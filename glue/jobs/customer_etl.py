import sys
import json
import boto3
import os
from pathlib import Path
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    transform_customer_data,
    calculate_data_quality_metrics,
)


from glue.libraries.audit import write_audit_log


# --------------------------------------------------
# 2. Read AWS Glue job parameters
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "curated_path",
        "rejected_path",
        "max_rejection_percentage",
        "audit_bucket",
    ],
)

input_path = args["input_path"]
curated_path = args["curated_path"]
rejected_path = args["rejected_path"]


# --------------------------------------------------
# 3. Configure audit location
# --------------------------------------------------

AUDIT_BUCKET = args["audit_bucket"]

audit_timestamp = datetime.now(timezone.utc).strftime(
    "%Y%m%dT%H%M%S%fZ"
)

AUDIT_KEY = (
    f"audit/customers/"
    f"{args['JOB_NAME']}/{audit_timestamp}.json"
)


# --------------------------------------------------
# 4. Initialize Spark and AWS Glue
# --------------------------------------------------

sc = SparkContext.getOrCreate()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(args["JOB_NAME"], args)

logger = glue_context.get_logger()


# --------------------------------------------------
# 5. Initialize configuration and metrics
# --------------------------------------------------

dq_metrics = None

MAX_REJECTION_PERCENTAGE = None


# --------------------------------------------------
# 6. Execute ETL pipeline
# --------------------------------------------------

try:

    # Read and validate the configured rejection threshold
    MAX_REJECTION_PERCENTAGE = float(
        args["max_rejection_percentage"]
    )

    if not 0 <= MAX_REJECTION_PERCENTAGE <= 100:

        raise ValueError(
            "max_rejection_percentage must be between 0 and 100"
        )

    # Read customer CSV
    customer_df = (
        spark.read
        .option("header", True)
        .schema(CUSTOMER_SCHEMA)
        .csv(input_path)
    )

    # Clean, validate, and split customer records
    valid_df, rejected_df = transform_customer_data(
        customer_df
    )

    # Calculate data quality metrics
    dq_metrics = calculate_data_quality_metrics(
        customer_df,
        valid_df,
        rejected_df,
    )

    # Log data quality metrics
    logger.info(
        f"Customer Data Quality Metrics: {dq_metrics}"
    )

    # Check record count reconciliation
    if dq_metrics["reconciliation_status"] == "FAILED":

        logger.error(
            f"Record count reconciliation failed: {dq_metrics}"
        )

        raise ValueError(
            "ETL failed: Source and processed record "
            "counts do not match"
        )

    # Check rejection threshold using exact counts
    if (
        dq_metrics["rejected_records"] * 100
        > MAX_REJECTION_PERCENTAGE
        * dq_metrics["source_records"]
    ):

        logger.error(
            f"Rejection threshold exceeded: {dq_metrics}"
        )

        raise ValueError(
            f"ETL failed: Rejection percentage "
            f"{dq_metrics['rejection_percentage']}% "
            f"exceeds allowed {MAX_REJECTION_PERCENTAGE}%"
        )

    # Write valid customers as Parquet
    valid_df.write.mode("overwrite").parquet(
        curated_path
    )

    # Write rejected customers as Parquet
    rejected_df.write.mode("overwrite").parquet(
        rejected_path
    )

    # Commit the Glue job after successful writes
    job.commit()

    # Prepare successful execution audit
    audit_data = {
        "job_name": args["JOB_NAME"],
        "execution_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_records": dq_metrics["source_records"],
        "valid_records": dq_metrics["valid_records"],
        "rejected_records": dq_metrics["rejected_records"],
        "rejection_percentage": dq_metrics[
            "rejection_percentage"
        ],
        "reconciliation_status": dq_metrics[
            "reconciliation_status"
        ],
        "max_rejection_percentage": MAX_REJECTION_PERCENTAGE,
        "job_status": "SUCCEEDED",
        "error_message": None,
    }

    # Write success audit to the configured destination
    audit_location = write_audit_log(
        AUDIT_BUCKET,
        AUDIT_KEY,
        audit_data,
    )

    # Log the actual audit destination
    logger.info(
        f"ETL success audit written to {audit_location}"
    )


# --------------------------------------------------
# 7. Handle ETL failures
# --------------------------------------------------

except Exception as error:

    logger.error(
        f"Customer ETL failed: {error}"
    )

    # Prepare failed execution audit
    failure_audit_data = {
        "job_name": args["JOB_NAME"],
        "execution_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_records": (
            dq_metrics["source_records"]
            if dq_metrics is not None
            else None
        ),
        "valid_records": (
            dq_metrics["valid_records"]
            if dq_metrics is not None
            else None
        ),
        "rejected_records": (
            dq_metrics["rejected_records"]
            if dq_metrics is not None
            else None
        ),
        "rejection_percentage": (
            dq_metrics["rejection_percentage"]
            if dq_metrics is not None
            else None
        ),
        "reconciliation_status": (
            dq_metrics["reconciliation_status"]
            if dq_metrics is not None
            else None
        ),
        "max_rejection_percentage": MAX_REJECTION_PERCENTAGE,
        "job_status": "FAILED",
        "error_message": str(error),
    }

    # Attempt to write failure audit
    try:

        audit_location = write_audit_log(
            AUDIT_BUCKET,
            AUDIT_KEY,
            failure_audit_data,
        )

        # Log the actual audit destination
        logger.info(
            f"ETL failure audit written to {audit_location}"
        )

    except Exception as audit_error:

        logger.error(
            f"Failed to write failure audit: {audit_error}"
        )

    # Re-raise the original error so Glue marks the job failed
    raise
