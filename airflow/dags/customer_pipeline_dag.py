"""Daily orchestration for the customer Glue ETL pipeline.

Airflow waits for the configured raw S3 object, then starts the existing
AWS Glue job. Data transformations remain in AWS Glue/PySpark.
"""
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


AWS_CONN_ID = os.getenv("AIRFLOW_AWS_CONN_ID", "aws_glue_conn")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
DATA_BUCKET = os.getenv("CUSTOMER_DATA_BUCKET")
INPUT_KEY = os.getenv(
    "CUSTOMER_INPUT_KEY",
    "raw/customers/customers_{{ ds_nodash }}.csv",
)
GLUE_JOB_NAME = os.getenv(
    "CUSTOMER_GLUE_JOB_NAME",
    "COADP-Customer-ETL-DEV",
)
MAX_REJECTION_PERCENTAGE = os.getenv(
    "MAX_REJECTION_PERCENTAGE",
    "20",
)

if not DATA_BUCKET:
    raise ValueError(
        "CUSTOMER_DATA_BUCKET must be set in the Airflow environment"
    )

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_s3_to_glue_pipeline",
    description="Wait for customer data in S3 and orchestrate AWS Glue ETL",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["customer", "aws-glue", "pyspark", "portfolio"],
) as dag:
    wait_for_customer_file = S3KeySensor(
        task_id="wait_for_customer_file",
        bucket_name=DATA_BUCKET,
        bucket_key=INPUT_KEY,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    run_customer_glue_etl = GlueJobOperator(
        task_id="run_customer_glue_etl",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        script_args={
            "--input_path": f"s3://{DATA_BUCKET}/{INPUT_KEY}",
            "--curated_path": (
                f"s3://{DATA_BUCKET}/curated/customers/run_date={{{{ ds }}}}/"
            ),
            "--rejected_path": (
                f"s3://{DATA_BUCKET}/rejected/customers/run_date={{{{ ds }}}}/"
            ),
            "--max_rejection_percentage": MAX_REJECTION_PERCENTAGE,
        },
        wait_for_completion=True,
        verbose=True,
        deferrable=False,
    )

    wait_for_customer_file >> run_customer_glue_etl
