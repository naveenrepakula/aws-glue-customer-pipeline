"""Scheduled Airflow control plane for S3 -> Lambda -> Glue -> Catalog.

S3 ObjectCreated events still invoke Lambda directly. Airflow independently
schedules a daily run, invokes the same Lambda entry point for its input key,
waits for the Glue run, then refreshes the Glue Catalog crawler.
"""
import json
import os
from datetime import timedelta

import boto3
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

AWS_CONN_ID = os.getenv("AIRFLOW_AWS_CONN_ID", "aws_glue_conn")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
DATA_BUCKET = os.getenv("CUSTOMER_DATA_BUCKET")
INPUT_KEY = os.getenv("CUSTOMER_INPUT_KEY", "raw/customers/customers_{{ ds_nodash }}.csv")
TRIGGER_LAMBDA = os.getenv("CUSTOMER_TRIGGER_LAMBDA", "customer-pipeline-dev-trigger-glue")
GLUE_JOB_NAME = os.getenv("CUSTOMER_GLUE_JOB_NAME", "COADP-Customer-ETL-DEV")
GLUE_CRAWLER_NAME = os.getenv("CUSTOMER_GLUE_CRAWLER_NAME", "customer-pipeline-dev-customer-crawler")

if not DATA_BUCKET:
    raise ValueError("CUSTOMER_DATA_BUCKET must be set in the Airflow environment")


def start_pipeline_from_lambda(bucket, key, **context):
    """Invoke the same Lambda used by S3 ObjectCreated notifications."""
    client = boto3.client("lambda", region_name=AWS_REGION)
    response = client.invoke(
        FunctionName=TRIGGER_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps({"bucket": bucket, "key": key}).encode("utf-8"),
    )
    result = json.loads(response["Payload"].read().decode("utf-8"))
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda invocation failed: {result}")
    if result.get("status") != "STARTED" or not result.get("job_run_id"):
        raise RuntimeError(f"Lambda did not start a new Glue run: {result}")
    context["ti"].xcom_push(key="glue_job_run_id", value=result["job_run_id"])
    return result["job_run_id"]


def glue_run_finished(job_name, **context):
    """Poll Glue status for the run started by the Lambda task."""
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="invoke_customer_lambda", key="glue_job_run_id")
    if not run_id:
        raise RuntimeError("Missing Glue JobRunId from Lambda invocation")
    client = boto3.client("glue", region_name=AWS_REGION)
    run = client.get_job_run(
        JobName=job_name, RunId=run_id, PredecessorsIncluded=False
    )["JobRun"]
    state = run["JobRunState"]
    if state == "SUCCEEDED":
        return True
    if state in {"FAILED", "TIMEOUT", "ERROR", "STOPPED", "EXPIRED"}:
        raise RuntimeError(
            f"Glue job {job_name} run {run_id} ended with {state}: "
            f"{run.get('ErrorMessage', 'No Glue error message')}"
        )
    return False


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_s3_lambda_glue_pipeline",
    description="Schedule and monitor S3 -> Lambda -> Glue -> Catalog",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["customer", "s3", "lambda", "aws-glue", "pyspark", "athena"],
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

    invoke_customer_lambda = PythonOperator(
        task_id="invoke_customer_lambda",
        python_callable=start_pipeline_from_lambda,
        op_kwargs={"bucket": DATA_BUCKET, "key": INPUT_KEY},
    )

    wait_for_glue_completion = PythonSensor(
        task_id="wait_for_glue_completion",
        python_callable=glue_run_finished,
        op_kwargs={"job_name": GLUE_JOB_NAME},
        poke_interval=60,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    refresh_glue_catalog = GlueCrawlerOperator(
        task_id="refresh_glue_catalog",
        config={"Name": GLUE_CRAWLER_NAME},
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        deferrable=False,
    )

    (
        wait_for_customer_file
        >> invoke_customer_lambda
        >> wait_for_glue_completion
        >> refresh_glue_catalog
    )
