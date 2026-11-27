"""Static contract checks for the Airflow orchestration DAG.

These checks do not import Airflow or contact AWS, so they run in the
lightweight CI test environment.
"""
from pathlib import Path

DAG_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "customer_pipeline_dag.py"
)
COMPOSE_SOURCE = (
    Path(__file__).resolve().parents[2] / "docker-compose.yaml"
).read_text(encoding="utf-8")


def test_dag_preserves_s3_lambda_glue_catalog_flow():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert "S3KeySensor(" in source
    assert "PythonOperator(" in source
    assert "PythonSensor(" in source
    assert "from airflow.sensors.python import PythonSensor" in source
    assert "GlueCrawlerOperator(" in source
    assert (
        "wait_for_customer_file\n"
        "        >> invoke_customer_lambda\n"
        "        >> wait_for_glue_completion\n"
        "        >> refresh_glue_catalog"
    ) in source


def test_dag_uses_configured_airflow_aws_connection():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert 'os.getenv("AIRFLOW_AWS_CONN_ID", "aws_glue_conn")' in source
    assert "AwsBaseHook(" in source
    assert "aws_access_key_id" not in source
    assert "aws_secret_access_key" not in source


def test_dag_has_daily_schedule_and_retries():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert '"retries": 2' in source
    assert 'schedule="0 1 * * *"' in source
    assert "catchup=False" in source


def test_glue_failure_has_bounded_lambda_retry():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert 'retry_count >= 2' in source
    assert 'FunctionName=TRIGGER_LAMBDA' in source
    assert 'glue_retry_count' in source


def test_lambda_response_contract_is_checked_before_monitoring():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert 'result.get("status") != "STARTED"' in source
    assert 'not result.get("job_run_id")' in source
    assert 'key="glue_job_run_id", value=result["job_run_id"]' in source
    assert 'InvocationType="RequestResponse"' in source
    assert 'response.get("FunctionError")' in source


def test_compose_uses_the_dag_aws_connection_and_dated_input_default():
    assert "AIRFLOW_AWS_CONN_ID: aws_glue_conn" in COMPOSE_SOURCE
    assert 'AIRFLOW_CONN_AWS_GLUE_CONN:' in COMPOSE_SOURCE
    assert (
        "CUSTOMER_INPUT_KEY: ${CUSTOMER_INPUT_KEY:-"
        "raw/customers/customers_{{ ds_nodash }}.csv}"
    ) in COMPOSE_SOURCE
    assert COMPOSE_SOURCE.count(
        "${USERPROFILE:-${HOME:-.}}/.aws:/home/airflow/.aws:ro"
    ) == 3
