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


def test_dag_preserves_s3_lambda_glue_catalog_flow():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert "S3KeySensor(" in source
    assert "PythonOperator(" in source
    assert "PythonSensor(" in source
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
