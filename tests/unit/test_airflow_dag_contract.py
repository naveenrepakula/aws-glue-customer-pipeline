"""Lightweight contract checks for the Airflow DAG source.

These checks do not import Airflow or contact AWS, so they can run in the
existing local unit-test environment.
"""
from pathlib import Path


DAG_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "customer_pipeline_dag.py"
)


def test_dag_uses_sensor_then_glue_operator():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert "S3KeySensor(" in source
    assert "GlueJobOperator(" in source
    assert "wait_for_customer_file >> run_customer_glue_etl" in source


def test_dag_uses_configurable_aws_connection_and_no_embedded_credentials():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert 'os.getenv("AIRFLOW_AWS_CONN_ID", "aws_glue_conn")' in source
    assert "aws_conn_id=AWS_CONN_ID" in source
    assert "aws_access_key_id" not in source
    assert "aws_secret_access_key" not in source


def test_dag_has_retries_and_daily_schedule():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    assert '"retries": 2' in source
    assert "schedule=\"0 1 * * *\"" in source
    assert "catchup=False" in source
