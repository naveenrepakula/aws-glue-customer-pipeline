"""Offline tests for the Lambda S3/EventBridge -> Glue trigger."""
import importlib.util
import os
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("GLUE_JOB_NAME", "test-customer-glue")
os.environ.setdefault("IDEMPOTENCY_TABLE", "test-idempotency")

HANDLER_PATH = (
    Path(__file__).resolve().parents[2]
    / "lambda"
    / "trigger_glue"
    / "handler.py"
)
SPEC = importlib.util.spec_from_file_location("customer_lambda_handler", HANDLER_PATH)
handler_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(handler_module)


class FakeTable:
    def __init__(self, existing=None):
        self.item = existing
        self.updates = []

    def put_item(self, Item, ConditionExpression):
        if self.item is not None:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
                "PutItem",
            )
        self.item = Item

    def get_item(self, Key):
        return {"Item": self.item} if self.item else {}

    def update_item(self, **kwargs):
        self.updates.append(kwargs)
        values = kwargs.get("ExpressionAttributeValues", {})
        if self.item is not None and "REMOVE job_run_id" in kwargs.get("UpdateExpression", ""):
            self.item.pop("job_run_id", None)
            if ":run_id" in values:
                self.item["job_run_id"] = values[":run_id"]
            if ":started" in values:
                self.item["status"] = values[":started"]
            if ":failed" in values:
                self.item["status"] = values[":failed"]


class FakeDynamoDB:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        assert name == "test-idempotency"
        return self._table


class FakeGlue:
    def __init__(self, prior_state="RUNNING"):
        self.calls = []
        self.prior_state = prior_state

    def get_job_run(self, **kwargs):
        return {"JobRun": {"JobRunState": self.prior_state}}

    def start_job_run(self, **kwargs):
        self.calls.append(kwargs)
        return {"JobRunId": "jr_test_123"}


def test_extracts_eventbridge_s3_event():
    event = {
        "source": "aws.s3",
        "detail": {
            "bucket": {"name": "customer-data"},
            "object": {"key": "raw/customers/customer+file.csv", "etag": "abc123"},
        },
    }
    assert handler_module._extract_object(event) == {
        "bucket": "customer-data",
        "key": "raw/customers/customer file.csv",
        "etag": "abc123",
    }


def test_starts_glue_with_expected_paths_and_audit_bucket(monkeypatch):
    table = FakeTable()
    glue = FakeGlue()
    monkeypatch.setattr(handler_module, "dynamodb", FakeDynamoDB(table))
    monkeypatch.setattr(handler_module, "glue", glue)

    result = handler_module.lambda_handler(
        {
            "bucket": "customer-data",
            "key": "raw/customers/customers_20261127.csv",
            "etag": "etag-001",
        },
        None,
    )

    assert result == {
        "status": "STARTED",
        "job_run_id": "jr_test_123",
        "duplicate": False,
    }
    args = glue.calls[0]["Arguments"]
    assert args["--input_path"] == "s3://customer-data/raw/customers/customers_20261127.csv"
    assert args["--curated_path"] == "s3://customer-data/curated/customers/run_date=20261127/"
    assert args["--rejected_path"] == "s3://customer-data/rejected/customers/run_date=20261127/"
    assert args["--audit_bucket"] == "customer-data"
    assert table.item["job_run_id"] == "jr_test_123"


def test_duplicate_event_returns_existing_glue_run(monkeypatch):
    table = FakeTable(
        {
            "object_id": "customer-data/raw/customers/customers_20261127.csv#etag-001",
            "status": "STARTED",
            "job_run_id": "jr_existing",
        }
    )
    glue = FakeGlue()
    monkeypatch.setattr(handler_module, "dynamodb", FakeDynamoDB(table))
    monkeypatch.setattr(handler_module, "glue", glue)

    result = handler_module.lambda_handler(
        {
            "bucket": "customer-data",
            "key": "raw/customers/customers_20261127.csv",
            "etag": "etag-001",
        },
        None,
    )

    assert result["duplicate"] is True
    assert result["job_run_id"] == "jr_existing"
    assert glue.calls == []


def test_ignores_non_customer_prefix(monkeypatch):
    table = FakeTable()
    glue = FakeGlue()
    monkeypatch.setattr(handler_module, "dynamodb", FakeDynamoDB(table))
    monkeypatch.setattr(handler_module, "glue", glue)

    result = handler_module.lambda_handler(
        {"bucket": "customer-data", "key": "archive/old.csv"},
        None,
    )

    assert result["status"] == "IGNORED"
    assert glue.calls == []


def test_failed_glue_run_can_be_restarted_for_same_object(monkeypatch):
    table = FakeTable(
        {
            "object_id": "customer-data/raw/customers/customers_20261127.csv#etag-001",
            "status": "STARTED",
            "job_run_id": "jr_failed",
        }
    )
    glue = FakeGlue(prior_state="FAILED")
    monkeypatch.setattr(handler_module, "dynamodb", FakeDynamoDB(table))
    monkeypatch.setattr(handler_module, "glue", glue)

    result = handler_module.lambda_handler(
        {
            "bucket": "customer-data",
            "key": "raw/customers/customers_20261127.csv",
            "etag": "etag-001",
        },
        None,
    )

    assert result["status"] == "STARTED"
    assert result["job_run_id"] == "jr_test_123"
    assert result["duplicate"] is False
    assert len(glue.calls) == 1
