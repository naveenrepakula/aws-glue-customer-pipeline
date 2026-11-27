"""Local Spark integration test: CSV -> validation -> Parquet -> audit."""
import json
from pathlib import Path

from pyspark.sql import SparkSession

from glue.libraries.audit import write_audit_log
from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    calculate_data_quality_metrics,
    transform_customer_data,
)


def test_sample_customer_csv_end_to_end(tmp_path, monkeypatch):
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("customer-pipeline-local-integration")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        repository_root = Path(__file__).resolve().parents[2]
        input_file = repository_root / "sample_data" / "customers.csv"
        source_df = (
            spark.read.option("header", True)
            .schema(CUSTOMER_SCHEMA)
            .csv(str(input_file))
        )

        valid_df, rejected_df = transform_customer_data(source_df)
        metrics = calculate_data_quality_metrics(source_df, valid_df, rejected_df)

        assert metrics["source_records"] == 7
        assert metrics["valid_records"] == 5
        assert metrics["rejected_records"] == 2
        assert metrics["reconciliation_status"] == "PASSED"
        assert metrics["rejection_percentage"] == 28.57

        curated_path = tmp_path / "curated"
        rejected_path = tmp_path / "rejected"
        valid_df.write.mode("overwrite").parquet(str(curated_path))
        rejected_df.write.mode("overwrite").parquet(str(rejected_path))

        curated_readback = spark.read.parquet(str(curated_path))
        rejected_readback = spark.read.parquet(str(rejected_path))
        assert curated_readback.count() == 5
        assert rejected_readback.count() == 2
        assert "rejection_reason" not in curated_readback.columns
        assert "rejection_reason" in rejected_readback.columns

        monkeypatch.setenv("COADP_LOCAL_AUDIT_DIR", str(tmp_path / "audit"))
        audit_key = "audit/customers/test-run.json"
        audit = {
            **metrics,
            "job_status": "SUCCEEDED",
            "curated_path": str(curated_path),
            "rejected_path": str(rejected_path),
        }
        audit_location = write_audit_log("local-test-bucket", audit_key, audit)
        audit_payload = json.loads(Path(audit_location).read_text(encoding="utf-8"))
        assert audit_payload["job_status"] == "SUCCEEDED"
        assert audit_payload["reconciliation_status"] == "PASSED"
    finally:
        spark.stop()
