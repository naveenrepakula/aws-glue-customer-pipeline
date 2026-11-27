from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    add_rejection_reason,
    clean_customer_data,
    split_valid_rejected,
    transform_customer_data,
    calculate_data_quality_metrics,
    validate_data_quality,
)


def test_clean_customer_data():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-customer-transformations")
        .getOrCreate()
    )

    input_data = [
        (
            " 1001 ",
            " Naveen ",
            " Repakula ",
            " naveen@example.com ",
            " 9876543210 ",
            " India ",
        )
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "country",
    ]
    df = spark.createDataFrame(input_data, columns)

    result_df = clean_customer_data(df)

    result = result_df.collect()[0]

    assert result["customer_id"] == "1001"
    assert result["first_name"] == "Naveen"
    assert result["last_name"] == "Repakula"
    assert result["email"] == "naveen@example.com"
    assert result["phone"] == "9876543210"
    assert result["country"] == "India"


def test_missing_email_rejection():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-customer-validation")
        .getOrCreate()
    )

    input_data = [
        (
            "1004",
            "Anil",
            "Kumar",
            None,
            "9876501234",
            "India",
            date(2026, 10, 3),
        )
    ]

    df = spark.createDataFrame(input_data, schema=CUSTOMER_SCHEMA)

    result_df = add_rejection_reason(df)

    result = result_df.collect()[0]

    assert result["rejection_reason"] == "MISSING_EMAIL"


def test_split_valid_rejected():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-customer-split")
        .getOrCreate()
    )

    input_data = [
        ("1001", None),
        ("1004", "MISSING_EMAIL"),
    ]

    columns = [
        "customer_id",
        "rejection_reason",
    ]

    df = spark.createDataFrame(input_data, columns)

    valid_df, rejected_df = split_valid_rejected(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 1


def test_transform_customer_data():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-customer-transformation")
        .getOrCreate()
    )

    input_data = [
        (
            "1001",
            " Naveen ",
            " Repakula ",
            "naveen@example.com",
            "9876543210",
            " India ",
            date(2026, 10, 1),
        ),
        (
            "1004",
            "Anil",
            "Kumar",
            None,
            "9876501234",
            "India",
            date(2026, 10, 3),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    valid_df, rejected_df = transform_customer_data(df)

    # Check number of valid and rejected records
    assert valid_df.count() == 1
    assert rejected_df.count() == 1

    # Get the records so we can verify their values
    valid_customer = valid_df.collect()[0]
    rejected_customer = rejected_df.collect()[0]

    # Verify valid customer
    assert valid_customer["customer_id"] == "1001"
    assert valid_customer["first_name"] == "Naveen"
    assert valid_customer["last_name"] == "Repakula"
    assert valid_customer["country"] == "India"

    # Verify rejected customer
    assert rejected_customer["customer_id"] == "1004"
    assert rejected_customer["rejection_reason"] == "MISSING_EMAIL"


def test_invalid_email_format_rejection():
    # ARRANGE: Prepare test data
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-invalid-email")
        .getOrCreate()
    )

    input_data = [
        (
            "1002",
            "Rahul",
            "Kumar",
            "rahulgmail.com",
            "9876543210",
            "India",
            date(2026, 10, 10),
        )
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )
    expected_reason = "INVALID_EMAIL_FORMAT"

    # ACT: Execute the transformation
    result_df = add_rejection_reason(df)
    actual_reason = result_df.collect()[0]["rejection_reason"]

    # ASSERT: Verify expected vs actual
    assert actual_reason == expected_reason


def test_duplicate_customer_id_rejection():
    # ARRANGE: Prepare duplicate customer records
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-duplicate-customers")
        .getOrCreate()
    )

    input_data = [
        (
            "1001", "Rahul", "Kumar",
            "rahul@gmail.com", "9876543210",
            "India", date(2026, 10, 10),
        ),
        (
            "1001", "Kiran", "Rao",
            "kiran@gmail.com", "9876543211",
            "India", date(2026, 10, 11),
        ),
        (
            "1002", "Naveen", "Repakula",
            "naveen@gmail.com", "9876543212",
            "India", date(2026, 10, 12),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_duplicate_count = 2
    expected_reason = "DUPLICATE_CUSTOMER_ID"

    # ACT: Execute the complete transformation
    valid_df, rejected_df = transform_customer_data(df)

    duplicate_df = rejected_df.filter(
        col("rejection_reason") == expected_reason
    )

    actual_duplicate_count = duplicate_df.count()

    # ASSERT: Verify duplicate handling
    assert actual_duplicate_count == expected_duplicate_count
    assert valid_df.count() == 1


def test_multiple_rejection_reasons():
    # ARRANGE: Customer has two validation errors
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-multiple-rejection-reasons")
        .getOrCreate()
    )

    input_data = [
        (
            "1003",
            "",                    # Missing first name
            "Kumar",
            "invalidemail.com",    # Invalid email format
            "9876543210",
            "India",
            date(2026, 10, 10),
        )
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_reasons = {
        "MISSING_FIRST_NAME",
        "INVALID_EMAIL_FORMAT",
    }

    # ACT: Run the customer transformation
    valid_df, rejected_df = transform_customer_data(df)

    actual_reasons = set(
        rejected_df.collect()[0]["rejection_reasons"]
    )

    # ASSERT: Both errors must be captured
    assert actual_reasons == expected_reasons
    assert valid_df.count() == 0


def test_duplicate_id_with_multiple_rejection_reasons():
    # ARRANGE
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-duplicate-multiple-errors")
        .getOrCreate()
    )

    input_data = [
        (
            "1001", "", "Kumar",
            "rahul@gmail.com", "9876543210",
            "India", date(2026, 10, 10),
        ),
        (
            "1001", "Kiran", "Rao",
            "kiran@gmail.com", "9876543211",
            "India", date(2026, 10, 11),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_reasons = {
        "MISSING_FIRST_NAME",
        "DUPLICATE_CUSTOMER_ID",
    }

    # ACT
    valid_df, rejected_df = transform_customer_data(df)

    customer = rejected_df.filter(
        col("first_name") == ""
    ).collect()[0]

    actual_reasons = set(customer["rejection_reasons"])

    # ASSERT
    assert actual_reasons == expected_reasons
    assert valid_df.count() == 0


def test_customer_data_quality_metrics():
    # ARRANGE
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-data-quality-metrics")
        .getOrCreate()
    )

    input_data = [
        (
            "1001", "Rahul", "Kumar",
            "rahul@gmail.com", "9876543210",
            "India", date(2026, 10, 10),
        ),
        (
            "1002", "", "Rao",
            "kiran@gmail.com", "9876543211",
            "India", date(2026, 10, 11),
        ),
        (
            "1003", "Naveen", "Repakula",
            "invalidemail.com", "9876543212",
            "India", date(2026, 10, 12),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_metrics = {
    "source_records": 3,
    "valid_records": 1,
    "rejected_records": 2,
    "rejection_percentage": 66.67,
    "reconciliation_status": "PASSED",
}

    # ACT
    valid_df, rejected_df = transform_customer_data(df)

    actual_metrics = calculate_data_quality_metrics(
        df,
        valid_df,
        rejected_df,
    )

    # ASSERT
    assert actual_metrics == expected_metrics


def test_customer_output_schema():
    # ARRANGE
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-customer-output-schema")
        .getOrCreate()
    )

    input_data = [
        (
            "1001", "Rahul", "Kumar",
            "rahul@gmail.com", "9876543210",
            "India", date(2026, 10, 10),
        ),
        (
            "1002", "", "Rao",
            "invalidemail.com", "9876543211",
            "India", date(2026, 10, 11),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_valid_columns = set(CUSTOMER_SCHEMA.fieldNames())

    # ACT
    valid_df, rejected_df = transform_customer_data(df)

    actual_valid_columns = set(valid_df.columns)
    actual_rejected_columns = set(rejected_df.columns)

    # ASSERT
    assert actual_valid_columns == expected_valid_columns

    assert "rejection_reason" in actual_rejected_columns
    assert "rejection_reasons" in actual_rejected_columns

    assert valid_df.count() == 1
    assert rejected_df.count() == 1

def test_reconciliation_failure():
    # ARRANGE
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-reconciliation-failure")
        .getOrCreate()
    )

    input_data = [
        (
            "1001", "Rahul", "Kumar",
            "rahul@gmail.com", "9876543210",
            "India", date(2026, 10, 10),
        ),
        (
            "1002", "", "Rao",
            "kiran@gmail.com", "9876543211",
            "India", date(2026, 10, 11),
        ),
        (
            "1003", "Naveen", "Repakula",
            "invalidemail.com", "9876543212",
            "India", date(2026, 10, 12),
        ),
    ]

    df = spark.createDataFrame(
        input_data,
        schema=CUSTOMER_SCHEMA,
    )

    expected_metrics = {
        "source_records": 3,
        "valid_records": 1,
        "rejected_records": 1,
        "rejection_percentage": 33.33,
        "reconciliation_status": "FAILED",
    }

    # ACT
    valid_df, rejected_df = transform_customer_data(df)

    # Simulate losing one rejected record
    rejected_df = rejected_df.limit(1)

    actual_metrics = calculate_data_quality_metrics(
        df,
        valid_df,
        rejected_df,
    )

    # ASSERT
    assert actual_metrics == expected_metrics

def test_data_quality_gate_passes_at_configured_threshold():
    metrics = {
        "source_records": 7,
        "valid_records": 5,
        "rejected_records": 2,
        "rejection_percentage": 28.57,
        "reconciliation_status": "PASSED",
    }
    validate_data_quality(metrics, 30)


def test_data_quality_gate_fails_when_rejection_threshold_exceeded():
    import pytest

    metrics = {
        "source_records": 7,
        "valid_records": 5,
        "rejected_records": 2,
        "rejection_percentage": 28.57,
        "reconciliation_status": "PASSED",
    }
    with pytest.raises(ValueError, match="exceeds allowed"):
        validate_data_quality(metrics, 20)


def test_data_quality_gate_fails_when_reconciliation_fails():
    import pytest

    metrics = {
        "source_records": 7,
        "valid_records": 5,
        "rejected_records": 1,
        "rejection_percentage": 14.29,
        "reconciliation_status": "FAILED",
    }
    with pytest.raises(ValueError, match="do not reconcile"):
        validate_data_quality(metrics, 30)
