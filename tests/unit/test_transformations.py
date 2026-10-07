from datetime import date

from pyspark.sql import SparkSession

from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    add_rejection_reason,
    clean_customer_data,
    split_valid_rejected,
    transform_customer_data,
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
