from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", DateType(), True),
    ]
)


def clean_customer_data(df: DataFrame) -> DataFrame:
    """
    Clean customer string columns by removing leading
    and trailing whitespace.
    """

    string_columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "country",
    ]

    for column_name in string_columns:
        df = df.withColumn(column_name, trim(col(column_name)))

    return df


def add_rejection_reason(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rejection_reason",
        when(
            col("customer_id").isNull() | (col("customer_id") == ""),
            "MISSING_CUSTOMER_ID",
        )
        .when(
            col("first_name").isNull() | (col("first_name") == ""), "MISSING_FIRST_NAME"
        )
        .when(col("last_name").isNull() | (col("last_name") == ""), "MISSING_LAST_NAME")
        .when(col("email").isNull() | (col("email") == ""), "MISSING_EMAIL")
        .when(col("created_at").isNull(), "MISSING_CREATED_AT")
        .otherwise(None),
    )


def split_valid_rejected(df: DataFrame):
    valid_df = df.filter(col("rejection_reason").isNull()).drop("rejection_reason")

    rejected_df = df.filter(col("rejection_reason").isNotNull())

    return valid_df, rejected_df


def transform_customer_data(df: DataFrame):
    cleaned_df = clean_customer_data(df)

    validated_df = add_rejection_reason(cleaned_df)

    valid_df, rejected_df = split_valid_rejected(validated_df)

    return valid_df, rejected_df
