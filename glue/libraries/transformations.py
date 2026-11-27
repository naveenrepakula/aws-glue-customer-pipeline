from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    col,
    concat,
    count,
    filter,
    lit,
    trim,
    when,
)
from pyspark.sql.window import Window
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
        .when(
            col("email").isNull() | (col("email") == ""),"MISSING_EMAIL",)
        .when(
             ~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),"INVALID_EMAIL_FORMAT",)
        .when(
              col("created_at").isNull(),"MISSING_CREATED_AT",)

    )


def add_multiple_rejection_reasons(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rejection_reasons",
        filter(
            array(
                when(
                    col("customer_id").isNull()
                    | (col("customer_id") == ""),
                    lit("MISSING_CUSTOMER_ID"),
                ),
                when(
                    col("first_name").isNull()
                    | (col("first_name") == ""),
                    lit("MISSING_FIRST_NAME"),
                ),
                when(
                    col("last_name").isNull()
                    | (col("last_name") == ""),
                    lit("MISSING_LAST_NAME"),
                ),
                when(
                    col("email").isNull()
                    | (col("email") == ""),
                    lit("MISSING_EMAIL"),
                ),
                when(
                    col("email").isNotNull()
                    & (col("email") != "")
                    & ~col("email").rlike(
                        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                    ),
                    lit("INVALID_EMAIL_FORMAT"),
                ),
                when(
                    col("created_at").isNull(),
                    lit("MISSING_CREATED_AT"),
                ),
            ),
             lambda reason: reason.isNotNull(),
        ),
    )


def reject_duplicate_customer_ids(df: DataFrame) -> DataFrame:

    customer_window = Window.partitionBy("customer_id")

    df = df.withColumn(
        "customer_id_count",
        count("*").over(customer_window)
    )

    duplicate_condition = (
        (col("customer_id_count") > 1)
        & col("customer_id").isNotNull()
        & (col("customer_id") != "")
    )

    df = df.withColumn(
        "rejection_reasons",
        when(
            duplicate_condition,
            concat(
                col("rejection_reasons"),
                array(lit("DUPLICATE_CUSTOMER_ID"))
            )
        ).otherwise(col("rejection_reasons"))
    )

    df = df.withColumn(
        "rejection_reason",
        when(
            duplicate_condition
            & col("rejection_reason").isNull(),
            "DUPLICATE_CUSTOMER_ID"
        ).otherwise(col("rejection_reason"))
    )

    return df.drop("customer_id_count")


def split_valid_rejected(df: DataFrame):
    valid_df = (
        df.filter(col("rejection_reason").isNull())
        .drop("rejection_reason", "rejection_reasons")
    )

    rejected_df = df.filter(
        col("rejection_reason").isNotNull()
    )

    return valid_df, rejected_df

def transform_customer_data(df: DataFrame):
    cleaned_df = clean_customer_data(df)

    validated_df = add_rejection_reason(cleaned_df)

    validated_df = add_multiple_rejection_reasons(validated_df)

    validated_df = reject_duplicate_customer_ids(validated_df)

    valid_df, rejected_df = split_valid_rejected(validated_df)

    return valid_df, rejected_df

def calculate_data_quality_metrics(
    source_df: DataFrame,
    valid_df: DataFrame,
    rejected_df: DataFrame,
) -> dict:

    source_count = source_df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    processed_count = valid_count + rejected_count

    reconciliation_status = (
        "PASSED"
        if source_count == processed_count
        else "FAILED"
    )

    rejection_percentage = (
        round((rejected_count / source_count) * 100, 2)
        if source_count > 0
        else 0.0
    )

    return {
        "source_records": source_count,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "rejection_percentage": rejection_percentage,
        "reconciliation_status": reconciliation_status,
    }
def validate_data_quality(metrics: dict, max_rejection_percentage: float) -> None:
    """Fail the ETL run when reconciliation or the rejection threshold fails."""
    if not 0 <= max_rejection_percentage <= 100:
        raise ValueError("max_rejection_percentage must be between 0 and 100")

    if metrics["reconciliation_status"] != "PASSED":
        raise ValueError(
            "ETL failed: source and processed record counts do not reconcile"
        )

    source_records = metrics["source_records"]
    rejected_records = metrics["rejected_records"]
    if (
        source_records > 0
        and rejected_records * 100 > max_rejection_percentage * source_records
    ):
        raise ValueError(
            f"ETL failed: rejection percentage "
            f"{metrics['rejection_percentage']}% exceeds allowed "
            f"{max_rejection_percentage}%"
        )
