from pyspark.sql import SparkSession

from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    transform_customer_data,
)

spark = SparkSession.builder.appName("CustomerETL").getOrCreate()

customer_df = (
    spark.read.option("header", True)
    .schema(CUSTOMER_SCHEMA)
    .csv("sample_data/customers.csv")
)

valid_df, rejected_df = transform_customer_data(customer_df)

valid_df.write.mode("overwrite").parquet("output/curated/customers/")

rejected_df.write.mode("overwrite").parquet("output/rejected/customers/")
