import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from glue.libraries.transformations import (
    CUSTOMER_SCHEMA,
    transform_customer_data,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "curated_path",
        "rejected_path",
    ],
)

input_path = args["input_path"]
curated_path = args["curated_path"]
rejected_path = args["rejected_path"]

sc = SparkContext.getOrCreate()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(args["JOB_NAME"], args)

customer_df = (
    spark.read.option("header", True)
    .schema(CUSTOMER_SCHEMA)
    .csv(input_path)
)

valid_df, rejected_df = transform_customer_data(customer_df)

valid_df.write.mode("overwrite").parquet(curated_path)

rejected_df.write.mode("overwrite").parquet(rejected_path)

job.commit()
