
from pyspark.sql import SparkSession

# Create local Spark session
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("COADP-Rejected-Validation")
    .getOrCreate()
)

# Read rejected records from downloaded Parquet
rejected_df = spark.read.parquet("local_output/rejected/")

# Display rejected customers and their reasons
rejected_df.show(truncate=False)

# Inspect the schema
rejected_df.printSchema()

# Count rejected records
rejected_count = rejected_df.count()
print(f"Actual rejected customer count: {rejected_count}")

# Check expected count
assert rejected_count == 2, (
    f"Expected 2 rejected customers, found {rejected_count}"
)

print("SUCCESS: Rejected customer count validated!")

spark.stop()
