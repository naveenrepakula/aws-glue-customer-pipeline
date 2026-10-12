
from pyspark.sql import SparkSession

# Step 1: Start a local Spark session
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("COADP-Curated-Validation")
    .getOrCreate()
)

# Step 2: Read the Parquet output
df = spark.read.parquet("local_output/curated/")

# Step 3: Display the customer records
df.show(truncate=False)

# Step 4: Display the schema
df.printSchema()

# Step 5: Count the records
actual_count = df.count()

print(f"Actual valid customer count: {actual_count}")

# Step 6: Validate expected count
assert actual_count == 5, (
    f"Expected 5 valid customers, found {actual_count}"
)

print("SUCCESS: Curated customer count validated!")

spark.stop()
