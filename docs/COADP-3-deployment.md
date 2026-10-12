# COADP-3 — AWS Glue Customer ETL Deployment (DEV)

## 1. Overview

**Jira Ticket:** COADP-3
**Environment:** DEV
**AWS Region:** ap-south-1 (Mumbai)
**Glue Job:** COADP-Customer-ETL-DEV
**Glue Version:** 5.0
**Worker Type:** G.1X
**Number of Workers:** 2

**Objective:** Deploy the customer ETL pipeline to AWS Glue, process raw customer CSV data from Amazon S3, and write valid and rejected records to separate Parquet datasets.

## 2. Deployment Artifacts

**ETL Script:**
`s3://customer-pipeline-dev-439701543859-glue-artifacts/glue/scripts/customer_etl.py`

**Reusable Python Library:**
`s3://customer-pipeline-dev-439701543859-glue-artifacts/glue/libraries/glue_libs.zip`

**IAM Execution Role:**
`COADP-Glue-ExecutionRole-DEV`

## 3. Runtime S3 Paths

**Input:**
`s3://customer-pipeline-dev-439701543859-data/raw/customers/customers.csv`

**Curated Output:**
`s3://customer-pipeline-dev-439701543859-data/curated/customers/dev-test/`

**Rejected Output:**
`s3://customer-pipeline-dev-439701543859-data/rejected/customers/dev-test/`

All paths are configured through AWS Glue job parameters.

## 4. Execution Configuration

- Job type: Spark ETL
- Language: Python 3
- Timeout: 10 minutes
- Maximum concurrent runs: 1
- Automatic retries: 0
- Output format: Parquet
- Write mode: overwrite

## 5. Execution Evidence

**Initial JobRunId:**
`jr_6359e48b61e20723b4f1fe560711e8733611777a8c1e86fe8c26dae7a2ee3230`

**Rerun JobRunId:**
`jr_b5a28667dc86063089107eab22e65e3f6b9fe837a38161d2f66bb3d95cc258d7`

Both executions completed with status `SUCCEEDED`.

## 6. Data Validation Results

| Validation | Expected | Actual | Status |
|---|---:|---:|---|
| Valid customers | 5 | 5 | Passed |
| Rejected customers | 2 | 2 | Passed |
| Total accounted for | 7 | 7 | Passed |
| Missing email | MISSING_EMAIL | MISSING_EMAIL | Passed |
| Missing customer ID | MISSING_CUSTOMER_ID | MISSING_CUSTOMER_ID | Passed |
| Missing optional phone | Accepted | Accepted | Passed |

The Parquet schemas and output records were inspected using local PySpark validation scripts.

## 7. CloudWatch Verification

Verified log groups:

- `/aws-glue/jobs/output`
- `/aws-glue/jobs/error`

Confirmed that log streams were accessible for the initial execution. Spark shutdown logs reported exit code 0 and successful SparkContext termination.

## 8. Rerun and Overwrite Behavior

The pipeline writes curated and rejected data using `mode("overwrite")`.

A controlled rerun was performed against isolated DEV test prefixes.

Results:

- Glue rerun succeeded.
- Previous Parquet output objects were replaced.
- Curated output still contained 5 valid customers.
- Rejected output still contained 2 invalid customers.
- No additional duplicate records were observed.

**Risk:** Overwrite mode can remove existing data under the configured output path. Shared or production paths must not be used without appropriate safeguards.

## 9. Deployment Status

AWS Glue deployment, initial execution, CloudWatch log inspection, output validation, and safe rerun testing completed.

Remaining delivery activities: Git review, Pull Request, code review, merge into `main`, and attaching deployment evidence to Jira.
