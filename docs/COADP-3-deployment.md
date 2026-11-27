# COADP-3 — AWS Glue Customer ETL Deployment (DEV)

## 1. Overview

**Jira Ticket:** COADP-3
**Environment:** DEV
**AWS Region:** ap-south-1 (Mumbai)
**Glue Job:** COADP-Customer-ETL-DEV
**Glue Version:** 5.0
**Worker Type:** G.1X
**Repository deployment JSON:** 2 workers, 10-minute timeout

**Objective:** Deploy the customer ETL pipeline to AWS Glue, process raw customer CSV data from Amazon S3, and write valid and rejected records to separate Parquet datasets.

## 2. Deployment Artifacts

**Previously documented ETL script:**
`s3://customer-pipeline-dev-439701543859-glue-artifacts/glue/scripts/customer_etl.py`

**Previously documented reusable Python library:**
`s3://customer-pipeline-dev-439701543859-glue-artifacts/glue/libraries/glue_libs.zip`

**IAM Execution Role:**
`COADP-Glue-ExecutionRole-DEV`

**Currently observed Glue job release (verified 2026-11-27):**
- Script: `s3://customer-pipeline-dev-439701543859-glue-artifacts/releases/671de3095fe9e93960a2404736ab2fbdd5fb0bd7/customer_etl.py`
- Extra Python files: `s3://customer-pipeline-dev-439701543859-glue-artifacts/releases/671de3095fe9e93960a2404736ab2fbdd5fb0bd7/glue_libs.zip`

## 3. Runtime S3 Paths

**Currently configured Glue job default input:**
`s3://customer-pipeline-dev-439701543859-data/raw/customers/customers.csv`

**Currently configured Glue job default curated output:**
`s3://customer-pipeline-dev-439701543859-data/curated/customers/dev-test/`

**Currently configured Glue job default rejected output:**
`s3://customer-pipeline-dev-439701543859-data/rejected/customers/dev-test/`

The Lambda trigger supplies per-run input, curated, rejected, rejection-threshold, and audit-bucket arguments when it starts the Glue job. The actual run arguments therefore need to be checked on each Glue run rather than inferred only from job defaults.

## 4. Execution Configuration

The repository deployment JSON records:
- Job type: Spark ETL
- Language: Python 3
- Glue version: 5.0
- Worker type: G.1X
- Number of workers: 2
- Timeout: 10 minutes
- Automatic retries: 0
- Output format: Parquet
- Write mode: overwrite

**Live AWS configuration drift observed on 2026-11-27:** the deployed job currently has 10 G.1X workers, a 480-minute timeout, and a 10% default rejection threshold. Its script and library locations also use the release-specific prefix shown above. These values differ from the repository deployment JSON and the historical deployment settings. Do not update or run the live Glue job until the intended worker count, timeout, rejection threshold, and artifact release are explicitly agreed.

## 5. Execution Evidence

**Historical initial JobRunId:**
`jr_6359e48b61e20723b4f1fe560711e8733611777a8c1e86fe8c26dae7a2ee3230`

**Historical rerun JobRunId:**
`jr_b5a28667dc86063089107eab22e65e3f6b9fe837a38161d2f66bb3d95cc258d7`

The original deployment record reports both executions completed with status `SUCCEEDED`. This is historical evidence and was not re-executed as part of the 2026-11-27 read-only verification.

## 6. Data Validation Results

| Validation | Expected | Actual | Status |
|---|---:|---:|---|
| Valid customers | 5 | 5 | Passed |
| Rejected customers | 2 | 2 | Passed |
| Total accounted for | 7 | 7 | Passed |
| Missing email | MISSING_EMAIL | MISSING_EMAIL | Passed |
| Missing customer ID | MISSING_CUSTOMER_ID | MISSING_CUSTOMER_ID | Passed |
| Missing optional phone | Accepted | Accepted | Passed |

The original deployment record states that Parquet schemas and output records were inspected using local PySpark validation scripts. This evidence was not re-run during the 2026-11-27 verification.

## 7. CloudWatch Verification

Previously documented log groups:
- `/aws-glue/jobs/output`
- `/aws-glue/jobs/error`

The original deployment record reports accessible log streams and successful SparkContext termination. This was not re-verified during the 2026-11-27 read-only review.

## 8. Rerun and Overwrite Behavior

The pipeline writes curated and rejected data using `mode("overwrite")`.

The original deployment record describes a controlled rerun against isolated DEV test prefixes. It reports that the rerun succeeded, the curated output contained 5 valid customers, and the rejected output contained 2 invalid customers.

**Risk:** Overwrite mode can remove existing data under the configured output path. Shared or production paths must not be used without appropriate safeguards.

## 9. Infrastructure and Integration Verification (2026-11-27)

The current CloudFormation template is being revised to reference the existing DEV data and artifacts buckets as parameters rather than attempting to create buckets with names that already exist.

Existing buckets:
- `customer-pipeline-dev-439701543859-data`
- `customer-pipeline-dev-439701543859-glue-artifacts`

The revised template does not manage or change the configuration of these buckets. The S3 EventBridge notification setting must be enabled separately and safely on the existing data bucket before the EventBridge rule can receive S3 object-created events. Preserve any existing bucket notification configuration when doing so.

Read-only AWS discovery on 2026-11-27 found:
- Existing Glue job: `COADP-Customer-ETL-DEV`
- Expected Lambda `customer-pipeline-dev-trigger-glue`: not present
- DynamoDB tables: none listed
- Glue crawlers: none listed
- Existing data bucket notification configuration: empty when inspected

The currently deployed input object is named `raw/customers/customers.csv`, while the Airflow example uses a dated key pattern, `raw/customers/customers_{{ ds_nodash }}.csv`. Align the input naming convention before enabling scheduled runs. The Lambda currently falls back to the current UTC date when the input filename does not contain an eight-digit date.

No CloudFormation stack deployment, bucket notification change, Lambda invocation, Glue job run, or other AWS write operation was performed during this verification.

## 10. Deployment Status

Historical Glue execution and data-validation evidence is retained above. Current infrastructure integration is **not yet deployed or end-to-end verified**. Remaining work includes:
1. Validate the revised CloudFormation template and parameter values.
2. Resolve the Glue configuration drift and input-key naming convention.
3. Review and test the Lambda event/idempotency contract.
4. Safely enable EventBridge on the existing data bucket.
5. Deploy the required supporting resources only after explicit approval.
6. Run an approved end-to-end test and attach fresh evidence to the project.
