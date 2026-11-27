# AWS Glue Customer Data Pipeline

A production-style Data Engineering portfolio project demonstrating an event-driven AWS data pipeline with scheduled orchestration, data quality controls, audit logging, and automated testing.

## Architecture

The data-processing flow is intentionally preserved:

```text
Source customer CSV
       |
       v
Amazon S3 - Raw
       | S3 ObjectCreated event (EventBridge)
       v
AWS Lambda ----------------------+
       |                         |
       | starts existing Glue    | DynamoDB idempotency
       | job                     | (bucket + key + ETag)
       v                         |
AWS Glue + PySpark <-------------+
       |
       +---------------------+
       |                     |
       v                     v
Curated S3               Rejected S3
Valid Parquet            Invalid records + reasons
       |
       v
Glue Data Catalog / Crawler
       |
       v
Amazon Athena

Apache Airflow: daily schedule -> raw-file sensor -> invoke the same
                 Lambda -> monitor Glue JobRunId -> refresh crawler.
GitHub Actions: run automated checks on feature pushes and pull requests.
```

**Important architecture boundary:** S3 ObjectCreated events still trigger Lambda directly. Lambda starts the existing Glue + PySpark job. Airflow is an additional scheduled control/monitoring path; it does not replace the S3-to-Lambda trigger or duplicate transformation logic. Both paths use the same Lambda and DynamoDB idempotency key to avoid duplicate Glue runs for the same object version.

## Technology stack

- **Languages and processing:** Python 3.11, PySpark, SQL, pytest
- **AWS:** Amazon S3, EventBridge, Lambda, DynamoDB, AWS Glue, Glue Data Catalog, Glue Crawler, Athena, CloudWatch
- **Orchestration:** Apache Airflow 2.10.5, Amazon provider, Docker Compose, PostgreSQL
- **Infrastructure and CI:** AWS CloudFormation, GitHub Actions

## Engineering features

- CSV ingestion from the S3 raw zone.
- PySpark cleansing, required-field checks, email validation, duplicate-ID detection, and rejection reasons.
- Separate curated and rejected Parquet outputs.
- Source/valid/rejected record reconciliation and configurable rejection-percentage gate.
- Success/failure audit JSON with execution metrics.
- Lambda S3/EventBridge trigger with DynamoDB idempotency by bucket, object key, and ETag.
- Airflow daily schedule, S3 input sensor, Lambda invocation, Glue run monitoring, crawler refresh, and bounded retries.
- Automated transformation, Lambda, Airflow DAG contract, and local pipeline integration tests.

## Repository layout

| Path | Purpose |
|---|---|
| `glue/jobs/` | AWS Glue ETL entry point |
| `glue/libraries/` | PySpark transformations, data-quality checks, and audit writer |
| `lambda/trigger_glue/` | S3/EventBridge and Airflow Lambda entry point |
| `airflow/dags/` | Daily scheduled orchestration and Glue monitoring |
| `infrastructure/` | CloudFormation resources and DEV parameters |
| `tests/unit/` | Transformation, Lambda, and Airflow contract tests |
| `tests/integration/` | Local CSV-to-Parquet and audit integration test |
| `sql/` | Athena analytics and validation SQL |
| `docs/` | Architecture, deployment, and Airflow operations |

## Run automated tests locally (no AWS calls)

### Prerequisites

- Python 3.11
- Java 17 (required by local PySpark)
- Docker Desktop / Docker Compose (for Compose validation and local Airflow)

Install test dependencies and run the same validation steps used by CI:

```bash
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.6 pytest boto3 cfn-lint

python -m pytest tests/unit tests/integration -v
python -m compileall -q airflow/dags lambda/trigger_glue
python -m json.tool infrastructure/parameters/dev.json > /dev/null
cfn-lint -t infrastructure/template.yaml

cp .env.example .env
# Set CUSTOMER_DATA_BUCKET to your actual DEV bucket before starting Airflow.
docker compose config --quiet
```

The GitHub Actions workflow runs the unit and integration tests, Python compilation, DEV parameter JSON validation, CloudFormation lint, and Docker Compose configuration validation on feature-branch pushes and pull requests to `main`. It is validation-only: it does not upload artifacts, update AWS resources, invoke Lambda, or start Glue. Any deployment requires a separate explicit approval and reviewed execution plan.

## Run Airflow locally

1. Install Docker Desktop and configure an AWS CLI profile (or IAM Identity Center session) on your host.
2. Copy `.env.example` to `.env`. Set the actual DEV bucket, input key, AWS profile/region, Lambda function, Glue job, and crawler names. Never commit `.env` or AWS credentials.
3. The Compose stack mounts the host AWS configuration read-only and creates the Airflow AWS connection `aws_glue_conn` using the configured region. The DAG uses this connection through Airflow's AWS hook; credentials are supplied by the host AWS profile/IAM identity, not hardcoded in the DAG.
4. Start the services:

   ```bash
   docker compose up airflow-init
   docker compose up -d
   docker compose ps
   ```

5. Open [http://localhost:8080](http://localhost:8080). The local development login is `admin` / `admin`; do not expose this development stack publicly.
6. Unpause `customer_s3_lambda_glue_pipeline`. It runs daily at **01:00 Asia/Kolkata**. Ensure the matching date-keyed CSV exists under `raw/customers/`.

See [docs/airflow-setup.md](docs/airflow-setup.md) for IAM permissions, CloudFormation/Lambda packaging, manual triggering, retries, and troubleshooting.

## What the Airflow DAG does

1. Waits for the configured date-specific customer CSV in S3.
2. Invokes the same Lambda used by the S3 ObjectCreated event path.
3. Captures and monitors the returned Glue JobRunId.
4. If Glue reaches a terminal failure state, asks Lambda to retry the same object version, up to two bounded restarts.
5. Refreshes the Glue crawler only after the Glue run succeeds.
6. Retries failed Airflow tasks twice, with a five-minute delay.

The S3 event and scheduled Airflow paths share DynamoDB idempotency keyed by bucket, key, and ETag. This is intended to prevent two independent triggers from launching duplicate Glue runs for the same S3 object version.

## Validation scope and deployment notes

**What has been validated in CI:** offline PySpark transformation tests, Lambda handler tests with mocked AWS clients, Airflow DAG contract tests, a local CSV-to-Parquet/audit integration test, Python compilation, CloudFormation lint, DEV JSON validation, and Docker Compose configuration validation.

**What has not been validated by those offline checks:** deployment to an AWS account, real IAM permissions, EventBridge delivery, live Lambda invocation, AWS Glue execution, S3 output permissions, crawler behavior, or Athena queries. Those require a controlled DEV AWS environment and must not be inferred from passing local/CI tests.

The CloudFormation template expects the existing Glue ETL job and a packaged Lambda ZIP in the configured artifacts bucket. Review the DEV parameter values and existing stack ownership before deploying. Do not deploy to production while the project is still being validated.

## Known production-hardening work

- Confirm safe rerun/overwrite semantics for curated and rejected outputs, including partial-write recovery.
- Add/verify CloudWatch alarms and operational notifications.
- Review least-privilege policies separately for Airflow, Lambda, Glue job execution, and crawler roles.
- Configure production Airflow authentication, RBAC, managed secrets, backups, and monitoring.
- Run end-to-end integration tests in DEV before describing the AWS deployment as production-verified.
- Enable S3 EventBridge delivery on the existing data bucket while preserving any existing bucket notification destinations.
- Align the Glue execution role's artifact read scope with the `releases/<revision>/` paths used by release packaging.
- Define recovery for an idempotency record stuck in `STARTING` and avoid treating this mechanism as exactly-once processing.
- Ensure distinct source objects cannot overwrite one another when they map to the same `run_date` output prefix.
- Schedule a crawler refresh for successful EventBridge-triggered runs if no Airflow run follows them.
- Grant Athena callers Catalog/query permissions and access to the workgroup results prefix; the workgroup alone does not grant access.

## Portfolio summary

This project demonstrates the separation of responsibilities expected in a data platform: S3 and Lambda handle event-driven ingestion, Glue/PySpark performs transformation and validation, S3 stores curated/rejected datasets, Glue Catalog and Athena expose the data, Airflow provides scheduled orchestration and monitoring, and GitHub Actions validates code changes before review.
