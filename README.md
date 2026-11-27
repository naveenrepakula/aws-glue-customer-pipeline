# AWS Glue Customer Data Pipeline

A production-style Data Engineering portfolio project using AWS S3, Lambda,
Glue/PySpark, Glue Data Catalog, Athena and Apache Airflow.

## Architecture

```text
Source customer CSV
       |
       v
Amazon S3 - Raw
       | S3 ObjectCreated / EventBridge
       v
AWS Lambda ---- DynamoDB idempotency
       |
       v
AWS Glue + PySpark
       |                         |
       v                         v
Curated S3                 Rejected S3
Valid Parquet              Invalid records + reasons
       |
       v
Glue Data Catalog / Crawler
       |
       v
Amazon Athena

Airflow: daily schedule, input sensor, Lambda invoke, Glue monitoring,
         crawler refresh, retries and operational visibility.
GitHub Actions: tests and source validation on each feature push / PR.
```

**The architecture is intentionally unchanged.** S3 ObjectCreated events
invoke Lambda, Lambda starts the existing Glue job, Glue writes curated and
rejected Parquet, and the crawler updates the Catalog for Athena. Airflow
schedules and monitors the same pipeline; it does not replace Lambda.

## Technology stack

- Python 3.11, PySpark, pytest
- Amazon S3, EventBridge, Lambda, DynamoDB
- AWS Glue ETL, Glue Data Catalog and Crawler, Athena
- Apache Airflow 2.10, Amazon provider, Docker Compose, PostgreSQL
- CloudFormation and GitHub Actions

## Key engineering features

- Data cleaning, required-field and email validation
- Duplicate customer ID detection and rejection reasons
- Curated/rejected Parquet separation
- Record-count reconciliation and configurable rejection threshold
- Audit JSON and Glue job execution logging
- Lambda idempotency keyed by S3 bucket/key/ETag
- Event-driven S3 trigger and scheduled Airflow orchestration
- Glue completion monitoring, crawler refresh and Airflow retries
- Automated PySpark, Lambda and Airflow contract tests

## Run tests locally (no AWS)

Install Python 3.11, Java 17, then run:

```bash
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.6 pytest boto3 cfn-lint
python -m pytest tests/unit -v
python -m compileall -q airflow/dags lambda/trigger_glue
python -m json.tool infrastructure/parameters/dev.json > /dev/null
cfn-lint -t infrastructure/template.yaml
```

GitHub Actions runs these checks for feature branch pushes and pull requests.
Offline tests do not deploy or call AWS resources.

## Run local Airflow

1. Copy `.env.example` to `.env` and set the real DEV bucket, daily input key,
   AWS profile/region, Lambda name, Glue job and crawler names.
2. Configure AWS credentials on the host. Never commit secrets.
3. Run:

```bash
docker compose up airflow-init
docker compose up -d
```

4. Open http://localhost:8080 and configure the local AWS profile/connection.
5. Unpause `customer_s3_lambda_glue_pipeline`.

Read [docs/airflow-setup.md](docs/airflow-setup.md) for the complete setup,
IAM permissions, CloudFormation/Lambda packaging and operational runbook.

## Repository structure

- `glue/jobs/`: AWS Glue ETL entry point
- `glue/libraries/`: PySpark transformations and data-quality metrics
- `lambda/trigger_glue/`: S3/EventBridge and Airflow Lambda trigger
- `airflow/dags/`: scheduled orchestration and monitoring
- `infrastructure/`: CloudFormation S3, Lambda, EventBridge, DynamoDB,
  Glue Catalog/Crawler and Athena resources
- `tests/unit/`: transformation, Lambda and Airflow contract tests
- `sql/`: SQL validation and analytics queries
- `docs/`: architecture, deployment and operational instructions

## Scope and production readiness

The existing Glue ETL job is expected to be deployed separately. The AWS
infrastructure template requires a packaged Lambda ZIP and an existing Glue
job name. Real S3/EventBridge/Lambda/Glue/Crawler/Athena integration tests
require a DEV AWS account and valid IAM policies. The local tests do not
claim to validate live AWS services.

Before production use, finish hardening Glue output idempotency/overwrite
behavior, audit lifecycle, runtime-role least privilege, CloudWatch alarms,
Airflow authentication/secrets and environment-specific deployments.
