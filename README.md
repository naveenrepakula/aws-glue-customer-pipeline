# AWS Glue Customer Data Pipeline with Apache Airflow

An end-to-end Data Engineering portfolio project demonstrating orchestration,
PySpark transformations, data-quality controls, S3 data zones, audit logging,
and CI testing.

## Architecture

```text
Incoming customer CSV
        |
        v
Amazon S3 (raw/customers/)
        |
        v
Apache Airflow (daily schedule + S3 sensor)
        |
        v
AWS Glue Job (PySpark)
        |
        +----------------------+----------------------+
        |                                             |
        v                                             v
S3 curated/customers/                         S3 rejected/customers/
        |
        v
Data quality metrics + audit record
```

Airflow coordinates the workflow and monitors the Glue job. The transformations
remain in the Glue PySpark application so the business logic is not duplicated.

## Technology stack

- Python 3.11, PySpark, pytest
- AWS Glue 5.0, Amazon S3, IAM, CloudWatch
- Apache Airflow 2.10 with Amazon provider
- Docker Compose and PostgreSQL for local Airflow
- GitHub Actions for automated tests and deployment workflow

## Pipeline capabilities

- Trim and normalize customer string fields
- Validate required fields and email format
- Identify duplicate customer IDs
- Route valid and rejected records to separate Parquet outputs
- Calculate source/valid/rejected counts and reconciliation status
- Enforce a configurable rejection threshold
- Write execution audit records
- Orchestrate S3 arrival and Glue execution with Airflow retries

## Run Airflow locally

1. Install Docker Desktop and Docker Compose.
2. Copy `.env.example` to `.env` and set your AWS bucket, profile, region,
   input key, and existing Glue job name.
3. Configure AWS credentials on the host. Do not commit credentials.
4. Run:

```bash
docker compose up airflow-init
docker compose up -d
```

5. Open http://localhost:8080 and sign in with the local development
   credentials documented in [Airflow setup](docs/airflow-setup.md).
6. Configure the `aws_glue_conn` connection in Airflow and trigger
   `customer_s3_to_glue_pipeline`.

Detailed steps, IAM requirements, and limitations are in
[docs/airflow-setup.md](docs/airflow-setup.md).

## Test locally

With Python, Java 17, and the project test dependencies installed:

```bash
python -m pip install -r requirements.txt
python -m pytest tests/unit -v
python -m compileall -q airflow/dags
```

Airflow DAG source contract tests do not require an AWS account. Running the
DAG against real S3 and Glue does require a configured AWS identity, IAM
permissions, an existing Glue job, and an input object.

## Repository layout

- `glue/jobs/`: AWS Glue ETL entry point
- `glue/libraries/`: reusable PySpark transformations
- `airflow/dags/`: orchestration DAG
- `tests/unit/`: transformation and DAG contract tests
- `infrastructure/`: CloudFormation infrastructure
- `sql/`: SQL validation and analytics queries
- `docs/`: architecture, deployment, and Airflow runbooks

## Important scope

The Docker Compose stack is a local development Airflow environment. It does
not provision AWS resources or make the pipeline production-ready by itself.
The Glue job must already exist in AWS. Review IAM, output idempotency, audit
bucket configuration, monitoring, and production secrets management before
using real production data.
