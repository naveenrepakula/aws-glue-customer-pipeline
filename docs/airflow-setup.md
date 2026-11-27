# Local Apache Airflow orchestration

This setup runs Apache Airflow locally with Docker Compose. Airflow orchestrates
the existing AWS Glue job; it does not reimplement the PySpark transformations.
No AWS resources are created by Docker Compose.

## Architecture

```text
S3 raw/customers/customers.csv
          |
          v
Airflow S3KeySensor
          |
          v
Airflow GlueJobOperator
          |
          v
AWS Glue: COADP-Customer-ETL-DEV
          |
          +--------------------------+
          |                          |
          v                          v
S3 curated/customers/          S3 rejected/customers/
          |
          v
Glue job audit record
```

The DAG runs daily at 01:00 Asia/Kolkata. By default it waits for the date-specific
source object `raw/customers/customers_{{ ds_nodash }}.csv`, starts the Glue job,
waits for completion, and retries failed tasks twice with a five-minute delay.
Each logical run writes to a `run_date={{ ds }}` output prefix. The S3 key can
be overridden in `.env`; retain a date template if each schedule should process
a distinct daily file.

## Prerequisites

- Docker Desktop with Docker Compose
- An AWS identity that can read the source S3 object and start, inspect, and
  read the status of the configured Glue job
- The Glue job already deployed in the target AWS account and region
- Access to the AWS credentials/profile used by the Docker container

## 1. Configure environment

Copy `.env.example` to `.env` and set:
- `CUSTOMER_DATA_BUCKET`: your actual data bucket name (do not include s3://)
- `CUSTOMER_INPUT_KEY`: exact incoming object key
- `CUSTOMER_GLUE_JOB_NAME`: existing Glue job name
- `AWS_PROFILE` and `AWS_DEFAULT_REGION`

The Compose file mounts the host `~/.aws` directory read-only. Configure
credentials on the host first (for example, using AWS IAM Identity Center /
AWS SSO), and ensure the selected profile is available inside the container.
Do not commit `.env`, access keys, secret keys, or AWS credential files.

## 2. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Open http://localhost:8080 and sign in with the local development account:
- Username: `admin`
- Password: `admin`

Change this development password if you expose the service beyond localhost.
Do not use these credentials in a shared or production environment.

## 3. Airflow AWS connection

Docker Compose provisions the `aws_glue_conn` connection automatically using
the `AIRFLOW_CONN_AWS_GLUE_CONN` environment variable. It contains only the
AWS region; it deliberately contains no static access keys or secret keys.
The AWS provider uses the mounted host profile / default boto3 credential
chain. The DAG reads the connection ID from `AIRFLOW_AWS_CONN_ID`.

To use a different connection ID, update `AIRFLOW_AWS_CONN_ID` and the matching
`AIRFLOW_CONN_<CONNECTION_ID>` environment variable in `docker-compose.yaml`,
then recreate the Airflow containers.

The AWS identity must have, at minimum, permission to:
- `s3:GetObject` and `s3:ListBucket` for the configured raw input
- `glue:StartJobRun`, `glue:GetJobRun`, and `glue:GetJobRuns` for the job
- Any additional permissions the Glue job's own execution role needs for
  its data, artifacts, and audit destinations

Airflow's AWS identity and the Glue job execution role are separate identities.
Do not give the Airflow identity broad S3 write/delete access just to start Glue.

## 4. Enable and run the DAG

In the Airflow UI:
1. Find `customer_s3_to_glue_pipeline`.
2. Unpause it.
3. Use **Trigger DAG** to start a manual run.

The S3 sensor waits until the configured object exists. Once detected, the
Glue task starts the named job and waits for the job run to finish. Review
task logs in Airflow and the corresponding Glue / CloudWatch logs for details.

## 5. Local validation without AWS

You can validate DAG syntax without connecting to AWS after installing the
same Airflow and Amazon provider versions:

```bash
python -m compileall airflow/dags
```

A successful compile only verifies Python syntax. To test real task execution,
an AWS account, valid IAM permissions, the source object, and the deployed
Glue job are required.

## Operational notes

- The DAG is a local orchestration example, not a production Airflow deployment.
  For production, use managed Airflow or a secured Airflow deployment with
  managed metadata storage, secrets backend, networking, monitoring, and RBAC.
- The sensor watches one configured object key. If source files are delivered
  with unique names, change the input-key strategy (for example, a date-based
  key or manifest) before enabling the schedule.
- The Glue job must accept the arguments supplied by this DAG. The repository's
  DEV job definition includes the rejection threshold argument.
- Re-running the same logical date writes to the same date-specific prefixes.
  Confirm the Glue write mode and rerun policy before using production data.
