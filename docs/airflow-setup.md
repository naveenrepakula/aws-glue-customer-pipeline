# Airflow scheduling and AWS pipeline operations

This repository keeps the requested architecture unchanged:

```text
Source customer CSV
       |
       v
Amazon S3 - Raw
       |
       | Object Created (EventBridge)
       v
AWS Lambda
       |  starts Glue once per S3 object version
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

Apache Airflow - daily schedule, Lambda invocation, Glue monitoring,
                  crawler refresh, retries and operational visibility
GitHub Actions - unit/contract tests and source validation on every push/PR
```

**Trigger behavior:** S3 ObjectCreated events invoke Lambda directly through
EventBridge. Airflow is a separate scheduled control plane: it checks the
scheduled source key, invokes the same Lambda, waits for the returned Glue
JobRunId to finish, and refreshes the Catalog crawler. DynamoDB idempotency
uses bucket + key + ETag so the S3 event and scheduled Airflow run do not
normally start two Glue runs for the same object version.

## Components added

- `lambda/trigger_glue/handler.py`: accepts S3 EventBridge events and
  Airflow invocations, filters the raw customer CSV prefix, records an
  idempotency key, and starts the existing Glue job.
- `infrastructure/template.yaml`: provisions EventBridge routing, Lambda
  execution role/function, DynamoDB idempotency table, Glue database/crawler,
  Athena workgroup and S3 buckets.
- `airflow/dags/customer_pipeline_dag.py`: scheduled sensor -> Lambda ->
  Glue completion sensor -> Catalog crawler workflow.
- `tests/unit/test_lambda_trigger.py`: offline Lambda event, Glue arguments,
  duplicate-event and ignore-prefix tests.
- `tests/unit/test_airflow_dag_contract.py`: verifies the intended DAG flow
  and schedule without importing Airflow or calling AWS.

## Local Airflow prerequisites

- Docker Desktop and Docker Compose
- Python 3.11 for local test execution
- AWS CLI configured with a profile or IAM Identity Center session
- AWS infrastructure deployed in the selected account/region
- Existing Glue ETL job and its execution role configured for raw reads,
  curated/rejected writes, and audit writes

## 1. Configure local settings

Copy `.env.example` to `.env`. Set your actual bucket, input key, Lambda name,
Glue job, crawler, AWS profile and region. The default key is date-based:

`raw/customers/customers_{{ ds_nodash }}.csv`

Place the matching daily file in the raw S3 prefix. Do not commit `.env`,
AWS access keys, secret keys, or credential files.

The Compose stack mounts the host AWS config read-only. Configure the selected
profile on the host first. The Airflow AWS connection is supplied through
`AIRFLOW_CONN_AWS_GLUE_CONN` and the DAG uses that connection via
`AwsBaseHook`; no static AWS keys are embedded in the DAG.

## 2. Start local Airflow

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Open http://localhost:8080. The local-only development login is:
- Username: `admin`
- Password: `admin`

Do not expose this development login publicly. Use a proper authentication
backend and secrets manager for shared/production Airflow.

## 3. Configure AWS permissions

The Airflow identity needs scoped permissions:
- `s3:ListBucket` on the data bucket and `s3:GetObject` on the raw customer
  input prefix (for the sensor and ETag lookup)
- `lambda:InvokeFunction` on the customer trigger Lambda
- `glue:GetJobRun` on the customer Glue job
- `glue:StartCrawler` and `glue:GetCrawler` on the customer crawler

The Lambda execution role is separate. The CloudFormation template grants it:
- `glue:StartJobRun` on the configured Glue job
- DynamoDB GetItem/PutItem/UpdateItem on the idempotency table
- CloudWatch Logs write permissions

The Glue job execution role must independently be able to read raw S3,
write curated/rejected Parquet and write audit JSON to the configured data
bucket. The crawler role needs read access to curated/rejected prefixes.
Athena users need Catalog read/query permissions and access to the workgroup
results location. Keep these identities separate; do not grant broad S3
delete permissions to Airflow or Lambda.

## 4. Deploy AWS infrastructure

The CloudFormation template now includes the Lambda, EventBridge rule,
DynamoDB table, Glue database/crawler and Athena workgroup. The Lambda ZIP
must be uploaded to the Glue artifacts bucket before the stack creates or
updates the Lambda function.

Build the Lambda package from the repository root:

```bash
mkdir -p dist
python - <<'PY'
from zipfile import ZipFile, ZIP_DEFLATED
with ZipFile("dist/trigger_glue.zip", "w", ZIP_DEFLATED) as archive:
    archive.write("lambda/trigger_glue/handler.py", "handler.py")
PY
```

For an existing deployment, upload the package to the artifacts bucket and
update the CloudFormation stack with the matching `LambdaArtifactKey`
parameter. On a brand-new account, bootstrap the artifacts bucket first,
upload the ZIP, then deploy the full stack. Use the DEV parameter file and
the correct account/region for your environment. Do not deploy to PROD while
the project is still being validated.

The Glue job name parameter must match an already deployed Glue job. The
template does not create or replace the existing ETL job.

## 5. Run the scheduled DAG

The DAG `customer_s3_lambda_glue_pipeline` runs daily at 01:00
Asia/Kolkata. It:
1. Waits up to one hour for the date-specific raw CSV.
2. Invokes the Lambda with bucket, key and object ETag.
3. Waits for that exact Glue JobRunId and fails if Glue reports a terminal
   failure.
4. Starts the Glue crawler after Glue succeeds.
5. Retries failed Airflow tasks twice, with a five-minute delay.

Unpause the DAG in the Airflow UI. For a manual test, ensure the matching
date-keyed input file exists, then select **Trigger DAG**. S3 event delivery
may start Lambda before Airflow; the shared DynamoDB idempotency key lets
Airflow reuse the existing Glue run rather than starting a second one.

## 6. Run offline tests on every code change

GitHub Actions runs on pushes to feature branches and on pull requests to
main. It runs the PySpark transformation tests, Lambda trigger tests, Airflow
DAG contract tests and Python compilation. These tests do not require AWS.

Run the same tests locally:

```bash
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.6 pytest boto3
python -m pytest tests/unit -v
python -m compileall -q airflow/dags lambda/trigger_glue
```

These are local/offline checks. They do not prove that AWS IAM, EventBridge,
Lambda, Glue, S3, the crawler or Athena is correctly configured. Validate
those integrations in DEV only after the AWS stack and job are deployed.

## Operational cautions

- The Lambda idempotency record is keyed by bucket/key/ETag and expires after
  30 days. Re-uploading the same key with a different ETag is a new object
  version and starts a new run.
- The Glue ETL currently writes with overwrite mode. Use unique run-date
  output prefixes and confirm rerun behavior before production use.
- Airflow retries a failed task; it does not blindly retry a Glue run that
  has already reached a terminal failed state. Investigate the Glue error,
  correct the cause, and trigger a controlled rerun.
- The crawler runs only after a successful Glue run. Athena tables become
  queryable after the crawler completes.
- The local Docker Compose Airflow stack is for development, not a secured
  production scheduler. Production requires managed secrets, authentication,
  RBAC, monitoring, backup, network controls and deployment automation.
