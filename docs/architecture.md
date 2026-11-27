# AWS Glue Customer Pipeline Architecture

## Processing and control flow

The data path is S3 Raw CSV → Lambda → the existing AWS Glue PySpark job →
Curated and Rejected S3 prefixes → Glue Data Catalog → Athena.

```text
S3 raw/customers/*.csv
        │ S3 Object Created through EventBridge
        ▼
Trigger Lambda ── DynamoDB idempotency record (bucket/key/ETag)
        │ StartJobRun
        ▼
COADP-Customer-ETL-DEV (existing job; not created by this template)
        ├── s3://<data-bucket>/curated/customers/run_date=YYYYMMDD/
        ├── s3://<data-bucket>/rejected/customers/run_date=YYYYMMDD/
        └── s3://<data-bucket>/audit/customers/<job>/<timestamp>.json
                    │
                    ▼
            Glue crawler and Data Catalog
                    │
                    ▼
                  Athena

Airflow (daily schedule) → S3 key sensor → invoke the same Lambda
                         → monitor the returned Glue JobRunId
                         → refresh the crawler after success
```

S3 EventBridge delivery and Airflow are two trigger/control paths for the same
job. Airflow remains the scheduled orchestration layer; it does not replace
S3 event processing or carry transformation logic. Both paths provide the
object ETag so DynamoDB can deduplicate the same object version.

## AWS resources and ownership

`infrastructure/template.yaml` creates the trigger Lambda and its execution
role, DynamoDB idempotency table, EventBridge rule and invoke permission, Glue
database and crawler, crawler role, and Athena workgroup. It takes existing
data and artifact buckets as parameters. It does not create buckets or the
Glue ETL job. Keep the job name `COADP-Customer-ETL-DEV` unless an explicitly
approved migration changes that dependency. The unrelated
`cdp-customer-pipeline-dev` Lambda is outside this repository's scope.

The existing data bucket must have S3 EventBridge notifications enabled for
the rule to receive object-created events. That bucket-level setting is not
managed by this template: changing a bucket's notification configuration can
replace other notification destinations. Review and preserve the full
existing configuration when enabling it.

## Identity and permissions

- **Trigger Lambda role:** CloudWatch Logs writes, `glue:StartJobRun` and
  `glue:GetJobRun` on the configured Glue job, and Get/Put/Update on only the
  idempotency table.
- **Airflow identity:** S3 list/head access for the raw input, invoke access
  for the trigger Lambda, Glue GetJobRun, and crawler start/status access.
- **Glue job execution role:** read raw CSV and Glue script/library artifacts;
  write curated, rejected, and audit objects. The checked-in sample policy
  only includes the older `glue/*` artifact path; a job using
  `releases/<revision>/*` needs matching read access.
- **Crawler role:** list the data bucket and read curated/rejected prefixes.
- **Athena caller:** Glue Catalog read and Athena query permissions, plus S3
  access to the workgroup result prefix. Creating an Athena workgroup does not
  grant those permissions or guarantee its result bucket policy.

Use separate roles and narrow resource scopes. In particular, Lambda does not
need broad S3 access, and Airflow does not need Glue job start permission
because it invokes Lambda.

## Idempotency and output behavior

The Lambda's conditional DynamoDB put claims `bucket/key#etag`; existing
records with a Glue run ID return that run unless it ended in a terminal
failure. Failed runs can be started again for the same object. A record left
in `STARTING` without a run ID has no lease recovery today, and a timeout in
the Glue start response can leave uncertainty about whether a run started.
This is deduplication with bounded retry behavior, not a strict exactly-once
guarantee. DynamoDB TTL cleanup is asynchronous.

The output date is parsed from a `customers_YYYYMMDD.csv` filename. Other raw
CSV names use the current UTC date. Glue writes in overwrite mode, so separate
objects mapping to the same date share output locations and can overwrite one
another. Confirm the naming and rerun policy before enabling production
ingestion.

The crawler targets curated and rejected prefixes and updates the configured
database. Airflow refreshes it after its monitored Glue run succeeds. A
standalone EventBridge-triggered run does not currently start the crawler, so
catalog freshness for event-only runs depends on a later scheduled crawler
refresh or an operational refresh.

## Local checks and deployment boundary

Unit and local integration tests use mocked AWS clients or local Spark. They
do not verify live IAM, S3 EventBridge delivery, Glue behavior, crawler table
discovery, or Athena query access. Review `COADP-3-deployment.md` and
`airflow-setup.md` before a separately approved deployment. CI runs validation
only and does not upload artifacts, update AWS resources, or start Glue.

No AWS deployment, Lambda invocation, data upload, or Glue run is part of
local development validation. Perform those actions only after explicit
approval and a reviewed deployment plan.
