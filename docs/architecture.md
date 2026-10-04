# Customer Data Pipeline Architecture

## DEV S3 Storage

The customer data pipeline uses two Amazon S3 buckets:

1. Customer Data Bucket
2. Glue Artifacts Bucket

Both buckets are provisioned and managed using AWS CloudFormation.

---

## S3 Storage Structure

```text
Customer Data Bucket
│
├── raw/
│   └── customers/
│       └── Incoming customer source files
│
├── curated/
│   └── customers/
│       └── Validated and transformed customer data
│
└── rejected/
    └── customers/
        └── Records that fail data-quality validation


Glue Artifacts Bucket
│
└── scripts/
    └── AWS Glue ETL scripts
```

---

## Customer Data Bucket

The Customer Data Bucket stores customer data throughout the ETL lifecycle.

### raw/customers/

The `raw/customers/` prefix is the landing location for incoming customer source files.

Example:

```text
raw/customers/customers_20261004.csv
```

Future pipeline components can configure an Amazon S3 `ObjectCreated` event for this prefix to start customer-data processing.

Expected flow:

```text
Source System
     |
     v
raw/customers/
     |
     v
AWS Glue ETL
```

---

### curated/customers/

The `curated/customers/` prefix stores customer records that successfully pass validation, cleaning, and transformation.

AWS Glue will eventually write processed data to this location.

Example:

```text
curated/customers/year=2026/month=10/day=04/part-00000.parquet
```

The curated layer is intended to contain analytics-ready customer data.

---

### rejected/customers/

The `rejected/customers/` prefix stores records that fail schema validation or data-quality checks.

Examples of rejected records may include:

- Missing required customer IDs
- Invalid data types
- Invalid email formats
- Malformed records
- Records that violate data-quality rules

Example:

```text
rejected/customers/year=2026/month=10/day=04/rejected-records.json
```

Keeping rejected records separate from curated data prevents invalid records from reaching downstream analytics systems.

---

## Glue Artifacts Bucket

The Glue Artifacts Bucket stores deployment artifacts required by AWS Glue.

### scripts/

The `scripts/` prefix is used to store AWS Glue ETL scripts.

Example:

```text
scripts/customer_etl.py
```

A deployed AWS Glue job can reference the script using an S3 location such as:

```text
s3://<glue-artifacts-bucket>/scripts/customer_etl.py
```

---

## Data Processing Flow

The planned customer-data processing flow is:

```text
Source Systems
      |
      v
raw/customers/
      |
      v
AWS Glue / PySpark
      |
      | Validate
      | Clean
      | Transform
      |
      +-----------------------+
      |                       |
      v                       v
Valid Records           Invalid Records
      |                       |
      v                       v
curated/customers/      rejected/customers/
```

The `raw` layer preserves incoming source data.

The `curated` layer contains successfully processed data.

The `rejected` layer contains records that require investigation or remediation.

---

## S3 Prefix Design

Amazon S3 uses object keys rather than a traditional directory-based file system.

Paths such as:

```text
raw/customers/
curated/customers/
rejected/customers/
scripts/
```

are logical S3 object-key prefixes.

These prefixes define where different pipeline components read and write data.

---

## Security

Both DEV S3 buckets are configured through AWS CloudFormation with server-side encryption and public-access protection.

### Server-Side Encryption

The buckets use Amazon S3 server-side encryption with AES-256.

CloudFormation configuration:

```yaml
BucketEncryption:
  ServerSideEncryptionConfiguration:
    - ServerSideEncryptionByDefault:
        SSEAlgorithm: AES256
```

### Public Access Protection

The following S3 public-access controls are enabled:

```text
BlockPublicAcls       = true
IgnorePublicAcls      = true
BlockPublicPolicy     = true
RestrictPublicBuckets = true
```

This prevents the pipeline buckets from being accidentally exposed publicly.

---

## Infrastructure Management

The DEV S3 infrastructure is provisioned using AWS CloudFormation.

The infrastructure template is located at:

```text
infrastructure/template.yaml
```

DEV environment parameters are maintained in:

```text
infrastructure/parameters/dev.json
```

Infrastructure changes should be made through Infrastructure as Code rather than manually creating AWS resources.

---

## Resource Naming Convention

AWS resources follow the naming convention:

```text
<project-name>-<environment>-<aws-account-id>-<purpose>
```

Examples:

```text
customer-pipeline-dev-<account-id>-data
customer-pipeline-dev-<account-id>-glue-artifacts
```

The AWS account ID is supplied dynamically by CloudFormation using the `AWS::AccountId` pseudo parameter.

The environment is supplied using the CloudFormation `Environment` parameter.

This approach allows the same CloudFormation template to be reused across environments such as:

```text
DEV
TEST
PROD
```

---

## DEV Infrastructure

The current DEV architecture is:

```text
AWS Account
│
└── CloudFormation
    │
    └── customer-pipeline-dev
        │
        ├── CustomerDataBucket
        │   │
        │   ├── raw/customers/
        │   ├── curated/customers/
        │   └── rejected/customers/
        │
        └── GlueArtifactsBucket
            │
            └── scripts/
```

---

## Future Pipeline Integration

Future tickets will extend this storage layer with processing and orchestration components.

The planned high-level architecture is:

```text
Source Systems
      |
      v
Amazon S3
raw/customers/
      |
      | ObjectCreated Event
      v
AWS Lambda
      |
      | Start Glue Job
      v
AWS Glue / PySpark
      |
      +-----------------------+
      |                       |
      v                       v
curated/customers/      rejected/customers/
      |
      v
AWS Glue Data Catalog
      |
      v
Amazon Athena
```

Additional monitoring and orchestration components can include Amazon CloudWatch and Apache Airflow.
