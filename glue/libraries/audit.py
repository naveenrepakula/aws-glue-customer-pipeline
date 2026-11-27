"""Audit output helper shared by Glue runtime and local integration tests."""
import json
import os
from pathlib import Path


def write_audit_log(bucket_name: str, audit_key: str, audit_data: dict) -> str:
    """Write JSON audit data locally for tests or to S3 in AWS Glue."""
    local_audit_dir = os.getenv("COADP_LOCAL_AUDIT_DIR")
    if local_audit_dir:
        audit_file = Path(local_audit_dir) / audit_key
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        audit_file.write_text(json.dumps(audit_data, indent=2), encoding="utf-8")
        return str(audit_file)

    import boto3

    boto3.client("s3").put_object(
        Bucket=bucket_name,
        Key=audit_key,
        Body=json.dumps(audit_data).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket_name}/{audit_key}"
