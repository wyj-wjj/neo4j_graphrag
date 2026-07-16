"""Create the local Compose object bucket only when explicitly authorized."""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError


def main() -> None:
    if os.getenv("ALLOW_S3_BUCKET_BOOTSTRAP", "").lower() != "true":
        raise RuntimeError("S3 bucket bootstrap requires ALLOW_S3_BUCKET_BOOTSTRAP=true")
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if str(error.get("Code", "")) not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)
    finally:
        client.close()


if __name__ == "__main__":
    main()
