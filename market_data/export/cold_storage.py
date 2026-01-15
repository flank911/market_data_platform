from __future__ import annotations

import os
from typing import Iterable

import boto3

from market_data.config.settings import S3Config


def upload_dataset_to_s3(
    *,
    s3: S3Config,
    dataset_id: str,
    base_dir: str,
    extra_files: Iterable[str] = (),
) -> str:
    if not s3.endpoint_url or not s3.access_key or not s3.secret_key:
        raise RuntimeError("S3 is not configured (endpoint/access/secret)")

    client = boto3.client(
        "s3",
        endpoint_url=s3.endpoint_url,
        aws_access_key_id=s3.access_key,
        aws_secret_access_key=s3.secret_key,
        region_name=s3.region,
    )

    prefix = f"{s3.prefix.rstrip('/')}/{dataset_id}"
    uploads = [
        os.path.join(base_dir, dataset_id, "candles.parquet"),
        os.path.join(base_dir, dataset_id, "features.parquet"),
        os.path.join(base_dir, dataset_id, "metadata.yaml"),
    ]
    uploads.extend(extra_files)

    for path in uploads:
        if not os.path.exists(path):
            continue
        key = f"{prefix}/{os.path.basename(path)}"
        client.upload_file(path, s3.bucket, key)
    return f"s3://{s3.bucket}/{prefix}"


