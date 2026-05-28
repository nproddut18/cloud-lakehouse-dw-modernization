"""
AWS connection helpers — Glue context, S3 client, Redshift connections.
"""

import boto3
from config.settings import S3, REDSHIFT
from utils.logger import get_logger

logger = get_logger(__name__)


def get_s3_client():
    """Return a boto3 S3 client using ambient AWS credentials."""
    return boto3.client("s3", region_name=S3.region)


def get_glue_client():
    """Return a boto3 Glue client."""
    return boto3.client("glue", region_name=S3.region)


def get_redshift_connection():
    """
    Return a redshift-connector connection.
    Requires: pip install redshift-connector
    """
    import redshift_connector  # type: ignore

    logger.info(f"Connecting to Redshift: {REDSHIFT.host}:{REDSHIFT.port}/{REDSHIFT.database}")
    conn = redshift_connector.connect(
        host=REDSHIFT.host,
        port=REDSHIFT.port,
        database=REDSHIFT.database,
        user=REDSHIFT.user,
        password=REDSHIFT.password,
    )
    conn.autocommit = False
    return conn


def list_s3_objects(prefix: str, suffix: str = "") -> list[str]:
    """List all S3 object keys under a given prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not suffix or key.endswith(suffix):
                keys.append(key)
    logger.debug(f"Found {len(keys)} objects under s3://{S3.bucket}/{prefix}")
    return keys
