"""
S3 Ingestion — loads raw source files into the Bronze zone on S3.

Supports CSV, JSON, and Parquet source formats.
Adds ingestion metadata (load_timestamp, source_file_name) before writing.

Usage:
    python ingestion/s3_ingestion.py --source orders --date 2025-01-15
"""

import argparse
import datetime
import os
from pathlib import Path

import boto3
import pandas as pd

from config.settings import S3, PIPELINE
from config.aws_config import get_s3_client
from utils.logger import get_logger
from utils.data_quality import assert_not_empty, assert_no_nulls_in_key_columns

logger = get_logger(__name__)

# Source → expected format mapping
SOURCE_CONFIG = {
    "customers":    {"format": "csv",     "key_cols": ["customer_id"]},
    "orders":       {"format": "parquet", "key_cols": ["order_id"]},
    "transactions": {"format": "json",    "key_cols": ["txn_id", "order_id"]},
    "products":     {"format": "csv",     "key_cols": ["product_id"]},
}


def read_source_file(source: str, date: str, local_dir: str = "data/raw") -> pd.DataFrame:
    """
    Read a raw source file from the local staging directory.
    In production this would pull from an SFTP, RDS export, or upstream API.
    """
    cfg = SOURCE_CONFIG[source]
    fmt = cfg["format"]
    filename = f"{source}_{date}.{fmt}"
    filepath = Path(local_dir) / filename

    logger.info(f"Reading source file: {filepath}")

    if fmt == "csv":
        df = pd.read_csv(filepath)
    elif fmt == "json":
        df = pd.read_json(filepath, lines=True)
    elif fmt == "parquet":
        df = pd.read_parquet(filepath)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    logger.info(f"Loaded {len(df):,} rows from {filename}")
    return df


def add_ingestion_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Stamp each row with load timestamp and source filename."""
    df = df.copy()
    df["_ingested_at"] = datetime.datetime.utcnow().isoformat()
    df["_source_file"] = source_file
    df["_pipeline_env"] = PIPELINE.env
    return df


def write_to_bronze(df: pd.DataFrame, source: str, date: str) -> str:
    """
    Write DataFrame to S3 Bronze zone as Parquet, partitioned by date.
    Returns the S3 destination path.
    """
    s3_key = f"{S3.bronze_prefix}/{source}/dt={date}/data.parquet"
    local_tmp = f"/tmp/{source}_{date}_bronze.parquet"

    df.to_parquet(local_tmp, index=False, engine="pyarrow")

    s3 = get_s3_client()
    s3.upload_file(local_tmp, S3.bucket, s3_key)

    dest = f"s3://{S3.bucket}/{s3_key}"
    logger.info(f"Bronze write complete → {dest}  ({len(df):,} rows)")
    return dest


def run_ingestion(source: str, date: str) -> None:
    """Full ingestion pipeline: read → validate → stamp → write to Bronze."""
    logger.info(f"Starting ingestion: source={source}, date={date}")

    cfg = SOURCE_CONFIG[source]
    df = read_source_file(source, date)

    # Data quality gates
    assert_not_empty(df, label=f"{source} raw")
    assert_no_nulls_in_key_columns(df, key_cols=cfg["key_cols"], label=source)

    df = add_ingestion_metadata(df, source_file=f"{source}_{date}")
    dest = write_to_bronze(df, source, date)

    logger.info(f"Ingestion complete for {source} → {dest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 Bronze ingestion")
    parser.add_argument("--source", required=True, choices=list(SOURCE_CONFIG.keys()))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    run_ingestion(source=args.source, date=args.date)
