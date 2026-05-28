"""
AWS Glue PySpark Job — Bronze → Silver cleansing layer.

This script is deployed as an AWS Glue 4.0 job. It reads raw Parquet files
from the S3 Bronze zone, applies cleansing transformations (deduplication,
null handling, type casting, standardized column naming), and writes
cleansed data to the S3 Silver zone as Delta Lake tables.

Deploy via AWS Console or CloudFormation. Run locally with glue_pyspark.
"""

import sys
from awsglue.transforms import *  # noqa: F401, F403
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, DateType, BooleanType,
)

# ── Job arguments ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_table",     # e.g. orders
    "batch_date",       # e.g. 2025-01-15
    "s3_bucket",
    "environment",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

SOURCE_TABLE = args["source_table"]
BATCH_DATE   = args["batch_date"]
S3_BUCKET    = args["s3_bucket"]
ENV          = args["environment"]

BRONZE_PATH  = f"s3://{S3_BUCKET}/bronze/{SOURCE_TABLE}/dt={BATCH_DATE}/"
SILVER_PATH  = f"s3://{S3_BUCKET}/silver/{SOURCE_TABLE}/"

# ── Schema definitions (enforce on read) ──────────────────────────────────────
SCHEMAS = {
    "orders": StructType([
        StructField("order_id",       StringType(),    False),
        StructField("customer_id",    StringType(),    False),
        StructField("product_id",     StringType(),    True),
        StructField("order_date",     DateType(),      True),
        StructField("order_amount",   DoubleType(),    True),
        StructField("quantity",       LongType(),      True),
        StructField("status",         StringType(),    True),
        StructField("_ingested_at",   TimestampType(), True),
        StructField("_source_file",   StringType(),    True),
    ]),
    "customers": StructType([
        StructField("customer_id",    StringType(),    False),
        StructField("first_name",     StringType(),    True),
        StructField("last_name",      StringType(),    True),
        StructField("email",          StringType(),    True),
        StructField("country",        StringType(),    True),
        StructField("created_at",     TimestampType(), True),
        StructField("is_active",      BooleanType(),   True),
        StructField("_ingested_at",   TimestampType(), True),
        StructField("_source_file",   StringType(),    True),
    ]),
    "transactions": StructType([
        StructField("txn_id",         StringType(),    False),
        StructField("order_id",       StringType(),    False),
        StructField("txn_timestamp",  TimestampType(), True),
        StructField("amount",         DoubleType(),    True),
        StructField("currency",       StringType(),    True),
        StructField("payment_method", StringType(),    True),
        StructField("is_fraud",       BooleanType(),   True),
        StructField("_ingested_at",   TimestampType(), True),
        StructField("_source_file",   StringType(),    True),
    ]),
}


def read_bronze(table: str, path: str):
    schema = SCHEMAS.get(table)
    if schema:
        return spark.read.schema(schema).parquet(path)
    return spark.read.parquet(path)


def cleanse(df, table: str):
    """Apply common cleansing rules."""
    # 1. Remove exact duplicates
    df = df.dropDuplicates()

    # 2. Trim string columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # 3. Normalize empty strings to null
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))

    # 4. Add Silver metadata
    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    df = df.withColumn("_batch_date", F.lit(BATCH_DATE))
    df = df.withColumn("_env", F.lit(ENV))

    return df


def write_silver_delta(df, path: str) -> None:
    """Write cleansed data to Silver zone as Delta Lake table (merge-on-read)."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{BATCH_DATE}'")
        .partitionBy("_batch_date")
        .save(path)
    )


# ── Main ───────────────────────────────────────────────────────────────────────
print(f"[Glue] Reading Bronze: {BRONZE_PATH}")
raw_df = read_bronze(SOURCE_TABLE, BRONZE_PATH)
print(f"[Glue] Raw row count: {raw_df.count():,}")

clean_df = cleanse(raw_df, SOURCE_TABLE)
print(f"[Glue] Clean row count: {clean_df.count():,}")

print(f"[Glue] Writing Silver Delta: {SILVER_PATH}")
write_silver_delta(clean_df, SILVER_PATH)
print("[Glue] Silver write complete.")

job.commit()
