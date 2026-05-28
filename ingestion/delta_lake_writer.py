"""
Delta Lake writer utilities — upsert (MERGE), append, overwrite partition.

Used by both Glue jobs and local PySpark transformation scripts.
"""

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable  # type: ignore
from utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_with_delta(app_name: str = "LakehousePipeline") -> SparkSession:
    """
    Build a SparkSession with Delta Lake extensions enabled.
    Call this once per application entry point.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    delta_path: str,
    merge_keys: list[str],
    update_cols: list[str] | None = None,
) -> None:
    """
    MERGE (upsert) new DataFrame into an existing Delta table.
    - If the Delta table does not exist, creates it from df.
    - merge_keys: columns to match on (e.g. ["order_id"])
    - update_cols: columns to update on match (None = update all)
    """
    if not DeltaTable.isDeltaTable(spark, delta_path):
        logger.info(f"Delta table not found at {delta_path} — creating new table.")
        df.write.format("delta").mode("overwrite").save(delta_path)
        return

    delta_tbl = DeltaTable.forPath(spark, delta_path)

    condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    if update_cols:
        update_map = {c: f"source.{c}" for c in update_cols}
    else:
        update_map = {c: f"source.{c}" for c in df.columns}

    insert_map = {c: f"source.{c}" for c in df.columns}

    (
        delta_tbl.alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    logger.info(f"Upsert complete → {delta_path}")


def append_delta(df: DataFrame, delta_path: str) -> None:
    """Append-only write to Delta Lake (e.g., for immutable event tables)."""
    df.write.format("delta").mode("append").save(delta_path)
    logger.info(f"Append complete → {delta_path}  ({df.count():,} rows)")


def overwrite_partition(df: DataFrame, delta_path: str, partition_col: str, partition_val: str) -> None:
    """
    Overwrite a single partition in a Delta table without touching other partitions.
    Useful for idempotent daily loads.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} = '{partition_val}'")
        .save(delta_path)
    )
    logger.info(f"Partition overwrite complete → {delta_path} [{partition_col}={partition_val}]")
