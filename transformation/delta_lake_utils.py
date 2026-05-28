"""
Delta Lake maintenance utilities — OPTIMIZE, VACUUM, schema evolution checks.
Run these periodically (e.g., weekly) to keep Delta tables healthy.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable  # type: ignore
from config.settings import S3
from utils.logger import get_logger

logger = get_logger(__name__)

MANAGED_TABLES = ["orders", "transactions", "customers", "products"]


def optimize_table(spark: SparkSession, path: str, z_order_cols: list[str] | None = None) -> None:
    """
    Run OPTIMIZE on a Delta table to compact small files.
    Optionally apply Z-ORDER for better data skipping on high-cardinality columns.
    """
    dt = DeltaTable.forPath(spark, path)
    if z_order_cols:
        cols_str = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})")
        logger.info(f"OPTIMIZE + ZORDER({cols_str}) complete → {path}")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        logger.info(f"OPTIMIZE complete → {path}")


def vacuum_table(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    """
    VACUUM old Delta files. Default retention = 7 days (168h).
    Never set retain_hours below 168 in production without disabling the safety check.
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    logger.info(f"VACUUM complete → {path} (retained {retain_hours}h)")


def get_table_history(spark: SparkSession, path: str, limit: int = 10):
    """Return the last N operations on a Delta table for auditing."""
    return DeltaTable.forPath(spark, path).history(limit)


def time_travel_read(spark: SparkSession, path: str, version: int = None, timestamp: str = None):
    """
    Read a Delta table at a specific version or timestamp.
    Use for debugging or rollback validation.
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(path)


def run_maintenance(spark: SparkSession) -> None:
    """Run OPTIMIZE + VACUUM on all managed Gold tables."""
    z_order_map = {
        "orders":       ["customer_id", "order_date"],
        "transactions": ["customer_id", "txn_timestamp"],
        "customers":    ["customer_id"],
        "products":     ["product_id", "category"],
    }
    for table in MANAGED_TABLES:
        path = S3.gold_path(table)
        try:
            optimize_table(spark, path, z_order_cols=z_order_map.get(table))
            vacuum_table(spark, path)
        except Exception as exc:
            logger.error(f"Maintenance failed for {table}: {exc}")
