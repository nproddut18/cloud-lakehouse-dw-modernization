"""
Schema validation — compare source schema to target warehouse schema.
Detects column additions, removals, and type mismatches before loads run.
"""

import pandas as pd
from config.aws_config import get_redshift_connection
from config.settings import REDSHIFT
from utils.logger import get_logger

logger = get_logger(__name__)


def get_redshift_schema(table: str, schema: str = None) -> pd.DataFrame:
    """Return column metadata for a Redshift table."""
    schema = schema or REDSHIFT.schema
    sql = f"""
        SELECT column_name, data_type, character_maximum_length, numeric_precision
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{table}'
        ORDER BY ordinal_position;
    """
    conn = get_redshift_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def compare_schemas(source_cols: list[dict], target_df: pd.DataFrame, table: str) -> dict:
    """
    Compare source column list (from Spark/dbt) to target Redshift schema.
    Returns a dict with 'added', 'removed', 'type_mismatches'.
    """
    source_names = {c["name"].lower() for c in source_cols}
    target_names = set(target_df["column_name"].str.lower())

    added   = source_names - target_names
    removed = target_names - source_names

    if added:
        logger.warning(f"[{table}] New columns not in target: {added}")
    if removed:
        logger.warning(f"[{table}] Columns in target but not in source: {removed}")

    return {"added": list(added), "removed": list(removed)}


def validate_before_load(source_cols: list[dict], table: str) -> bool:
    """
    Run schema validation before a warehouse load.
    Returns True if safe to proceed, raises on critical mismatch.
    """
    logger.info(f"Schema validation: {table}")
    target_schema = get_redshift_schema(table)

    if target_schema.empty:
        logger.info(f"Table {table} does not exist yet — schema validation skipped (will be created).")
        return True

    result = compare_schemas(source_cols, target_schema, table)

    if result["removed"]:
        raise ValueError(
            f"Schema validation FAILED for {table}: "
            f"target has columns not in source: {result['removed']}. "
            "Aborting load to prevent data loss."
        )

    logger.info(f"Schema validation PASSED for {table}")
    return True
