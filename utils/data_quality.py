"""
Data quality assertion helpers — lightweight, no external dependency required.
Use these at layer boundaries (Bronze → Silver → Gold → Warehouse) to gate
pipeline progression on data integrity checks.
"""

import pandas as pd
from pyspark.sql import DataFrame as SparkDF
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Pandas checks ─────────────────────────────────────────────────────────────

def assert_not_empty(df: pd.DataFrame, label: str = "DataFrame") -> None:
    if len(df) == 0:
        raise AssertionError(f"[DQ FAIL] {label} is empty — expected at least 1 row.")
    logger.debug(f"[DQ PASS] {label}: not empty ({len(df):,} rows)")


def assert_no_nulls_in_key_columns(df: pd.DataFrame, key_cols: list[str], label: str = "") -> None:
    for col in key_cols:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise AssertionError(
                f"[DQ FAIL] {label}.{col}: {null_count:,} null values in key column."
            )
    logger.debug(f"[DQ PASS] {label}: no nulls in key columns {key_cols}")


def assert_unique(df: pd.DataFrame, key_cols: list[str], label: str = "") -> None:
    dupes = df.duplicated(subset=key_cols).sum()
    if dupes > 0:
        raise AssertionError(
            f"[DQ FAIL] {label}: {dupes:,} duplicate rows on key columns {key_cols}."
        )
    logger.debug(f"[DQ PASS] {label}: unique on {key_cols}")


def assert_value_in_set(df: pd.DataFrame, col: str, valid_values: set, label: str = "") -> None:
    invalid = df[~df[col].isin(valid_values)][col].unique()
    if len(invalid) > 0:
        raise AssertionError(
            f"[DQ FAIL] {label}.{col}: unexpected values: {invalid[:10].tolist()}"
        )
    logger.debug(f"[DQ PASS] {label}.{col}: all values in valid set")


def assert_row_count_in_range(df: pd.DataFrame, min_rows: int, max_rows: int, label: str = "") -> None:
    n = len(df)
    if not (min_rows <= n <= max_rows):
        raise AssertionError(
            f"[DQ FAIL] {label}: row count {n:,} not in expected range [{min_rows:,}, {max_rows:,}]"
        )
    logger.debug(f"[DQ PASS] {label}: row count {n:,} in range")


# ── PySpark checks ─────────────────────────────────────────────────────────────

def spark_assert_not_empty(df: SparkDF, label: str = "SparkDF") -> None:
    if df.rdd.isEmpty():
        raise AssertionError(f"[DQ FAIL] {label} Spark DataFrame is empty.")
    logger.debug(f"[DQ PASS] {label}: Spark DataFrame not empty")


def spark_assert_no_nulls(df: SparkDF, cols: list[str], label: str = "") -> None:
    from pyspark.sql import functions as F
    for col in cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise AssertionError(f"[DQ FAIL] {label}.{col}: {null_count:,} nulls in Spark DataFrame.")
    logger.debug(f"[DQ PASS] {label}: no nulls in {cols}")
