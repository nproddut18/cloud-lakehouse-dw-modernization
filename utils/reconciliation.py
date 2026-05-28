"""
Reconciliation checks — compare source counts/sums against warehouse targets.
Produces a reconciliation report that can be logged or stored in an audit table.
"""

import pandas as pd
from config.aws_config import get_redshift_connection
from config.settings import REDSHIFT
from utils.logger import get_logger

logger = get_logger(__name__)


def row_count_check(conn, table: str, min_expected: int = 1) -> int:
    """
    Query Redshift for a table's row count and assert it meets a minimum.
    Returns the actual row count.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(1) FROM {table};")
        count = cur.fetchone()[0]

    if count < min_expected:
        raise AssertionError(
            f"[RECON FAIL] {table}: {count:,} rows — expected at least {min_expected:,}."
        )

    logger.info(f"[RECON PASS] {table}: {count:,} rows")
    return count


def sum_check(conn, table: str, col: str, expected_sum: float, tolerance_pct: float = 0.01) -> float:
    """
    Assert that the sum of a numeric column in Redshift is within tolerance of an expected value.
    Useful for validating financial totals after load.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT SUM({col}) FROM {table};")
        actual = float(cur.fetchone()[0] or 0)

    diff_pct = abs(actual - expected_sum) / max(abs(expected_sum), 1)
    if diff_pct > tolerance_pct:
        raise AssertionError(
            f"[RECON FAIL] {table}.{col}: sum={actual:,.2f}, expected={expected_sum:,.2f} "
            f"(diff={diff_pct:.2%} > tolerance={tolerance_pct:.2%})"
        )

    logger.info(f"[RECON PASS] {table}.{col}: sum={actual:,.2f} within {tolerance_pct:.1%} tolerance")
    return actual


def full_reconciliation_report(
    source_counts: dict[str, int],
    batch_date: str,
) -> pd.DataFrame:
    """
    Compare source row counts (e.g., from S3/Glue) to Redshift row counts.
    Returns a DataFrame with pass/fail status per table.
    """
    conn = get_redshift_connection()
    rows = []
    try:
        for table, source_count in source_counts.items():
            full_table = f"{REDSHIFT.schema}.{table}"
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT(1) FROM {full_table} WHERE _batch_date = '{batch_date}';"
                    )
                    target_count = cur.fetchone()[0]
                diff = target_count - source_count
                status = "PASS" if abs(diff) == 0 else "WARN" if abs(diff) < 10 else "FAIL"
            except Exception as e:
                target_count = -1
                diff = -1
                status = f"ERROR: {e}"

            rows.append({
                "table":        table,
                "batch_date":   batch_date,
                "source_count": source_count,
                "target_count": target_count,
                "diff":         diff,
                "status":       status,
            })
    finally:
        conn.close()

    report = pd.DataFrame(rows)
    logger.info(f"Reconciliation report:\n{report.to_string(index=False)}")
    return report
