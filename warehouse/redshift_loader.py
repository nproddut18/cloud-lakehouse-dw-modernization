"""
Amazon Redshift loader — COPY from S3 (bulk load) and incremental MERGE.

Handles:
- Initial full loads via COPY command (fastest bulk path)
- Incremental merges using a staging table pattern
- Sort key maintenance via VACUUM SORT ONLY after large loads
"""

import redshift_connector  # type: ignore

from config.settings import REDSHIFT, S3
from config.aws_config import get_redshift_connection
from utils.logger import get_logger
from utils.reconciliation import row_count_check

logger = get_logger(__name__)


# ── Table definitions (sort/dist keys) ────────────────────────────────────────
TABLE_DDL = {
    "fact_orders": """
        CREATE TABLE IF NOT EXISTS {schema}.fact_orders (
            order_sk          BIGINT         NOT NULL ENCODE AZ64,
            order_id          VARCHAR(64)    NOT NULL ENCODE ZSTD,
            customer_sk       BIGINT                  ENCODE AZ64,
            product_sk        BIGINT                  ENCODE AZ64,
            order_date_sk     INTEGER                 ENCODE AZ64,
            order_date        DATE                    ENCODE AZ64,
            order_amount      DECIMAL(18,2)           ENCODE AZ64,
            quantity          INTEGER                 ENCODE AZ64,
            status            VARCHAR(32)             ENCODE ZSTD,
            dbt_updated_at    TIMESTAMP               ENCODE AZ64,
            _batch_date       VARCHAR(10)             ENCODE ZSTD
        )
        DISTKEY(customer_sk)
        SORTKEY(order_date_sk, customer_sk);
    """,
    "fact_transactions": """
        CREATE TABLE IF NOT EXISTS {schema}.fact_transactions (
            txn_id            VARCHAR(64)    NOT NULL ENCODE ZSTD,
            order_sk          BIGINT                  ENCODE AZ64,
            customer_sk       BIGINT                  ENCODE AZ64,
            txn_date_sk       INTEGER                 ENCODE AZ64,
            txn_timestamp     TIMESTAMP               ENCODE AZ64,
            amount            DECIMAL(18,2)           ENCODE AZ64,
            currency          VARCHAR(8)              ENCODE ZSTD,
            payment_method    VARCHAR(32)             ENCODE ZSTD,
            is_fraud          BOOLEAN                 ENCODE RAW,
            risk_tier         VARCHAR(8)              ENCODE ZSTD,
            dbt_updated_at    TIMESTAMP               ENCODE AZ64,
            _batch_date       VARCHAR(10)             ENCODE ZSTD
        )
        DISTKEY(customer_sk)
        SORTKEY(txn_date_sk, customer_sk);
    """,
}


def create_table_if_not_exists(conn, table: str, schema: str) -> None:
    ddl = TABLE_DDL.get(table, "")
    if not ddl:
        logger.warning(f"No DDL found for table: {table}")
        return
    with conn.cursor() as cur:
        cur.execute(ddl.format(schema=schema))
    conn.commit()
    logger.info(f"Ensured table exists: {schema}.{table}")


def copy_from_s3(conn, table: str, s3_path: str, schema: str = None) -> None:
    """
    Full load via Redshift COPY from S3 Parquet.
    Fastest path for initial or monthly full refreshes.
    """
    schema = schema or REDSHIFT.schema
    sql = f"""
        COPY {schema}.{table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT.iam_role}'
        FORMAT AS PARQUET;
    """
    logger.info(f"COPY {schema}.{table} ← {s3_path}")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"COPY complete: {schema}.{table}")


def incremental_merge(
    conn,
    table: str,
    s3_path: str,
    merge_key: str,
    schema: str = None,
) -> None:
    """
    Incremental MERGE pattern:
      1. Load new data into a staging table
      2. DELETE matching rows from target
      3. INSERT from staging
      4. Drop staging
    """
    schema = schema or REDSHIFT.schema
    staging = f"{table}_staging_{merge_key}"

    with conn.cursor() as cur:
        # Step 1: Create staging
        cur.execute(f"CREATE TEMP TABLE {staging} (LIKE {schema}.{table});")

        # Step 2: COPY new data into staging
        cur.execute(f"""
            COPY {staging}
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT.iam_role}'
            FORMAT AS PARQUET;
        """)

        # Step 3: Delete matching rows from target
        cur.execute(f"""
            DELETE FROM {schema}.{table}
            USING {staging}
            WHERE {schema}.{table}.{merge_key} = {staging}.{merge_key};
        """)

        # Step 4: Insert from staging
        cur.execute(f"""
            INSERT INTO {schema}.{table}
            SELECT * FROM {staging};
        """)

        # Step 5: Drop staging
        cur.execute(f"DROP TABLE {staging};")

    conn.commit()
    logger.info(f"Incremental merge complete: {schema}.{table} on key={merge_key}")


def vacuum_sort(conn, table: str, schema: str = None) -> None:
    """Run VACUUM SORT ONLY to reclaim sort order after large loads."""
    schema = schema or REDSHIFT.schema
    with conn.cursor() as cur:
        cur.execute(f"VACUUM SORT ONLY {schema}.{table};")
    logger.info(f"VACUUM SORT ONLY complete: {schema}.{table}")


def load_table(table: str, s3_path: str, mode: str = "incremental", merge_key: str = None) -> None:
    """High-level entry point — connect, load, validate."""
    conn = get_redshift_connection()
    try:
        create_table_if_not_exists(conn, table, REDSHIFT.schema)

        if mode == "full":
            copy_from_s3(conn, table, s3_path)
        else:
            if not merge_key:
                raise ValueError("merge_key required for incremental mode")
            incremental_merge(conn, table, s3_path, merge_key)

        vacuum_sort(conn, table)

        # Post-load row count check
        row_count_check(conn, table=f"{REDSHIFT.schema}.{table}", min_expected=1)

    finally:
        conn.close()
