"""
Snowflake loader — bulk COPY from S3 stage and MERGE for data mart tables.

Assumes an external S3 stage named LAKEHOUSE_STAGE is pre-configured:
  CREATE STAGE LAKEHOUSE_STAGE
  URL='s3://naren-lakehouse-prod/'
  CREDENTIALS=(AWS_KEY_ID=... AWS_SECRET_KEY=...);
"""

import snowflake.connector  # type: ignore

from config.settings import SNOWFLAKE
from utils.logger import get_logger

logger = get_logger(__name__)


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE.account,
        user=SNOWFLAKE.user,
        password=SNOWFLAKE.password,
        warehouse=SNOWFLAKE.warehouse,
        database=SNOWFLAKE.database,
        schema=SNOWFLAKE.schema,
        role=SNOWFLAKE.role,
    )


def copy_into_table(table: str, stage_path: str, file_format: str = "PARQUET") -> None:
    """
    COPY INTO Snowflake table from an S3 external stage.
    """
    sql = f"""
        COPY INTO {SNOWFLAKE.schema}.{table}
        FROM @LAKEHOUSE_STAGE/{stage_path}
        FILE_FORMAT = (TYPE = '{file_format}')
        ON_ERROR = 'SKIP_FILE'
        PURGE = FALSE;
    """
    logger.info(f"Snowflake COPY INTO {table} ← {stage_path}")
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()
            for row in results:
                logger.info(f"  COPY result: {row}")


def merge_into_table(
    table: str,
    stage_path: str,
    merge_cols: list[str],
    update_cols: list[str],
) -> None:
    """
    MERGE from S3 stage into Snowflake table.
    Used for SCD Type 1 dimension refreshes and incremental fact loads.
    """
    # Build JOIN condition
    join_cond = " AND ".join([f"target.{c} = source.{c}" for c in merge_cols])
    update_set = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
    insert_cols = ", ".join(update_cols + merge_cols)
    insert_vals = ", ".join([f"source.{c}" for c in (update_cols + merge_cols)])

    sql = f"""
        MERGE INTO {SNOWFLAKE.schema}.{table} AS target
        USING (
            SELECT $1 AS data FROM @LAKEHOUSE_STAGE/{stage_path}
            (FILE_FORMAT => 'PARQUET_FORMAT')
        ) AS source
        ON {join_cond}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """
    logger.info(f"Snowflake MERGE INTO {table}")
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            logger.info(f"  Rows affected: {cur.rowcount}")


def run_warehouse_query(sql: str) -> list:
    """Execute an ad-hoc Snowflake query and return results."""
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
