"""
Apache Airflow DAG — Full Lakehouse Pipeline

Schedule: daily at 03:00 UTC
Pipeline:
  1. Ingest raw files → S3 Bronze (customers, orders, transactions)
  2. Glue jobs: Bronze → Silver (parallel per table)
  3. PySpark Gold transformations (orders, transactions)
  4. Redshift loads (fact_orders, fact_transactions)
  5. dbt run (staging → marts)
  6. Reconciliation checks

Failure behavior: Slack alert + email to nproddut18@gmail.com
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# ── Default args ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "naren_prodduturi",
    "depends_on_past":  False,
    "email":            ["nproddut18@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

S3_BUCKET   = Variable.get("s3_bucket", default_var="naren-lakehouse-prod")
GLUE_ROLE   = Variable.get("glue_iam_role", default_var="arn:aws:iam::123456789:role/GlueRole")
ENV         = Variable.get("pipeline_env", default_var="prod")
TABLES      = ["customers", "orders", "transactions"]

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="lakehouse_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Lakehouse pipeline: S3 Bronze → Silver → Gold → Redshift → dbt",
    schedule_interval="0 3 * * *",      # daily at 03:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "etl", "redshift", "dbt"],
) as dag:

    # ── Step 0: S3 sensor — wait for upstream drop ─────────────────────────────
    with TaskGroup("s3_sensors") as s3_sensors:
        for table in TABLES:
            S3KeySensor(
                task_id=f"wait_for_{table}",
                bucket_name=S3_BUCKET,
                bucket_key=f"landing/{table}/dt={{{{ ds }}}}/",
                wildcard_match=True,
                aws_conn_id="aws_default",
                timeout=3600,
                poke_interval=120,
            )

    # ── Step 1: S3 ingestion (Bronze) ──────────────────────────────────────────
    with TaskGroup("bronze_ingestion") as bronze_group:
        for table in TABLES:
            PythonOperator(
                task_id=f"ingest_{table}",
                python_callable=lambda t=table, **ctx: __import__(
                    "ingestion.s3_ingestion", fromlist=["run_ingestion"]
                ).run_ingestion(source=t, date=ctx["ds"]),
            )

    # ── Step 2: Glue jobs — Bronze → Silver (parallel) ─────────────────────────
    with TaskGroup("glue_silver") as glue_group:
        for table in TABLES:
            GlueJobOperator(
                task_id=f"glue_{table}_silver",
                job_name=f"lakehouse-bronze-to-silver-{table}",
                script_location=f"s3://{S3_BUCKET}/glue-scripts/glue_job.py",
                iam_role_name=GLUE_ROLE,
                script_args={
                    "--source_table": table,
                    "--batch_date":   "{{ ds }}",
                    "--s3_bucket":    S3_BUCKET,
                    "--environment":  ENV,
                },
                aws_conn_id="aws_default",
                region_name="us-east-1",
            )

    # ── Step 3: PySpark Gold transformations ───────────────────────────────────
    with TaskGroup("gold_transforms") as gold_group:
        for table in ["orders", "transactions"]:
            BashOperator(
                task_id=f"gold_{table}",
                bash_command=(
                    f"python /opt/pipeline/transformation/pyspark_transforms.py "
                    f"--table {table} --date {{{{ ds }}}}"
                ),
            )

    # ── Step 4: Redshift loads ────────────────────────────────────────────────
    with TaskGroup("redshift_loads") as redshift_group:
        for table in ["fact_orders", "fact_transactions"]:
            PythonOperator(
                task_id=f"load_{table}",
                python_callable=lambda t=table, **ctx: __import__(
                    "warehouse.redshift_loader", fromlist=["load_table"]
                ).load_table(
                    table=t,
                    s3_path=f"s3://{S3_BUCKET}/gold/{t.replace('fact_', '')}/dt={ctx['ds']}/",
                    mode="incremental",
                    merge_key=t.replace("fact_", "").rstrip("s") + "_id",
                ),
            )

    # ── Step 5: dbt run ────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/pipeline/dbt && dbt run --models marts --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/pipeline/dbt && dbt test --models marts --target prod",
    )

    # ── Step 6: Reconciliation ─────────────────────────────────────────────────
    reconciliation = PythonOperator(
        task_id="reconciliation",
        python_callable=lambda **ctx: __import__(
            "utils.reconciliation", fromlist=["full_reconciliation_report"]
        ).full_reconciliation_report(
            source_counts={"fact_orders": 0, "fact_transactions": 0},  # populated at runtime
            batch_date=ctx["ds"],
        ),
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    s3_sensors >> bronze_group >> glue_group >> gold_group >> redshift_group >> dbt_run >> dbt_test >> reconciliation
