# Cloud Lakehouse & Data Warehouse Modernization Platform

**End-to-end lakehouse architecture on AWS S3, Glue, Delta Lake, Apache Airflow, Amazon Redshift, and Snowflake — with dbt modeling, SCD Type 2 logic, and incremental load optimization.**

---

## Architecture Overview

```
Raw Sources → S3 (Bronze) → Glue + PySpark (Silver) → Delta Lake (Gold) → Redshift / Snowflake → BI Dashboards
```

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) for an operational data warehouse modernization use case, covering:

- **Ingestion Layer** — AWS Glue jobs ingest raw CSV/JSON/Parquet files from S3 into the Bronze zone
- **Transformation Layer** — PySpark and Delta Lake pipelines cleanse, deduplicate, and enrich data into Silver and Gold zones
- **Warehouse Layer** — Redshift (fact/dimension tables, star schema) and Snowflake (data marts) are loaded via optimized COPY and INSERT statements
- **Modeling Layer** — dbt models handle staging, intermediate transformations, and mart-level SCD Type 2 logic
- **Orchestration** — Apache Airflow DAGs coordinate the full pipeline with retry logic, alerting, and dependency management
- **Data Quality** — Schema validation, reconciliation checks, and row-count assertions at each layer boundary

---

## Repository Structure

```
cloud-lakehouse-dw-modernization/
├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   ├── __init__.py
│   ├── settings.py          # Global config (S3 paths, DB connections, env vars)
│   └── aws_config.py        # AWS Glue / S3 / Redshift connection helpers
├── ingestion/
│   ├── __init__.py
│   ├── s3_ingestion.py      # S3 → Bronze zone raw file loader
│   ├── glue_job.py          # AWS Glue PySpark job: Bronze → Silver cleansing
│   └── delta_lake_writer.py # Delta Lake writer utilities (upserts, merges)
├── transformation/
│   ├── __init__.py
│   ├── pyspark_transforms.py  # Silver → Gold PySpark transformation logic
│   └── delta_lake_utils.py    # Delta Lake compaction, vacuuming, schema evolution
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_transactions.sql
│   │   ├── intermediate/
│   │   │   └── int_customer_orders.sql
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── fact_orders.sql
│   │       └── fact_transactions.sql
│   └── macros/
│       └── scd_type2.sql
├── warehouse/
│   ├── redshift_loader.py     # Redshift COPY + INSERT loader
│   ├── snowflake_loader.py    # Snowflake bulk loader with MERGE logic
│   └── schema_validation.py  # Source-to-target schema diff and row count checks
├── utils/
│   ├── logger.py              # Centralized structured logger
│   ├── data_quality.py        # Great Expectations-style assertion helpers
│   └── reconciliation.py     # Source vs target reconciliation reports
├── airflow/
│   └── dags/
│       └── lakehouse_pipeline_dag.py  # Full pipeline DAG definition
└── tests/
    ├── test_ingestion.py
    └── test_transformations.py
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Storage** | AWS S3 (Bronze/Silver/Gold zones), Delta Lake |
| **Ingestion** | AWS Glue (PySpark), AWS Lambda, REST API connectors |
| **Transformation** | Apache Spark, PySpark, Spark SQL, Delta Lake |
| **Orchestration** | Apache Airflow 2.x |
| **Warehouse** | Amazon Redshift, Snowflake |
| **Modeling** | dbt (data build tool), Star Schema, SCD Type 2 |
| **Data Quality** | Schema validation, reconciliation checks, row-count assertions |
| **DevOps** | Git, AWS CloudFormation, IAM, CI/CD |

---

## Setup

### Prerequisites

```bash
pip install -r requirements.txt
```

Set the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
export REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
export REDSHIFT_PASSWORD=your_password
export SNOWFLAKE_PASSWORD=your_password
```

### Running the Pipeline

**Trigger individual Glue ingestion:**
```bash
python ingestion/s3_ingestion.py --source orders --date 2025-01-15
```

**Run transformations locally:**
```bash
python transformation/pyspark_transforms.py --layer silver --table orders
```

**Load to Redshift:**
```bash
python warehouse/redshift_loader.py --table fact_orders --mode incremental
```

**Run dbt models:**
```bash
cd dbt/
dbt run --models marts
dbt test
```

**Trigger Airflow DAG:**
```bash
airflow dags trigger lakehouse_full_pipeline --conf '{"execution_date": "2025-01-15"}'
```

---

## Data Model

### Star Schema (Redshift / Snowflake)

```
                    ┌──────────────────┐
                    │  fact_orders     │
                    │  ─────────────── │
                    │  order_sk (PK)   │
                    │  customer_sk (FK)│──────► dim_customers
                    │  product_sk (FK) │──────► dim_products
                    │  date_sk (FK)    │──────► dim_date
                    │  order_amount    │
                    │  quantity        │
                    └──────────────────┘
```

### SCD Type 2 — dim_customers

Tracks full customer history. New rows are inserted with updated `effective_start_date`, `effective_end_date`, and `is_current` flag when attributes change.

---

## Performance Optimizations

- **Redshift**: sort keys on date columns, distribution keys on join columns, compressed encodings
- **Snowflake**: clustering keys, result cache, query pruning via partitioned stages
- **Delta Lake**: Z-ordering on high-cardinality columns, OPTIMIZE compaction, VACUUM on stale files
- **Airflow**: parallel task groups, sensor-based triggering to avoid polling overhead
- **Incremental loads**: `MERGE` / `UPSERT` logic to avoid full-refresh scans

---

## Author

**Naren Prodduturi** — Data Engineer | MS Computer Science, UNC Charlotte
📧 nproddut18@gmail.com | [GitHub](https://github.com/narenp18)
