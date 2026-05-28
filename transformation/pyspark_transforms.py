"""
Silver → Gold PySpark transformations.

Applies business logic, joins, aggregations, and enrichment to produce
Gold-layer datasets that are warehouse-ready and BI-consumable.

Usage (local):
    python transformation/pyspark_transforms.py --table orders --date 2025-01-15
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import S3
from ingestion.delta_lake_writer import get_spark_with_delta, upsert_delta
from utils.logger import get_logger

logger = get_logger(__name__)


def build_gold_orders(spark: SparkSession, batch_date: str) -> DataFrame:
    """
    Build the Gold orders dataset:
    - Join orders (Silver) with customers and products
    - Calculate order totals, customer lifetime value, running totals
    - Tag first-time vs repeat customers
    """
    orders_path   = S3.silver_path("orders")
    customers_path = S3.silver_path("customers")
    products_path  = S3.silver_path("products")

    orders    = spark.read.format("delta").load(orders_path).filter(F.col("_batch_date") == batch_date)
    customers = spark.read.format("delta").load(customers_path)
    products  = spark.read.format("delta").load(products_path)

    # Enrich orders
    enriched = (
        orders
        .join(customers.select("customer_id", "first_name", "last_name", "country", "is_active"),
              on="customer_id", how="left")
        .join(products.select("product_id", "product_name", "category", "unit_cost"),
              on="product_id", how="left")
    )

    # Calculate margin
    enriched = enriched.withColumn(
        "gross_margin",
        F.col("order_amount") - (F.col("unit_cost") * F.col("quantity"))
    )

    # Running total per customer (partition by customer, order by order_date)
    win = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, 0)
    enriched = enriched.withColumn("customer_running_total", F.sum("order_amount").over(win))

    # Tag repeat purchasers
    order_rank = Window.partitionBy("customer_id").orderBy("order_date")
    enriched = enriched.withColumn("order_rank", F.rank().over(order_rank))
    enriched = enriched.withColumn("is_repeat_customer", F.col("order_rank") > 1)

    # Add Gold metadata
    enriched = (
        enriched
        .withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_batch_date", F.lit(batch_date))
    )

    logger.info(f"Gold orders built: {enriched.count():,} rows")
    return enriched


def build_gold_transactions(spark: SparkSession, batch_date: str) -> DataFrame:
    """
    Build Gold transactions:
    - Join with orders and customers
    - Add fraud risk signals
    - Daily aggregation windows
    """
    txn_path    = S3.silver_path("transactions")
    orders_path = S3.silver_path("orders")

    txns   = spark.read.format("delta").load(txn_path).filter(F.col("_batch_date") == batch_date)
    orders = spark.read.format("delta").load(orders_path)

    enriched = txns.join(
        orders.select("order_id", "customer_id", "order_amount"),
        on="order_id", how="left"
    )

    # Flag high-value transactions
    enriched = enriched.withColumn(
        "risk_tier",
        F.when(F.col("amount") > 10000, "HIGH")
         .when(F.col("amount") > 1000, "MEDIUM")
         .otherwise("LOW")
    )

    # Daily txn count per customer
    daily_win = Window.partitionBy("customer_id", F.to_date("txn_timestamp"))
    enriched = enriched.withColumn("daily_txn_count", F.count("txn_id").over(daily_win))

    enriched = enriched.withColumn("_gold_processed_at", F.current_timestamp())
    logger.info(f"Gold transactions built: {enriched.count():,} rows")
    return enriched


def run_gold_pipeline(table: str, batch_date: str) -> None:
    spark = get_spark_with_delta()

    if table == "orders":
        df = build_gold_orders(spark, batch_date)
        gold_path = S3.gold_path("orders")
        upsert_delta(spark, df, gold_path, merge_keys=["order_id"])

    elif table == "transactions":
        df = build_gold_transactions(spark, batch_date)
        gold_path = S3.gold_path("transactions")
        upsert_delta(spark, df, gold_path, merge_keys=["txn_id"])

    else:
        raise ValueError(f"Unknown table for Gold pipeline: {table}")

    logger.info(f"Gold pipeline complete: {table} → {gold_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold transformation")
    parser.add_argument("--table", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    run_gold_pipeline(table=args.table, batch_date=args.date)
