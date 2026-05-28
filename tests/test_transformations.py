"""
Unit tests for PySpark Gold transformation logic.
Run with: pytest tests/test_transformations.py -v

Requires: pyspark, delta-spark installed in test environment.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType


@pytest.fixture(scope="session")
def spark():
    """Create a minimal local SparkSession for testing."""
    return (
        SparkSession.builder
        .appName("TestLakehousePipeline")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def orders_schema():
    return StructType([
        StructField("order_id",       StringType(),  False),
        StructField("customer_id",    StringType(),  False),
        StructField("product_id",     StringType(),  True),
        StructField("order_date",     DateType(),    True),
        StructField("order_amount",   DoubleType(),  True),
        StructField("quantity",       IntegerType(), True),
        StructField("status",         StringType(),  True),
    ])


@pytest.fixture
def sample_orders_spark(spark, orders_schema):
    data = [
        ("ORD001", "CUST01", "PROD01", "2025-01-15", 250.00, 1, "completed"),
        ("ORD002", "CUST02", "PROD02", "2025-01-15", 89.99,  2, "completed"),
        ("ORD003", "CUST01", "PROD01", "2025-01-14", 500.00, 1, "completed"),
    ]
    return spark.createDataFrame(data, schema=[
        "order_id", "customer_id", "product_id", "order_date",
        "order_amount", "quantity", "status"
    ])


class TestSparkTransformations:
    def test_row_count_preserved(self, sample_orders_spark):
        assert sample_orders_spark.count() == 3

    def test_dedup_removes_duplicates(self, spark, sample_orders_spark):
        # Add a duplicate row
        dup = sample_orders_spark.union(sample_orders_spark.limit(1))
        deduped = dup.dropDuplicates()
        assert deduped.count() == 3

    def test_running_total_increases_monotonically(self, spark, sample_orders_spark):
        from pyspark.sql.window import Window
        win = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(
            Window.unboundedPreceding, 0
        )
        result = sample_orders_spark.withColumn("running_total", F.sum("order_amount").over(win))

        # CUST01 has 2 orders — running total on second should be >= first
        cust01 = (
            result
            .filter(F.col("customer_id") == "CUST01")
            .orderBy("order_date")
            .select("running_total")
            .collect()
        )
        totals = [row["running_total"] for row in cust01]
        for i in range(1, len(totals)):
            assert totals[i] >= totals[i - 1], "Running total should not decrease"

    def test_risk_tier_classification(self, spark):
        data = [("T1", 500.0), ("T2", 5000.0), ("T3", 15000.0)]
        df = spark.createDataFrame(data, ["txn_id", "amount"])
        df = df.withColumn(
            "risk_tier",
            F.when(F.col("amount") > 10000, "HIGH")
             .when(F.col("amount") > 1000,  "MEDIUM")
             .otherwise("LOW")
        )
        tiers = {row["txn_id"]: row["risk_tier"] for row in df.collect()}
        assert tiers["T1"] == "LOW"
        assert tiers["T2"] == "MEDIUM"
        assert tiers["T3"] == "HIGH"

    def test_metadata_columns_added(self, spark, sample_orders_spark):
        result = sample_orders_spark.withColumn("_gold_processed_at", F.current_timestamp())
        assert "_gold_processed_at" in result.columns
