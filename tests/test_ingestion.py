"""
Unit tests for ingestion layer.
Run with: pytest tests/test_ingestion.py -v
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ingestion.s3_ingestion import add_ingestion_metadata, run_ingestion
from utils.data_quality import assert_not_empty, assert_no_nulls_in_key_columns


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_orders_df():
    return pd.DataFrame({
        "order_id":    ["ORD001", "ORD002", "ORD003"],
        "customer_id": ["CUST01", "CUST02", "CUST01"],
        "product_id":  ["PROD01", "PROD02", "PROD01"],
        "order_date":  ["2025-01-15", "2025-01-15", "2025-01-15"],
        "order_amount": [250.00, 89.99, 500.00],
        "quantity":    [1, 2, 3],
        "status":      ["completed", "completed", "pending"],
    })


@pytest.fixture
def sample_customers_df():
    return pd.DataFrame({
        "customer_id": ["CUST01", "CUST02"],
        "first_name":  ["Alice", "Bob"],
        "last_name":   ["Smith", "Jones"],
        "email":       ["alice@example.com", "bob@example.com"],
        "country":     ["US", "UK"],
        "is_active":   [True, True],
    })


# ── Metadata stamping ─────────────────────────────────────────────────────────

class TestAddIngestionMetadata:
    def test_adds_ingested_at(self, sample_orders_df):
        result = add_ingestion_metadata(sample_orders_df, "orders_2025-01-15")
        assert "_ingested_at" in result.columns

    def test_adds_source_file(self, sample_orders_df):
        result = add_ingestion_metadata(sample_orders_df, "orders_2025-01-15")
        assert all(result["_source_file"] == "orders_2025-01-15")

    def test_does_not_modify_original(self, sample_orders_df):
        original_cols = list(sample_orders_df.columns)
        _ = add_ingestion_metadata(sample_orders_df, "orders_2025-01-15")
        assert list(sample_orders_df.columns) == original_cols


# ── Data quality assertions ───────────────────────────────────────────────────

class TestDataQuality:
    def test_assert_not_empty_passes(self, sample_orders_df):
        assert_not_empty(sample_orders_df, label="orders")  # no exception

    def test_assert_not_empty_fails_on_empty(self):
        with pytest.raises(AssertionError, match="empty"):
            assert_not_empty(pd.DataFrame(), label="empty")

    def test_assert_no_nulls_passes(self, sample_orders_df):
        assert_no_nulls_in_key_columns(sample_orders_df, key_cols=["order_id", "customer_id"])

    def test_assert_no_nulls_fails(self):
        df = pd.DataFrame({"order_id": ["A", None, "C"], "amount": [1, 2, 3]})
        with pytest.raises(AssertionError, match="null"):
            assert_no_nulls_in_key_columns(df, key_cols=["order_id"])


# ── S3 ingestion (mocked) ─────────────────────────────────────────────────────

class TestRunIngestion:
    @patch("ingestion.s3_ingestion.write_to_bronze")
    @patch("ingestion.s3_ingestion.read_source_file")
    def test_run_ingestion_calls_write(self, mock_read, mock_write, sample_orders_df):
        mock_read.return_value = sample_orders_df
        mock_write.return_value = "s3://bucket/bronze/orders/dt=2025-01-15/data.parquet"

        run_ingestion(source="orders", date="2025-01-15")

        mock_write.assert_called_once()

    @patch("ingestion.s3_ingestion.write_to_bronze")
    @patch("ingestion.s3_ingestion.read_source_file")
    def test_run_ingestion_fails_on_empty(self, mock_read, mock_write):
        mock_read.return_value = pd.DataFrame()

        with pytest.raises(AssertionError):
            run_ingestion(source="orders", date="2025-01-15")

        mock_write.assert_not_called()
