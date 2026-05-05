"""
tests/test_pipeline.py

Unit tests for the core pipeline logic.
Run with: pytest tests/ -v

I've kept the tests focused on the transform layer since that's where bugs
tend to hide. Integration tests for the full E→T→L chain are at the bottom.
"""

import pytest
import pandas as pd
import numpy as np
import sqlite3
import os
import tempfile
from pathlib import Path


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def sample_config():
    """Minimal config that mirrors the real one — avoids file I/O in tests."""
    return {
        "paths": {
            "raw_data": "data/raw/retail_sales_dataset.csv",
            "processed_data": "data/processed/retail_sales_processed.csv",
            "db_path": ":memory:",  # use in-memory SQLite for tests
            "log_dir": "logs",
        },
        "extract": {
            "chunk_size": 50,
            "encoding": "utf-8",
            "date_column": "Date",
        },
        "transform": {
            "expected_columns": [
                "Transaction ID", "Date", "Customer ID", "Gender",
                "Age", "Product Category", "Quantity", "Price per Unit", "Total Amount"
            ],
            "age_bounds": {"min": 10, "max": 100},
            "price_bounds": {"min": 0, "max": 50000},
            "add_revenue_per_unit": True,
            "add_age_group": True,
            "add_day_of_week": True,
            "add_month": True,
            "add_high_value_flag": True,
            "high_value_threshold": 500,
        },
        "load": {
            "db_table": "retail_transactions",
            "if_exists": "replace",
            "chunksize": 100,
        },
        "pipeline": {
            "batch_size": 50,
            "batch_delay_seconds": 0,  # no delay in tests
        },
        "logging": {
            "level": "WARNING",  # less noise during tests
            "log_to_file": False,
            "log_to_console": False,
        },
    }


@pytest.fixture
def raw_df():
    """A small, clean sample of what the raw CSV looks like."""
    return pd.DataFrame({
        "Transaction ID": [1, 2, 3, 4, 5],
        "Date": ["2023-01-15", "2023-03-22", "2023-06-10", "2023-09-05", "2023-11-30"],
        "Customer ID": ["CUST001", "CUST002", "CUST003", "CUST004", "CUST005"],
        "Gender": ["Male", "Female", "Male", "Female", "Male"],
        "Age": [25, 34, 45, 28, 60],
        "Product Category": ["Electronics", "Clothing", "Beauty", "Electronics", "Clothing"],
        "Quantity": [1, 2, 3, 1, 4],
        "Price per Unit": [300, 50, 25, 500, 30],
        "Total Amount": [300, 100, 75, 500, 120],
    })


@pytest.fixture
def dirty_df():
    """A sample with various data quality issues for testing the cleaning logic."""
    return pd.DataFrame({
        "Transaction ID": [1, 2, None, 4, 5],
        "Date": ["2023-01-15", "not-a-date", "2023-06-10", "2023-09-05", None],
        "Customer ID": ["CUST001", None, "CUST003", "CUST004", "CUST005"],
        "Gender": ["Male", "Female", None, "Female", "Male"],
        "Age": [25, 150, 45, -5, 60],   # 150 and -5 are invalid
        "Product Category": ["Electronics", "Clothing", "Beauty", None, "Clothing"],
        "Quantity": [1, 2, -1, 0, 4],   # -1 and 0 are invalid
        "Price per Unit": [300, 50, 25, 500, 30],
        "Total Amount": [300, 100, 75, 500, 120],
    })


# ─────────────────────────────────────────────
# Transform tests
# ─────────────────────────────────────────────

class TestStandardiseColumns:
    def test_lowercase(self, raw_df):
        from src.transform import standardise_columns
        result = standardise_columns(raw_df)
        assert all(c == c.lower() for c in result.columns)

    def test_spaces_replaced(self, raw_df):
        from src.transform import standardise_columns
        result = standardise_columns(raw_df)
        assert "product_category" in result.columns
        assert "price_per_unit" in result.columns
        assert "total_amount" in result.columns

    def test_original_not_mutated(self, raw_df):
        from src.transform import standardise_columns
        original_cols = list(raw_df.columns)
        standardise_columns(raw_df)
        assert list(raw_df.columns) == original_cols


class TestFixDtypes:
    def test_date_parsed(self, raw_df):
        from src.transform import standardise_columns, fix_dtypes
        df = standardise_columns(raw_df)
        df = fix_dtypes(df, "date")
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_bad_dates_become_nat(self):
        from src.transform import fix_dtypes
        df = pd.DataFrame({
            "date": ["2023-01-01", "not-a-date", "2023-06-15"],
            "age": [25, 30, 45],
        })
        result = fix_dtypes(df, "date")
        assert result["date"].isna().sum() == 1


class TestHandleNulls:
    def test_null_transaction_id_rows_dropped(self, dirty_df):
        from src.transform import standardise_columns, handle_nulls
        df = standardise_columns(dirty_df)
        result = handle_nulls(df)
        assert result["transaction_id"].isna().sum() == 0

    def test_null_gender_filled_with_unknown(self, dirty_df):
        from src.transform import standardise_columns, handle_nulls
        df = standardise_columns(dirty_df)
        result = handle_nulls(df)
        assert "Unknown" in result["gender"].values

    def test_null_category_filled(self, dirty_df):
        from src.transform import standardise_columns, handle_nulls
        df = standardise_columns(dirty_df)
        result = handle_nulls(df)
        assert result["product_category"].isna().sum() == 0


class TestRemoveInvalidRecords:
    def test_invalid_age_removed(self, dirty_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, handle_nulls, remove_invalid_records
        df = standardise_columns(dirty_df)
        df = fix_dtypes(df)
        df = handle_nulls(df)
        result = remove_invalid_records(df, sample_config)
        assert (result["age"] > 100).sum() == 0
        assert (result["age"] < 10).sum() == 0

    def test_non_positive_quantity_removed(self, dirty_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, handle_nulls, remove_invalid_records
        df = standardise_columns(dirty_df)
        df = fix_dtypes(df)
        df = handle_nulls(df)
        result = remove_invalid_records(df, sample_config)
        assert (result["quantity"] <= 0).sum() == 0


class TestEngineerFeatures:
    def test_age_group_created(self, raw_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, engineer_features
        df = standardise_columns(raw_df)
        df = fix_dtypes(df)
        result = engineer_features(df, sample_config)
        assert "age_group" in result.columns
        assert result["age_group"].notna().all()

    def test_high_value_flag(self, raw_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, engineer_features
        df = standardise_columns(raw_df)
        df = fix_dtypes(df)
        result = engineer_features(df, sample_config)
        assert "is_high_value" in result.columns
        # row with total_amount=500 should be flagged (threshold=500)
        high_val_rows = result[result["total_amount"] >= 500]
        assert high_val_rows["is_high_value"].all()

    def test_day_of_week_added(self, raw_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, engineer_features
        df = standardise_columns(raw_df)
        df = fix_dtypes(df)
        result = engineer_features(df, sample_config)
        assert "day_of_week" in result.columns

    def test_revenue_per_unit(self, raw_df, sample_config):
        from src.transform import standardise_columns, fix_dtypes, engineer_features
        df = standardise_columns(raw_df)
        df = fix_dtypes(df)
        result = engineer_features(df, sample_config)
        assert "revenue_per_unit" in result.columns
        # spot check: 300 / 1 = 300.0
        assert result.iloc[0]["revenue_per_unit"] == 300.0


class TestFullTransform:
    def test_transform_runs_on_clean_data(self, raw_df, sample_config):
        from src.transform import transform
        result = transform(raw_df, sample_config, batch_id=1)
        assert len(result) > 0
        assert "processed_at" in result.columns
        assert "batch_id" in result.columns

    def test_batch_id_assigned(self, raw_df, sample_config):
        from src.transform import transform
        result = transform(raw_df, sample_config, batch_id=42)
        assert (result["batch_id"] == 42).all()

    def test_output_has_more_columns_than_input(self, raw_df, sample_config):
        from src.transform import transform
        result = transform(raw_df, sample_config, batch_id=1)
        assert len(result.columns) > len(raw_df.columns)


# ─────────────────────────────────────────────
# Load tests
# ─────────────────────────────────────────────

class TestLoadToDb:
    def test_rows_written_to_sqlite(self, raw_df, sample_config, tmp_path):
        from src.transform import transform
        from src.load import load_to_db, get_row_count

        db_path = str(tmp_path / "test.db")
        sample_config["paths"]["db_path"] = db_path
        sample_config["load"]["if_exists"] = "replace"

        df_clean = transform(raw_df, sample_config, batch_id=1)
        rows = load_to_db(df_clean, sample_config)

        assert rows == len(df_clean)
        assert get_row_count(db_path, "retail_transactions") == len(df_clean)

    def test_empty_df_returns_zero(self, sample_config, tmp_path):
        from src.load import load_to_db

        sample_config["paths"]["db_path"] = str(tmp_path / "test.db")
        result = load_to_db(pd.DataFrame(), sample_config)
        assert result == 0


# ─────────────────────────────────────────────
# Extract tests
# ─────────────────────────────────────────────

class TestValidateSchema:
    def test_passes_on_correct_columns(self, raw_df, sample_config):
        from src.extract import validate_schema
        assert validate_schema(raw_df, sample_config) is True

    def test_fails_on_missing_columns(self, sample_config):
        from src.extract import validate_schema
        df = pd.DataFrame({"Transaction ID": [1], "Date": ["2023-01-01"]})
        assert validate_schema(df, sample_config) is False


# ─────────────────────────────────────────────
# Integration test
# ─────────────────────────────────────────────

class TestIntegration:
    def test_full_pipeline_run(self, sample_config, tmp_path):
        """
        End-to-end test using real source data and a temp database.
        If the source CSV exists, run the full pipeline and check we get data out.
        Skip gracefully if the CSV isn't present (CI might not have it).
        """
        src = Path("data/raw/retail_sales_dataset.csv")
        if not src.exists():
            pytest.skip("Source CSV not found — skipping integration test")

        sample_config["paths"]["db_path"] = str(tmp_path / "test.db")
        sample_config["paths"]["processed_data"] = str(tmp_path / "processed.csv")

        from src.pipeline import run_full
        stats = run_full(sample_config)

        assert stats.total_extracted > 0
        assert stats.total_loaded > 0
        assert stats.batches_failed == 0
