"""
transform.py — all the data cleaning and feature engineering lives here.

The general idea: take messy raw data, make it trustworthy and analysis-ready.
Each step is its own function so they can be tested and reused independently.

Order of operations:
  1. Rename / standardise columns
  2. Fix types
  3. Handle nulls / bad values
  4. Validate business rules
  5. Feature engineering
  6. Add pipeline metadata (timestamps etc.)
"""

import pandas as pd
import numpy as np
from datetime import datetime

from src.utils import get_logger


logger = get_logger(__name__)


# ─────────────────────────────────────────────
# Step 1 — column standardisation
# ─────────────────────────────────────────────

def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase + snake_case column names so downstream code doesn't have to
    worry about 'Product Category' vs 'product_category' vs 'ProductCategory'.
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "_", regex=True)
    )
    logger.debug(f"Columns after standardisation: {list(df.columns)}")
    return df


# ─────────────────────────────────────────────
# Step 2 — type coercion
# ─────────────────────────────────────────────

def fix_dtypes(df: pd.DataFrame, date_column: str = "date") -> pd.DataFrame:
    """
    Parse dates and make sure numeric columns are actually numeric.
    Errors here are coerced to NaN so we can handle them in the null step.
    """
    df = df.copy()

    # parse the date column
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        bad_dates = df[date_column].isna().sum()
        if bad_dates > 0:
            logger.warning(f"{bad_dates} unparseable date values — will be dropped")

    # numeric columns
    for col in ["age", "quantity", "price_per_unit", "total_amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ─────────────────────────────────────────────
# Step 3 — handle nulls
# ─────────────────────────────────────────────

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deal with missing values. Strategy varies by column:
      - Critical identifiers → drop the row, we can't do anything useful without them
      - Numeric fields → fill with median (less sensitive to outliers than mean)
      - Categorical → fill with 'Unknown'

    Logs a summary so you know what got cleaned.
    """
    df = df.copy()
    initial_len = len(df)

    # rows with no transaction id or date are useless — drop them
    critical_cols = ["transaction_id", "date"]
    for col in critical_cols:
        if col in df.columns:
            before = len(df)
            df = df.dropna(subset=[col])
            dropped = before - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows with null '{col}'")

    # numeric fills
    for col in ["age", "quantity", "price_per_unit", "total_amount"]:
        if col in df.columns and df[col].isna().any():
            median_val = df[col].median()
            null_count = df[col].isna().sum()
            df[col] = df[col].fillna(median_val)
            logger.info(f"Filled {null_count} nulls in '{col}' with median ({median_val:.1f})")

    # categorical fills
    for col in ["gender", "product_category", "customer_id"]:
        if col in df.columns and df[col].isna().any():
            null_count = df[col].isna().sum()
            df[col] = df[col].fillna("Unknown")
            logger.info(f"Filled {null_count} nulls in '{col}' with 'Unknown'")

    final_len = len(df)
    if initial_len != final_len:
        logger.info(f"Null handling: {initial_len - final_len} rows removed")

    return df


# ─────────────────────────────────────────────
# Step 4 — outlier / sanity checks
# ─────────────────────────────────────────────

def remove_invalid_records(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Drop rows that violate basic business rules.
    These aren't statistical outliers — they're just wrong data.
    """
    df = df.copy()
    initial_len = len(df)

    age_cfg = config.get("transform", {}).get("age_bounds", {"min": 10, "max": 100})
    price_cfg = config.get("transform", {}).get("price_bounds", {"min": 0, "max": 50000})

    # age must be a plausible human age
    if "age" in df.columns:
        mask = df["age"].between(age_cfg["min"], age_cfg["max"])
        bad = (~mask).sum()
        if bad > 0:
            logger.warning(f"Removing {bad} rows with age outside [{age_cfg['min']}, {age_cfg['max']}]")
        df = df[mask]

    # negative prices / quantities make no sense
    if "price_per_unit" in df.columns:
        mask = df["price_per_unit"].between(price_cfg["min"], price_cfg["max"])
        bad = (~mask).sum()
        if bad > 0:
            logger.warning(f"Removing {bad} rows with invalid price_per_unit")
        df = df[mask]

    if "quantity" in df.columns:
        mask = df["quantity"] > 0
        bad = (~mask).sum()
        if bad > 0:
            logger.warning(f"Removing {bad} rows with quantity <= 0")
        df = df[mask]

    # recalculate total_amount and flag big discrepancies
    if all(c in df.columns for c in ["quantity", "price_per_unit", "total_amount"]):
        expected = df["quantity"] * df["price_per_unit"]
        mismatch = (df["total_amount"] != expected).sum()
        if mismatch > 0:
            logger.warning(
                f"{mismatch} rows have total_amount != quantity * price_per_unit — "
                "keeping original values but flagging"
            )
            df["amount_mismatch_flag"] = df["total_amount"] != expected

    rows_removed = initial_len - len(df)
    logger.info(f"Validation removed {rows_removed} invalid records")
    return df


# ─────────────────────────────────────────────
# Step 5 — feature engineering
# ─────────────────────────────────────────────

def engineer_features(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Add derived columns that'll be useful for analysis / ML downstream.
    Each feature is gated by a config flag so it's easy to turn off.
    """
    df = df.copy()
    transform_cfg = config.get("transform", {})

    # revenue per unit (sanity check column + useful metric)
    if transform_cfg.get("add_revenue_per_unit", True):
        if all(c in df.columns for c in ["total_amount", "quantity"]):
            df["revenue_per_unit"] = (df["total_amount"] / df["quantity"]).round(2)

    # age brackets — useful for demographic segmentation
    if transform_cfg.get("add_age_group", True) and "age" in df.columns:
        bins = [0, 25, 35, 45, 55, 65, 100]
        labels = ["18-25", "26-35", "36-45", "46-55", "56-65", "65+"]
        df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=True)
        df["age_group"] = df["age_group"].astype(str)

    # temporal features from the date column
    if "date" in df.columns:
        if transform_cfg.get("add_day_of_week", True):
            df["day_of_week"] = df["date"].dt.day_name()

        if transform_cfg.get("add_month", True):
            df["month"] = df["date"].dt.month
            df["month_name"] = df["date"].dt.strftime("%B")
            df["year"] = df["date"].dt.year

        # quarter — handy for business reporting
        df["quarter"] = df["date"].dt.quarter.apply(lambda q: f"Q{q}")

    # high-value order flag
    if transform_cfg.get("add_high_value_flag", True) and "total_amount" in df.columns:
        threshold = transform_cfg.get("high_value_threshold", 500)
        df["is_high_value"] = df["total_amount"] >= threshold

    logger.info(f"Feature engineering done — {len(df.columns)} total columns")
    return df


# ─────────────────────────────────────────────
# Step 6 — pipeline metadata
# ─────────────────────────────────────────────

def add_pipeline_metadata(df: pd.DataFrame, batch_id: int = 0) -> pd.DataFrame:
    """
    Stamp each row with pipeline metadata so we know when/how it was processed.
    In a real system you'd also tag the source system, environment, run ID, etc.
    """
    df = df.copy()
    df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["batch_id"] = batch_id
    return df


# ─────────────────────────────────────────────
# Main transform entry point
# ─────────────────────────────────────────────

def transform(df: pd.DataFrame, config: dict, batch_id: int = 0) -> pd.DataFrame:
    """
    Run the full transformation chain on a dataframe (or a chunk of one).

    Returns the cleaned, feature-enriched dataframe ready for loading.
    Raises on critical failures so the pipeline can decide what to do.
    """
    logger.info(f"Starting transform on {len(df):,} rows (batch_id={batch_id})")
    input_rows = len(df)

    try:
        df = standardise_columns(df)
        df = fix_dtypes(df, date_column="date")
        df = handle_nulls(df)
        df = remove_invalid_records(df, config)
        df = engineer_features(df, config)
        df = add_pipeline_metadata(df, batch_id=batch_id)
    except Exception as e:
        logger.error(f"Transform failed on batch {batch_id}: {e}", exc_info=True)
        raise

    output_rows = len(df)
    drop_rate = ((input_rows - output_rows) / input_rows * 100) if input_rows > 0 else 0
    logger.info(
        f"Transform complete: {input_rows} → {output_rows} rows "
        f"({drop_rate:.1f}% drop rate)"
    )
    return df
