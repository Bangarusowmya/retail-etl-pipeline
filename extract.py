"""
extract.py — handles reading raw data from CSV.

Supports two modes:
  - full load (read everything at once)
  - chunked / streaming (yields batches to simulate a real-time feed)

In production this would be reading from S3, a Kafka topic, or a database CDC stream.
For now it's CSV chunks with a configurable delay — good enough to demo the concept.
"""

import time
import pandas as pd
from pathlib import Path
from typing import Generator, Optional

from src.utils import get_logger, load_config


logger = get_logger(__name__)


def load_full(filepath: str, config: dict) -> pd.DataFrame:
    """
    Read the entire CSV in one shot. Used for the initial validation pass
    and for smaller datasets where streaming isn't necessary.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {filepath}")

    encoding = config.get("extract", {}).get("encoding", "utf-8")

    logger.info(f"Loading full dataset from: {filepath}")
    df = pd.read_csv(path, encoding=encoding)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")

    return df


def stream_batches(
    filepath: str,
    config: dict,
    delay: Optional[float] = None,
) -> Generator[pd.DataFrame, None, None]:
    """
    Yield chunks of the CSV one at a time, with a small sleep between each.
    This simulates a streaming data source — think Kafka consumer, CDC feed, etc.

    Args:
        filepath: path to the raw CSV
        config:   pipeline config dict
        delay:    override the batch delay from config (useful in tests)

    Yields:
        pd.DataFrame chunks
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {filepath}")

    chunk_size = config.get("extract", {}).get("chunk_size", 100)
    batch_delay = delay if delay is not None else config.get("pipeline", {}).get(
        "batch_delay_seconds", 2
    )
    encoding = config.get("extract", {}).get("encoding", "utf-8")

    logger.info(f"Starting streaming extraction from: {filepath}")
    logger.info(f"Batch size: {chunk_size} rows | Delay: {batch_delay}s between batches")

    batch_num = 0
    total_rows = 0

    for chunk in pd.read_csv(path, chunksize=chunk_size, encoding=encoding):
        batch_num += 1
        total_rows += len(chunk)
        logger.info(f"Batch {batch_num}: extracted {len(chunk)} rows (running total: {total_rows:,})")

        yield chunk

        # simulate the delay between messages arriving from a real stream
        if batch_delay > 0:
            time.sleep(batch_delay)

    logger.info(f"Extraction complete — {batch_num} batches, {total_rows:,} total rows")


def validate_schema(df: pd.DataFrame, config: dict) -> bool:
    """
    Quick sanity check: make sure the expected columns are all there.
    Better to catch this early than get a confusing KeyError mid-transform.
    """
    expected = config.get("transform", {}).get("expected_columns", [])
    if not expected:
        logger.warning("No expected columns defined in config — skipping schema validation")
        return True

    missing = [col for col in expected if col not in df.columns]
    if missing:
        logger.error(f"Schema validation failed. Missing columns: {missing}")
        return False

    logger.info("Schema validation passed ✓")
    return True


def get_file_stats(filepath: str) -> dict:
    """
    Return basic file metadata — handy for logging/monitoring at pipeline start.
    """
    path = Path(filepath)
    if not path.exists():
        return {}

    stat = path.stat()
    return {
        "filename": path.name,
        "size_kb": round(stat.st_size / 1024, 2),
        "last_modified": stat.st_mtime,
    }
