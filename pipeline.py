"""
pipeline.py — wires up Extract → Transform → Load.

Two modes:
  - run_streaming: processes the file in chunks, simulating a real-time feed
  - run_full:      reads everything at once (useful for backfills / one-off runs)

The pipeline tracks run stats (rows in/out, batches, timing) and logs a
summary at the end. If a batch fails partway through, we log the error and
move on rather than killing the whole run — losing a batch is better than
losing everything.
"""

import time
from datetime import datetime
from typing import Optional

from src.extract import load_full, stream_batches, validate_schema, get_file_stats
from src.transform import transform
from src.load import load_to_db, save_to_csv, get_row_count
from src.utils import get_logger, pretty_separator


logger = get_logger(__name__)


class PipelineStats:
    """
    Lightweight stats tracker. Not using dataclasses to keep Python 3.6 compat,
    but honestly could just be a dict — I like the dot-access though.
    """
    def __init__(self):
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.total_extracted: int = 0
        self.total_transformed: int = 0
        self.total_loaded: int = 0
        self.batches_processed: int = 0
        self.batches_failed: int = 0
        self.errors: list = []

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return round(self.end_time - self.start_time, 2)
        return 0.0

    @property
    def success_rate(self) -> float:
        total = self.batches_processed + self.batches_failed
        if total == 0:
            return 0.0
        return round(self.batches_processed / total * 100, 1)

    def summary(self) -> str:
        return (
            f"  Batches processed : {self.batches_processed}\n"
            f"  Batches failed    : {self.batches_failed}\n"
            f"  Rows extracted    : {self.total_extracted:,}\n"
            f"  Rows transformed  : {self.total_transformed:,}\n"
            f"  Rows loaded       : {self.total_loaded:,}\n"
            f"  Duration          : {self.duration_seconds}s\n"
            f"  Success rate      : {self.success_rate}%"
        )


# ─────────────────────────────────────────────
# Streaming pipeline (main mode)
# ─────────────────────────────────────────────

def run_streaming(config: dict, delay_override: Optional[float] = None) -> PipelineStats:
    """
    Process the source file in batches with a simulated delay between each.
    This is the primary mode — meant to demonstrate a streaming ETL pattern.

    Args:
        config:         loaded config dict
        delay_override: override batch delay (handy for tests)
    """
    stats = PipelineStats()
    stats.start_time = time.time()

    raw_path = config["paths"]["raw_data"]
    processed_path = config["paths"]["processed_data"]

    logger.info(pretty_separator("="))
    logger.info("RETAIL ETL PIPELINE — STREAMING MODE")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(pretty_separator("="))

    # print some file metadata upfront
    file_info = get_file_stats(raw_path)
    if file_info:
        logger.info(f"Source file: {file_info['filename']} ({file_info['size_kb']} KB)")

    # clear the output CSV at the start of a new run
    import os
    if os.path.exists(processed_path):
        os.remove(processed_path)
        logger.info("Cleared previous processed output file")

    # schema check before we start streaming
    try:
        import pandas as pd
        sample = pd.read_csv(raw_path, nrows=5)
        if not validate_schema(sample, config):
            raise ValueError("Schema validation failed — stopping pipeline")
    except Exception as e:
        logger.error(f"Pre-flight check failed: {e}")
        stats.errors.append(str(e))
        stats.end_time = time.time()
        return stats

    # main streaming loop
    batch_id = 0
    for raw_chunk in stream_batches(raw_path, config, delay=delay_override):
        batch_id += 1
        logger.info(pretty_separator())
        logger.info(f"Processing batch {batch_id} ({len(raw_chunk)} rows)...")

        stats.total_extracted += len(raw_chunk)

        try:
            # transform
            transformed = transform(raw_chunk, config, batch_id=batch_id)
            stats.total_transformed += len(transformed)

            # load to db
            rows_loaded = load_to_db(transformed, config)
            stats.total_loaded += rows_loaded

            # append to processed CSV
            save_to_csv(transformed, processed_path, mode="a")

            stats.batches_processed += 1
            logger.info(f"Batch {batch_id} complete ✓")

        except Exception as e:
            stats.batches_failed += 1
            stats.errors.append(f"Batch {batch_id}: {str(e)}")
            logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
            logger.warning("Skipping failed batch and continuing...")
            continue

    # final summary
    stats.end_time = time.time()
    db_path = config["paths"]["db_path"]
    db_count = get_row_count(db_path, config["load"]["db_table"])

    logger.info(pretty_separator("="))
    logger.info("PIPELINE COMPLETE")
    logger.info(stats.summary())
    logger.info(f"  DB row count      : {db_count:,}")
    logger.info(pretty_separator("="))

    return stats


# ─────────────────────────────────────────────
# Full-load pipeline (backfill / one-shot mode)
# ─────────────────────────────────────────────

def run_full(config: dict) -> PipelineStats:
    """
    Load and process the entire file at once.
    Faster than streaming for small datasets or one-off backfills.
    """
    stats = PipelineStats()
    stats.start_time = time.time()

    raw_path = config["paths"]["raw_data"]
    processed_path = config["paths"]["processed_data"]

    logger.info(pretty_separator("="))
    logger.info("RETAIL ETL PIPELINE — FULL LOAD MODE")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(pretty_separator("="))

    try:
        # extract
        df = load_full(raw_path, config)
        stats.total_extracted = len(df)

        if not validate_schema(df, config):
            raise ValueError("Schema validation failed")

        # transform
        df_clean = transform(df, config, batch_id=1)
        stats.total_transformed = len(df_clean)
        stats.batches_processed = 1

        # load
        rows_loaded = load_to_db(df_clean, config)
        stats.total_loaded = rows_loaded
        save_to_csv(df_clean, processed_path, mode="w")

    except Exception as e:
        stats.batches_failed = 1
        stats.errors.append(str(e))
        logger.error(f"Full-load pipeline failed: {e}", exc_info=True)
        raise

    stats.end_time = time.time()
    logger.info(pretty_separator("="))
    logger.info("PIPELINE COMPLETE")
    logger.info(stats.summary())
    logger.info(pretty_separator("="))

    return stats
