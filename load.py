"""
load.py — writes processed data to SQLite and optionally to CSV.

Keeps it simple: one table in SQLite, append by default.
If you're migrating to Postgres later, the SQLAlchemy engine swap is the only thing
that changes — the rest of this file stays the same.
"""

import os
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional

from src.utils import get_logger


logger = get_logger(__name__)


# ─────────────────────────────────────────────
# SQLite helpers
# ─────────────────────────────────────────────

def get_db_connection(db_path: str) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database. The WAL journal mode helps
    with concurrent reads while a write is in progress.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_schema(conn: sqlite3.Connection, table_name: str):
    """
    Create the transactions table if it doesn't exist yet.
    Nothing fancy — just making sure the schema is in place before we start loading.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            transaction_id    INTEGER,
            date              TEXT,
            customer_id       TEXT,
            gender            TEXT,
            age               REAL,
            product_category  TEXT,
            quantity          REAL,
            price_per_unit    REAL,
            total_amount      REAL,
            revenue_per_unit  REAL,
            age_group         TEXT,
            day_of_week       TEXT,
            month             REAL,
            month_name        TEXT,
            year              REAL,
            quarter           TEXT,
            is_high_value     INTEGER,
            processed_at      TEXT,
            batch_id          INTEGER,
            amount_mismatch_flag INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    logger.debug(f"Schema initialised for table '{table_name}'")


def load_to_db(df: pd.DataFrame, config: dict) -> int:
    """
    Append the processed dataframe to the SQLite table.

    Returns the number of rows written so the caller can log/report it.
    """
    db_path = config.get("paths", {}).get("db_path", "data/retail_pipeline.db")
    table = config.get("load", {}).get("db_table", "retail_transactions")
    if_exists = config.get("load", {}).get("if_exists", "append")
    chunk_size = config.get("load", {}).get("chunksize", 500)

    if df.empty:
        logger.warning("load_to_db called with an empty dataframe — nothing to write")
        return 0

    logger.info(f"Writing {len(df):,} rows to '{table}' in {db_path}")

    try:
        conn = get_db_connection(db_path)
        init_schema(conn, table)

        # pandas to_sql is convenient but doesn't give per-chunk progress —
        # so we do it manually for large batches
        rows_written = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i: i + chunk_size]
            chunk.to_sql(
                name=table,
                con=conn,
                if_exists=if_exists if i == 0 else "append",
                index=False,
                method="multi",
            )
            rows_written += len(chunk)

        conn.close()
        logger.info(f"DB write complete — {rows_written:,} rows inserted")
        return rows_written

    except Exception as e:
        logger.error(f"Database write failed: {e}", exc_info=True)
        raise


# ─────────────────────────────────────────────
# CSV output
# ─────────────────────────────────────────────

def save_to_csv(df: pd.DataFrame, output_path: str, mode: str = "a") -> None:
    """
    Save processed data to CSV. Appends by default so streaming batches
    accumulate in the same file across a run.

    First batch writes the header; subsequent batches skip it.
    """
    if df.empty:
        logger.warning("save_to_csv: dataframe is empty, skipping")
        return

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # write header only on the first write (when file doesn't exist yet)
    write_header = not Path(output_path).exists() or mode == "w"

    df.to_csv(output_path, mode=mode, header=write_header, index=False)
    logger.info(f"Saved {len(df):,} rows to {output_path} (header={'yes' if write_header else 'no'})")


# ─────────────────────────────────────────────
# Query helpers (for the dashboard / reporting)
# ─────────────────────────────────────────────

def query_db(sql: str, db_path: str) -> pd.DataFrame:
    """
    Run an arbitrary SELECT against the SQLite DB and return a dataframe.
    Read-only — use this for reporting, not for mutations.
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}. Run the pipeline first.")

    conn = sqlite3.connect(db_path)
    try:
        result = pd.read_sql_query(sql, conn)
    finally:
        conn.close()
    return result


def get_row_count(db_path: str, table: str = "retail_transactions") -> int:
    """Quick row count — useful for health checks after a load."""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0
