# Retail Sales ETL Pipeline

A production-style real-time data pipeline for processing retail transaction data. Built with Python, Pandas, and SQLite — designed to simulate a streaming ETL system using batch processing with configurable delays.

This is a portfolio project that demonstrates end-to-end pipeline engineering: from raw CSV ingestion through multi-step transformation to a queryable SQLite database with a lightweight analytics layer on top.

---

## What this does

Takes a raw retail sales CSV and runs it through a proper ETL pipeline:

1. **Extract** — reads the source file in configurable chunks, simulating a streaming data feed (think Kafka consumer or CDC events)
2. **Transform** — cleans the data, enforces business rules, engineers useful features (age groups, revenue metrics, temporal features, high-value order flags)
3. **Load** — writes processed rows to a SQLite database and a clean CSV, batch by batch
4. **Report** — optional analytics dashboard using SQL queries on the loaded data

---

## Architecture

```
                        ┌─────────────────────────────────────┐
                        │         RETAIL ETL PIPELINE         │
                        └─────────────────────────────────────┘

 ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
 │              │    │              │    │              │    │                  │
 │  Raw CSV     │───▶│   EXTRACT    │───▶│  TRANSFORM   │───▶│      LOAD        │
 │  (source)    │    │              │    │              │    │                  │
 │              │    │ • Read in    │    │ • Std cols   │    │ • SQLite DB      │
 └──────────────┘    │   chunks     │    │ • Fix dtypes │    │ • Processed CSV  │
                     │ • Schema     │    │ • Handle     │    │                  │
                     │   validate   │    │   nulls      │    └──────────────────┘
                     │ • Simulate   │    │ • Remove     │             │
                     │   streaming  │    │   invalid    │             │
                     │   (sleep)    │    │   records    │    ┌──────────────────┐
                     │              │    │ • Feature    │    │                  │
                     └──────────────┘    │   engineer   │    │   DASHBOARD      │
                                         │ • Add meta   │    │   (SQL queries)  │
                                         │              │    │                  │
                                         └──────────────┘    └──────────────────┘

                     ┌──────────────────────────────────────────────────────────┐
                     │                  SUPPORTING LAYERS                       │
                     │   config/config.yaml   │   logs/   │   tests/            │
                     └──────────────────────────────────────────────────────────┘
```

---

## Project structure

```
retail_etl_pipeline/
│
├── data/
│   ├── raw/
│   │   └── retail_sales_dataset.csv       ← source data
│   └── processed/
│       └── retail_sales_processed.csv     ← cleaned output (generated)
│
├── src/
│   ├── extract.py     ← CSV reading, streaming simulation, schema validation
│   ├── transform.py   ← cleaning, feature engineering, metadata tagging
│   ├── load.py        ← SQLite writes, CSV output, query helpers
│   ├── pipeline.py    ← orchestration (streaming + full-load modes)
│   └── utils.py       ← logger setup, config loading, shared helpers
│
├── logs/              ← timestamped log files (auto-created)
│
├── config/
│   └── config.yaml    ← all tunable parameters live here
│
├── tests/
│   └── test_pipeline.py
│
├── requirements.txt
├── README.md
└── main.py            ← entry point
```

---

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/Bangarusowmya/retail-etl-pipeline.git
cd retail-etl-pipeline

python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Run the pipeline

**Streaming mode** (default — processes data in batches with a 2s delay):
```bash
python main.py
```

**Full load mode** (reads everything at once, no delay):
```bash
python main.py --mode full
```

**With analytics dashboard** (runs pipeline + prints SQL summaries):
```bash
python main.py --dashboard
python main.py --mode full --dashboard
```

### 3. Run the tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=src    # with coverage
```

---

## Configuration

All pipeline parameters are in `config/config.yaml`. You can tune batch size, delays, feature engineering toggles, database settings, and log levels without touching the code.

Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pipeline.batch_size` | 100 | Rows per streaming batch |
| `pipeline.batch_delay_seconds` | 2 | Simulated delay between batches |
| `transform.high_value_threshold` | 500 | Total Amount above this = high-value order |
| `load.if_exists` | append | `append` or `replace` the DB table |
| `logging.level` | INFO | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

---

## Sample output

```
=============================================================
RETAIL ETL PIPELINE — STREAMING MODE
Started at: 2026-01-15 10:23:41
=============================================================
Source file: retail_sales_dataset.csv (42.5 KB)
Schema validation passed ✓
------------------------------------------------------------
Processing batch 1 (100 rows)...
  Filled 0 nulls — data looks clean
  Transform complete: 100 → 100 rows (0.0% drop rate)
  DB write complete — 100 rows inserted
  Batch 1 complete ✓
...
=============================================================
PIPELINE COMPLETE
  Batches processed : 10
  Batches failed    : 0
  Rows extracted    : 1000
  Rows transformed  : 1000
  Rows loaded       : 1000
  Duration          : 20.4s
  Success rate      : 100.0%
=============================================================
```

**Analytics dashboard (--dashboard flag):**

```
  Revenue by Product Category
──────────────────────────────────────────────────
 product_category  transactions  total_revenue  avg_order_value
      Electronics           342         342450           1001.3
         Clothing           340         154200            453.5
           Beauty           318          57600            181.1

  Top 5 Months by Revenue
──────────────────────────────────────────────────
 month_name  year  transactions  revenue
    January  2023            98    53200
   February  2023            87    48100
  ...
```

---

## Tech stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.8+ | Widely used in data engineering |
| Data manipulation | Pandas | Industry standard for tabular ETL |
| Database | SQLite | Zero-setup, good enough for most analytics workloads |
| Streaming simulation | `time.sleep` + chunked reads | Kafka-compatible pattern without the infrastructure overhead |
| Config | YAML | Human-readable, easy to version-control |
| Logging | Python stdlib `logging` | Structured, level-aware, file + console |
| Testing | pytest | Clean, minimal, widely understood |

In a production environment, SQLite would be swapped for PostgreSQL (the SQLAlchemy engine change is the only code difference), and the streaming simulation would be replaced with a real Kafka consumer. The transform and load layers are deliberately decoupled from the transport layer to make that swap easy.

---

## Performance notes

The current implementation is single-threaded and processes ~1000 rows in ~20 seconds (most of that is the simulated batch delay). A few things that would help at scale:

- **Disable the batch delay** or reduce it for production runs — set `batch_delay_seconds: 0` in config
- **Increase chunk size** — larger batches mean fewer SQLite round-trips. 1000–5000 rows per batch is a better default for real data
- **Use WAL mode** (already enabled) — allows concurrent reads during writes, which matters if you have dashboards querying while the pipeline is running
- **Consider Parquet for intermediate storage** instead of CSV — faster reads, column-wise compression, preserves dtypes
- **Connection pooling** — the current code opens/closes a connection per batch; a connection pool (SQLAlchemy) would reduce overhead at high frequency

---

## Future enhancements

A few things I'd add given more time:

- **Airflow DAG** — replace the manual `time.sleep` loop with a proper Airflow pipeline with retries, SLAs, and alerting
- **Real Kafka integration** — swap `stream_batches()` for a `confluent-kafka` consumer; the transform/load layers don't change
- **dbt for transformations** — move the SQL-side transformations to dbt models for better documentation and lineage tracking
- **Great Expectations** for data quality — more expressive than the manual validation in `transform.py`, with a built-in expectation suite and HTML reports
- **Incremental loads** — track the last processed `Transaction ID` or `Date` and only process new records on each run
- **PostgreSQL / DuckDB** — DuckDB in particular would be a huge win for the analytics dashboard queries
- **Metrics / observability** — emit pipeline stats (rows/sec, error rate, batch latency) to Prometheus / Grafana
- **Docker + docker-compose** — package the pipeline for easy deployment; add a Postgres container for the production DB config

---

## Dataset

The pipeline was built and tested against a retail sales dataset with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| Transaction ID | int | Unique transaction identifier |
| Date | date | Transaction date |
| Customer ID | string | Customer reference |
| Gender | string | Customer gender |
| Age | int | Customer age |
| Product Category | string | Category (Electronics, Clothing, Beauty) |
| Quantity | int | Units purchased |
| Price per Unit | int | Unit price in local currency |
| Total Amount | int | Total transaction value |

After transformation, the pipeline adds: `revenue_per_unit`, `age_group`, `day_of_week`, `month`, `month_name`, `year`, `quarter`, `is_high_value`, `processed_at`, `batch_id`.

---

## Contributing

PRs welcome. Run `pytest tests/ -v` before opening one.

