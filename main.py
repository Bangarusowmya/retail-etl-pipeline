"""
main.py — pipeline entry point.

Run modes:
  python main.py               → streaming (default)
  python main.py --mode full   → full load (no batch delay)
  python main.py --dashboard   → run pipeline + print summary stats

Usage examples:
  python main.py
  python main.py --mode full
  python main.py --dashboard
  python main.py --mode full --dashboard
"""

import argparse
import sys
from pathlib import Path

from src.utils import load_config, get_logger, pretty_separator
from src.pipeline import run_streaming, run_full
from src.load import query_db


def parse_args():
    parser = argparse.ArgumentParser(
        description="Retail Sales ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # run in streaming mode
  python main.py --mode full        # run full load
  python main.py --dashboard        # run + show analytics summary
        """
    )
    parser.add_argument(
        "--mode",
        choices=["streaming", "full"],
        default="streaming",
        help="Pipeline run mode (default: streaming)",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Print analytics dashboard after pipeline completes",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml)",
    )
    return parser.parse_args()


def print_dashboard(config: dict):
    """
    Print a simple analytics summary using SQL queries on the loaded data.
    Nothing fancy — just enough to validate the pipeline worked and give
    a quick sense of the data.
    """
    db_path = config["paths"]["db_path"]
    print("\n" + pretty_separator("="))
    print("  RETAIL SALES — ANALYTICS DASHBOARD")
    print(pretty_separator("="))

    queries = {
        "Revenue by Product Category": """
            SELECT product_category,
                   COUNT(*)              AS transactions,
                   SUM(total_amount)     AS total_revenue,
                   AVG(total_amount)     AS avg_order_value
            FROM retail_transactions
            GROUP BY product_category
            ORDER BY total_revenue DESC
        """,

        "Sales by Gender": """
            SELECT gender,
                   COUNT(*)          AS transactions,
                   SUM(total_amount) AS revenue,
                   AVG(age)          AS avg_age
            FROM retail_transactions
            GROUP BY gender
        """,

        "Top 5 Months by Revenue": """
            SELECT month_name, year,
                   COUNT(*)          AS transactions,
                   SUM(total_amount) AS revenue
            FROM retail_transactions
            GROUP BY year, month
            ORDER BY revenue DESC
            LIMIT 5
        """,

        "Age Group Breakdown": """
            SELECT age_group,
                   COUNT(*)          AS customers,
                   SUM(total_amount) AS revenue,
                   AVG(total_amount) AS avg_spend
            FROM retail_transactions
            GROUP BY age_group
            ORDER BY revenue DESC
        """,

        "High-Value Orders Summary": """
            SELECT is_high_value,
                   COUNT(*)          AS orders,
                   SUM(total_amount) AS revenue
            FROM retail_transactions
            GROUP BY is_high_value
        """,

        "Category × Gender Revenue Split": """
            SELECT product_category,
                   gender,
                   COUNT(*)          AS txns,
                   SUM(total_amount) AS revenue
            FROM retail_transactions
            GROUP BY product_category, gender
            ORDER BY product_category, revenue DESC
        """,
    }

    for title, sql in queries.items():
        print(f"\n{'─' * 50}")
        print(f"  {title}")
        print(f"{'─' * 50}")
        try:
            result = query_db(sql.strip(), db_path)
            if result.empty:
                print("  (no data)")
            else:
                # format numbers nicely
                for col in result.select_dtypes(include="float").columns:
                    result[col] = result[col].round(2)
                print(result.to_string(index=False))
        except Exception as e:
            print(f"  Query failed: {e}")

    print("\n" + pretty_separator("=") + "\n")


def main():
    args = parse_args()

    # load config — bail early with a clear message if it's missing
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

    logger = get_logger("main", config)

    # make sure output dirs exist before we start
    from src.utils import ensure_dirs
    ensure_dirs([
        config["paths"]["raw_data"],
        config["paths"]["processed_data"],
        config["paths"]["db_path"],
        config["paths"]["log_dir"],
    ])

    # run the pipeline
    try:
        if args.mode == "streaming":
            stats = run_streaming(config)
        else:
            stats = run_full(config)

        if stats.batches_failed > 0 and stats.batches_processed == 0:
            logger.error("Pipeline completed with all batches failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Pipeline crashed: {e}", exc_info=True)
        sys.exit(1)

    # optional dashboard
    if args.dashboard:
        print_dashboard(config)

    logger.info("All done. Exiting.")


if __name__ == "__main__":
    main()
