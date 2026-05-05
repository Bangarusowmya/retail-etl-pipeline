"""
utils.py — shared helpers used across the pipeline.

Nothing too fancy here — just stuff I kept copy-pasting between modules
and finally decided to centralise. Logger setup, config loading, that sort of thing.
"""

import logging
import os
import yaml
from datetime import datetime
from pathlib import Path


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Read the YAML config and return it as a plain dict.
    Raises a clear error if the file is missing — easier to debug than a KeyError later.
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file not found at '{config_path}'. "
            "Make sure you're running from the project root."
        )

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_logger(name: str, config: dict = None) -> logging.Logger:
    """
    Set up a logger that writes to both console and a rotating log file.

    Each pipeline run gets its own timestamped log file so we don't lose history.
    The log directory is created automatically if it doesn't exist.
    """
    logger = logging.getLogger(name)

    # don't add duplicate handlers if this logger was already set up
    if logger.handlers:
        return logger

    # pull settings from config, or fall back to sensible defaults
    log_level = logging.INFO
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    log_dir = "logs"

    if config:
        log_cfg = config.get("logging", {})
        level_str = log_cfg.get("level", "INFO").upper()
        log_level = getattr(logging, level_str, logging.INFO)
        log_format = log_cfg.get("format", log_format)
        date_fmt = log_cfg.get("date_format", date_fmt)
        log_dir = config.get("paths", {}).get("log_dir", log_dir)

    logger.setLevel(log_level)
    formatter = logging.Formatter(log_format, datefmt=date_fmt)

    # console handler
    if not config or config.get("logging", {}).get("log_to_console", True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # file handler — one log file per run
    if not config or config.get("logging", {}).get("log_to_file", True):
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"pipeline_{timestamp}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def ensure_dirs(paths: list):
    """Make sure all required directories exist before the pipeline starts."""
    for path in paths:
        dir_path = Path(path).parent if "." in Path(path).name else Path(path)
        dir_path.mkdir(parents=True, exist_ok=True)


def get_run_timestamp() -> str:
    """Simple timestamp string for tagging pipeline runs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pretty_separator(char: str = "-", width: int = 60) -> str:
    """Returns a separator line. Purely cosmetic for log readability."""
    return char * width
