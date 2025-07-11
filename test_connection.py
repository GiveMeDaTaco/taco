#!/usr/bin/env python3
"""
Simple script to verify Teradata Vantage connectivity using tlptaco DBRunner.
"""
import sys
from tlptaco.config.loader import load_config
from tlptaco.db.runner import DBRunner

def main():
    config_path = "example_campaign.yaml"
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading config {config_path}: {e}", file=sys.stderr)
        sys.exit(1)
    # Ensure log directory exists for logging
    import os, logging
    if config.logging.file:
        log_dir = os.path.dirname(config.logging.file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    # Set up a logger
    try:
        from tlptaco.utils.logging import configure_logging
        logger = configure_logging(config.logging)
    except ImportError:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("test_connection")
    # Initialize runner and attempt a simple query
    runner = DBRunner(config.database, logger)
    try:
        # Execute a trivial query to test connectivity
        print("Testing connectivity with SELECT 1...")
        df = runner.to_df("SELECT 1 AS test;")
        print("Connection successful.")
        print(df)
    except Exception as e:
        print(f"Connection test failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        runner.cleanup()

if __name__ == "__main__":
    main()