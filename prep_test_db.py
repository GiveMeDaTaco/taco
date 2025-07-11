#!/usr/bin/env python3
"""
Prepare Teradata tables for the example campaign test.
"""
import sys
from tlptaco.config.loader import load_config
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import configure_logging

def main():
    config_path = "example_campaign.yaml"
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading config {config_path}: {e}", file=sys.stderr)
        sys.exit(1)
    # Ensure log directories exist
    import os
    for log_path in (config.logging.file, config.logging.debug_file):
        if log_path:
            log_dir = os.path.dirname(log_path)
            if log_dir and not os.path.isdir(log_dir):
                os.makedirs(log_dir, exist_ok=True)
    # Set up logging
    logger = configure_logging(config.logging)
    # Create runner
    runner = DBRunner(config.database, logger)
    # Statements to drop, create, and populate test tables
    statements = [
        # Drop existing tables if they exist (default database)
        "DROP TABLE customers;",
        "DROP TABLE sales;",
        # Create tables in default database
        "CREATE TABLE customers (customer_id INTEGER, status VARCHAR(20));",
        "CREATE TABLE sales (customer_id INTEGER, amount DECIMAL(10,2));",
        # Insert sample data (Teradata requires single-row INSERTs)
        "INSERT INTO customers (customer_id, status) VALUES (1, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (2, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (3, 'Inactive');",
        "INSERT INTO sales (customer_id, amount) VALUES (1, 50.00);",
        "INSERT INTO sales (customer_id, amount) VALUES (1, 150.00);",
        "INSERT INTO sales (customer_id, amount) VALUES (2, 75.00);",
        "INSERT INTO sales (customer_id, amount) VALUES (3, 0.00);"
    ]
    for sql in statements:
        try:
            logger.info(f"Executing: {sql}")
            runner.run(sql)
        except Exception as e:
            logger.warning(f"Ignoring error for statement: {e}")
    # Cleanup
    runner.cleanup()
    logger.info("Test tables have been prepared.")

if __name__ == "__main__":
    main()