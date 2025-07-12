#!/usr/bin/env python3
"""
Prepare Teradata tables for the example POC campaign test.
"""
import sys
import os
from tlptaco.config.loader import load_config
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import configure_logging

def main():
    config_path = "example_campaign_poc.yaml"
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading config {config_path}: {e}", file=sys.stderr)
        sys.exit(1)
    # Ensure log directories exist
    for log_path in (config.logging.file, config.logging.debug_file):
        if log_path:
            log_dir = os.path.dirname(log_path)
            if log_dir and not os.path.isdir(log_dir):
                os.makedirs(log_dir, exist_ok=True)
    # Configure logging
    logger = configure_logging(config.logging)
    # Initialize DB runner
    runner = DBRunner(config.database, logger)

    # Drop existing POC tables
    statements = [
        "DROP TABLE segments;",
        "DROP TABLE regions;",
        "DROP TABLE accounts;",
        "DROP TABLE customers;",
    ]
    # Create tables
    statements += [
        "CREATE TABLE customers (customer_id INTEGER, status VARCHAR(20));",
        "CREATE TABLE accounts (customer_id INTEGER, account_id INTEGER, acct_type VARCHAR(20));",
        "CREATE TABLE regions (customer_id INTEGER, region_id INTEGER, region_type VARCHAR(20));",
        "CREATE TABLE segments (customer_id INTEGER, segment_id INTEGER, segment_flag VARCHAR(20));",
    ]
    # Insert sample data
    # --- add 50 more inserts for every table ---
    statements += [
        "INSERT INTO customers (customer_id, status) VALUES (6, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (7, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (8, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (9, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (10, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (11, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (12, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (13, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (14, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (15, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (16, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (17, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (18, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (19, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (20, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (21, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (22, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (23, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (24, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (25, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (26, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (27, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (28, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (29, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (30, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (31, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (32, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (33, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (34, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (35, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (36, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (37, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (38, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (39, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (40, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (41, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (42, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (43, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (44, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (45, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (46, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (47, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (48, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (49, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (50, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (51, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (52, 'Active');",
        "INSERT INTO customers (customer_id, status) VALUES (53, 'Pending');",
        "INSERT INTO customers (customer_id, status) VALUES (54, 'Inactive');",
        "INSERT INTO customers (customer_id, status) VALUES (55, 'Active');",

        # --- accounts (one per new customer, acct_type alternates) ---
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (6, 306, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (7, 307, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (8, 308, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (9, 309, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (10, 310, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (11, 311, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (12, 312, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (13, 313, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (14, 314, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (15, 315, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (16, 316, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (17, 317, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (18, 318, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (19, 319, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (20, 320, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (21, 321, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (22, 322, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (23, 323, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (24, 324, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (25, 325, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (26, 326, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (27, 327, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (28, 328, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (29, 329, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (30, 330, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (31, 331, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (32, 332, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (33, 333, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (34, 334, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (35, 335, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (36, 336, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (37, 337, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (38, 338, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (39, 339, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (40, 340, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (41, 341, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (42, 342, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (43, 343, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (44, 344, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (45, 345, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (46, 346, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (47, 347, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (48, 348, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (49, 349, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (50, 350, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (51, 351, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (52, 352, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (53, 353, 'Checking');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (54, 354, 'Savings');",
        "INSERT INTO accounts (customer_id, account_id, acct_type) VALUES (55, 355, 'Checking');",

        # --- regions ---
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (6, 60, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (7, 70, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (8, 80, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (9, 90, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (10, 100, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (11, 110, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (12, 120, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (13, 130, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (14, 140, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (15, 150, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (16, 160, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (17, 170, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (18, 180, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (19, 190, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (20, 200, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (21, 210, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (22, 220, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (23, 230, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (24, 240, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (25, 250, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (26, 260, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (27, 270, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (28, 280, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (29, 290, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (30, 300, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (31, 310, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (32, 320, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (33, 330, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (34, 340, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (35, 350, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (36, 360, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (37, 370, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (38, 380, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (39, 390, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (40, 400, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (41, 410, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (42, 420, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (43, 430, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (44, 440, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (45, 450, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (46, 460, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (47, 470, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (48, 480, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (49, 490, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (50, 500, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (51, 510, 'East');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (52, 520, 'West');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (53, 530, 'North');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (54, 540, 'South');",
        "INSERT INTO regions (customer_id, region_id, region_type) VALUES (55, 550, 'East');",

        # --- segments ---
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (6, 6001, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (7, 6002, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (8, 6003, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (9, 6004, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (10, 6005, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (11, 6006, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (12, 6007, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (13, 6008, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (14, 6009, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (15, 6010, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (16, 6011, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (17, 6012, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (18, 6013, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (19, 6014, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (20, 6015, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (21, 6016, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (22, 6017, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (23, 6018, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (24, 6019, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (25, 6020, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (26, 6021, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (27, 6022, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (28, 6023, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (29, 6024, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (30, 6025, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (31, 6026, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (32, 6027, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (33, 6028, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (34, 6029, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (35, 6030, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (36, 6031, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (37, 6032, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (38, 6033, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (39, 6034, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (40, 6035, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (41, 6036, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (42, 6037, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (43, 6038, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (44, 6039, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (45, 6040, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (46, 6041, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (47, 6042, 'Promo2');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (48, 6043, 'HighTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (49, 6044, 'LowTx');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (50, 6045, 'PromoA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (51, 6046, 'PromoB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (52, 6047, 'FlagA');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (53, 6048, 'FlagB');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (54, 6049, 'Promo1');",
        "INSERT INTO segments (customer_id, segment_id, segment_flag) VALUES (55, 6050, 'Promo2');"
    ]

    # Execute statements
    for sql in statements:
        try:
            logger.info(f"Executing: {sql}")
            runner.run(sql)
        except Exception as e:
            logger.warning(f"Ignoring error: {e}")

    # Cleanup
    runner.cleanup()
    logger.info("Test POC tables have been prepared.")

if __name__ == "__main__":
    main()