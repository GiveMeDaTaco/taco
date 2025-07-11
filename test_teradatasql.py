#!/usr/bin/env python3
"""
Test direct connectivity to Teradata Vantage using the teradatasql driver.
"""
import sys
import os
import teradatasql
from tlptaco.config.loader import load_config

def main():
    # Load database config
    cfg = load_config("example_campaign.yaml").database
    host = cfg.host
    user = cfg.user
    pwd = cfg.password
    port = 1025  # default Teradata Vantage SQL port
    print(f"Connecting to Teradata via teradatasql {host}:{port} as {user}...")
    try:
        # Optionally set default database via environment var
        db = os.getenv('TD_DATABASE')
        # teradatasql uses default port 1025; omit 'port' parameter
        # Build connection kwargs; only include database if specified
        conn_kwargs = {
            'host': host,
            'user': user,
            'password': pwd
        }
        if db:
            conn_kwargs['database'] = db
        conn = teradatasql.connect(**conn_kwargs)
        cur = conn.cursor()
        cur.execute("SELECT 1 AS test;")
        rows = cur.fetchall()
        print("Connection successful, query result:", rows)
        conn.close()
    except Exception as e:
        print(f"teradatasql.connect failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()