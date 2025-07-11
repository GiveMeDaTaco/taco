#!/usr/bin/env python3
"""
Direct Teradataml context test.
"""
import sys
import teradataml
from tlptaco.config.loader import load_config

def main():
    cfg = load_config("example_campaign.yaml").database
    try:
        print("Attempting teradataml.create_context...")
        teradataml.create_context(
            host=cfg.host,
            username=cfg.user,
            password=cfg.password,
            logmech=cfg.logmech
        )
        print("Context created successfully.")
    except Exception as e:
        print(f"Direct teradataml context error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            teradataml.remove_context()
        except:
            pass

if __name__ == '__main__':
    main()