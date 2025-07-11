"""
Command-line interface for tlptaco version 2.
"""
import argparse
import sys

from tlptaco.config.loader import load_config
from tlptaco.db.runner import DBRunner
from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.engines.output import OutputEngine
from tlptaco.utils.logging import configure_logging

def main():
    parser = argparse.ArgumentParser(description="tlptaco v2: Eligibility → Waterfall → Output pipeline")
    parser.add_argument("--config", "-c", required=True, help="Path to configuration YAML/JSON file")
    parser.add_argument("--mode", "-m", choices=["full", "presizing"], default="full",
                        help="Run mode: full (includes output) or presizing (eligibility+waterfall only)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose (DEBUG) console output")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = configure_logging(config.logging, verbose=args.verbose)
    runner = DBRunner(config.database, logger)

    # Eligibility stage
    eligibility = EligibilityEngine(config.eligibility, runner, logger)
    eligibility.run()

    # Waterfall stage
    waterfall = WaterfallEngine(config.waterfall, runner, logger)
    waterfall.run(eligibility)

    if args.mode == "full":
        # Output stage
        output = OutputEngine(config.output, runner, logger)
        output.run(eligibility)

    runner.cleanup()

if __name__ == "__main__":
    main()