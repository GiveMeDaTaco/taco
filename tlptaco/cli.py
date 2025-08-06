"""
Command-line interface for tlptaco version 2.
"""
import argparse
import sys

# Initialize CLI spinner as early as possible to cover heavy imports
from tlptaco.utils.loading_bar import LoadingSpinner
_spinner = LoadingSpinner()
_spinner.start()
import atexit
# Ensure spinner is stopped on any exit (including --help or errors)
atexit.register(_spinner.stop)

from tlptaco.config.loader import load_config
from tlptaco.db.runner import DBRunner
from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.engines.output import OutputEngine
from tlptaco.utils.logging import configure_logging
from typing import List
import time


def main():
    parser = argparse.ArgumentParser(description="tlptaco v2: Eligibility → Waterfall → Output pipeline")
    import os
    parser.add_argument("--config", "-c", required=True, help="Path to configuration YAML/JSON file")
    parser.add_argument("--output-dir", "-o", default=None,
                        help="Directory to write outputs and logs (defaults to current working directory)")
    parser.add_argument("--mode", "-m", choices=["full", "presizing"], default="full",
                        help="Run mode: full (includes output) or presizing (eligibility+waterfall only)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose (DEBUG) console output")
    parser.add_argument("--progress", "-p", action="store_true", help="Show progress bars for pipeline stages (requires rich)")
    # Stop the initialization spinner before parsing arguments (so help prints cleanly)
    try:
        _spinner.stop()
    except Exception:
        pass
    args = parser.parse_args()

    # Determine working directory for outputs/logs
    workdir = os.path.abspath(args.output_dir) if args.output_dir else os.getcwd()
    logs_dir_root = os.path.join(workdir, 'logs')
    os.makedirs(logs_dir_root, exist_ok=True)
    from tlptaco.utils.fs import grant_group_rwx
    grant_group_rwx(logs_dir_root)
    # Load configuration
    config = load_config(args.config)
    # ------------------------------------------------------------------
    # Derive default log filenames (include offer_code for easy tracing)
    # ------------------------------------------------------------------
    import re
    safe_offer = re.sub(r'[^A-Za-z0-9_-]+', '_', config.offer_code or 'run')
    logs_dir = os.path.join(workdir, 'logs')

    # If user supplied file paths, make them absolute (relative to workdir)
    if config.logging.file and not os.path.isabs(config.logging.file):
        config.logging.file = os.path.join(workdir, config.logging.file)
    if config.logging.debug_file and not os.path.isabs(config.logging.debug_file):
        config.logging.debug_file = os.path.join(workdir, config.logging.debug_file)
    if getattr(config.logging, 'sql_file', None) and not os.path.isabs(config.logging.sql_file):
        config.logging.sql_file = os.path.join(workdir, config.logging.sql_file)

    # Override waterfall and output paths to live under --output-dir
    # Waterfall output directory
    wf_dir = config.waterfall.output_directory
    if wf_dir and not os.path.isabs(wf_dir):
        config.waterfall.output_directory = os.path.join(workdir, wf_dir)

    # Output channel file locations
    for channel_cfg in config.output.channels.values():
        loc = channel_cfg.file_location
        if loc and not os.path.isabs(loc):
            channel_cfg.file_location = os.path.join(workdir, loc)
    logger = configure_logging(config.logging, verbose=args.verbose)
    runner = DBRunner(config.database, logger)
    # Ensure spinner is stopped after runner is ready
    try:
        _spinner.stop()
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Prepare Pre-SQL engine (executes user-provided scripts before pipeline)
    # ------------------------------------------------------------------
    from tlptaco.engines.presql import PreSQLEngine

    presql_engine = PreSQLEngine(config.pre_sql, runner, logger)

    # Instantiate engines
    eligibility_engine = EligibilityEngine(config.eligibility, runner, logger)
    waterfall_engine = WaterfallEngine(config.waterfall, runner, logger)
    # Propagate metadata from config
    waterfall_engine.offer_code = config.offer_code
    waterfall_engine.campaign_planner = config.campaign_planner
    waterfall_engine.lead = config.lead
    if args.mode == "full":
        output_engine = OutputEngine(config.output, runner, logger)

    if args.progress:
        # Lazy import of ProgressManager to avoid requiring rich if unused
        from tlptaco.utils.loading_bar import ProgressManager
        # Determine steps for each stage
        elig_steps = eligibility_engine.num_steps()
        wf_steps = waterfall_engine.num_steps(eligibility_engine)
        layers = []
        presql_steps = presql_engine.num_steps()
        if presql_steps:
            layers.append(("Pre-SQL", presql_steps))
        layers.extend([("Eligibility", elig_steps), ("Waterfall", wf_steps)])
        if args.mode == "full":
            out_steps = output_engine.num_steps(eligibility_engine)
            layers.append(("Output", out_steps))
        # Run with progress bars
        with ProgressManager(layers, units="steps", title=config.offer_code) as pm:
            # 1. Pre-SQL stage
            if presql_steps:
                presql_engine.run(progress=pm)

            # 2. main pipeline engines
            eligibility_engine.run(progress=pm)
            start_wf = time.time()
            waterfall_engine.run(progress=pm)
            logger.info(f"Waterfall stage completed in {time.time()-start_wf:.2f}s")
            if args.mode == "full":
                start_out = time.time()
                output_engine.run(progress=pm)
                logger.info(f"Output stage completed in {time.time()-start_out:.2f}s")
    else:
        # Run without progress bars
        # Execute pre-sql first
        presql_engine.run()

        # main pipeline with simple timing
        start_elig = time.time()
        eligibility_engine.run()
        logger.info(f"Eligibility stage completed in {time.time()-start_elig:.2f}s")

        start_wf = time.time()
        waterfall_engine.run(eligibility_engine)
        logger.info(f"Waterfall stage completed in {time.time()-start_wf:.2f}s")

        if args.mode == "full":
            start_out = time.time()
            output_engine.run(eligibility_engine)
            logger.info(f"Output stage completed in {time.time()-start_out:.2f}s")

    runner.cleanup()

if __name__ == "__main__":
    main()
