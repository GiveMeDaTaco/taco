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
from typing import List, Tuple


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
    os.makedirs(os.path.join(workdir, 'logs'), exist_ok=True)
    # Load configuration
    config = load_config(args.config)
    # ------------------------------------------------------------------
    # Derive default log filenames (include offer_code for easy tracing)
    # ------------------------------------------------------------------
    import re
    safe_offer = re.sub(r'[^A-Za-z0-9_-]+', '_', config.offer_code or 'run')
    logs_dir = os.path.join(workdir, 'logs')

    # Override logging paths to use workdir if not explicitly set
    if not config.logging.file:
        config.logging.file = os.path.join(logs_dir, f'tlptaco_{safe_offer}.log')
    if not config.logging.debug_file:
        config.logging.debug_file = os.path.join(logs_dir, f'tlptaco_{safe_offer}.debug.log')
    # Default SQL log file path
    if not getattr(config.logging, 'sql_file', None):
        config.logging.sql_file = os.path.join(logs_dir, f'tlptaco_{safe_offer}.sql.log')

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
    # Execute pre-run SQL files (if any) BEFORE running engines
    # ------------------------------------------------------------------

    def _split_sql(text: str) -> List[str]:
        """Split raw SQL text on semicolons into individual statements.
        A very naive split that works for well-formed scripts without
        procedural blocks containing semicolons."""
        stmts = [s.strip() for s in text.split(';')]
        return [s for s in stmts if s]

    pre_sql_files: List[str] = config.pre_sql or []
    sql_statements: List[Tuple[str, str]] = []  # list of (file, stmt)
    for path in pre_sql_files:
        try:
            with open(path, 'r') as f:
                content = f.read()
            for stmt in _split_sql(content):
                sql_statements.append((path, stmt))
        except Exception as e:
            logger.error(f"Failed reading pre_sql file {path}: {e}")
            raise

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
        if sql_statements:
            layers.append(("SQL Statements", len(sql_statements)))
        layers.extend([("Eligibility", elig_steps), ("Waterfall", wf_steps)])
        if args.mode == "full":
            out_steps = output_engine.num_steps(eligibility_engine)
            layers.append(("Output", out_steps))
        # Run with progress bars
        with ProgressManager(layers, units="steps", title=config.offer_code) as pm:
            # 1. run any pre_sql statements
            if sql_statements:
                for _file, stmt in sql_statements:
                    logger.info(f"Executing pre-SQL from {_file}")
                    try:
                        runner.run(stmt)
                    finally:
                        pm.update("SQL Statements")

            # 2. run pipeline engines
            eligibility_engine.run(progress=pm)
            waterfall_engine.run(progress=pm)
            if args.mode == "full":
                output_engine.run(progress=pm)
    else:
        # Run without progress bars
        # Execute pre-sql first
        for _file, stmt in sql_statements:
            logger.info(f"Executing pre-SQL from {_file}")
            runner.run(stmt)

        # main pipeline
        eligibility_engine.run()
        waterfall_engine.run(eligibility_engine)
        if args.mode == "full":
            output_engine.run(eligibility_engine)

    runner.cleanup()

if __name__ == "__main__":
    main()
