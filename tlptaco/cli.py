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
    # Override logging paths to use workdir if not explicitly set
    if not config.logging.file:
        config.logging.file = os.path.join(workdir, 'logs', 'tlptaco.log')
    if not config.logging.debug_file:
        config.logging.debug_file = os.path.join(workdir, 'logs', 'tlptaco.debug.log')

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

    # Instantiate engines
    eligibility_engine = EligibilityEngine(config.eligibility, runner, logger)
    waterfall_engine = WaterfallEngine(config.waterfall, runner, logger)
    if args.mode == "full":
        output_engine = OutputEngine(config.output, runner, logger)

    if args.progress:
        # Lazy import of ProgressManager to avoid requiring rich if unused
        from tlptaco.utils.loading_bar import ProgressManager
        # Determine steps for each stage
        elig_steps = eligibility_engine.num_steps()
        wf_steps = waterfall_engine.num_steps(eligibility_engine)
        layers = [("Eligibility", elig_steps), ("Waterfall", wf_steps)]
        if args.mode == "full":
            out_steps = output_engine.num_steps(eligibility_engine)
            layers.append(("Output", out_steps))
        # Run with progress bars
        with ProgressManager(layers, units="steps", title=config.offer_code) as pm:
            eligibility_engine.run(progress=pm)
            waterfall_engine.run(progress=pm)
            if args.mode == "full":
                output_engine.run(progress=pm)
    else:
        # Run without progress bars
        eligibility_engine.run()
        waterfall_engine.run(eligibility_engine)
        if args.mode == "full":
            output_engine.run(eligibility_engine)

    runner.cleanup()

if __name__ == "__main__":
    main()
