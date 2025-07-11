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
    parser.add_argument("--progress", "-p", action="store_true", help="Show progress bars for pipeline stages (requires rich)")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = configure_logging(config.logging, verbose=args.verbose)
    runner = DBRunner(config.database, logger)

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
        wf_steps = waterfall_engine.num_steps()
        layers = [("Eligibility", elig_steps), ("Waterfall", wf_steps)]
        if args.mode == "full":
            out_steps = output_engine.num_steps()
            layers.append(("Output", out_steps))
        # Run with progress bars
        with ProgressManager(layers, units="steps") as pm:
            eligibility_engine.run(progress=pm)
            waterfall_engine.run(eligibility_engine, progress=pm)
            if args.mode == "full":
                output_engine.run(eligibility_engine, progress=pm)
    else:
        # Run without progress bars
        eligibility_engine.run()
        waterfall_engine.run(eligibility_engine)
        if args.mode == "full":
            output_engine.run(eligibility_engine)

    runner.cleanup()

if __name__ == "__main__":
    main()