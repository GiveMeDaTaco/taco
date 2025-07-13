"""
Output engine: exports final data to files with optional transforms.
"""
from tlptaco.config.schema import OutputConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.iostream.writer import write_dataframe
from tlptaco.sql.generator import SQLGenerator
import os
import importlib

class OutputEngine:
    def __init__(self, cfg: OutputConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("output")

    def num_steps(self) -> int:
        """Return the number of output channels to process."""
        return len(self.cfg.channels)

    def run(self, eligibility_engine, progress=None):
        """
        For each channel, builds the context for the "Claim and Exclude" logic,
        runs the SQL template, and writes the final, deduplicated output file.
        """
        elig_cfg = eligibility_engine.cfg
        templates_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(templates_dir)

        for channel_name, out_cfg in self.cfg.channels.items():
            self.logger.info(f"Preparing output for channel '{channel_name}'")

            # --- This is the new core logic: Building the 'cases' for the template ---
            cases = []
            exclusion_conditions = [] # Stores conditions to exclude previously claimed records

            # Find the corresponding channel configuration from the eligibility setup
            if channel_name not in elig_cfg.conditions.channels:
                self.logger.warning(f"Channel '{channel_name}' found in output config but not in eligibility config. Skipping.")
                continue

            channel_elig_cfg = elig_cfg.conditions.channels[channel_name]

            # Case 1: The Channel BA template. This is always first.
            # A record is claimed if it passes all main AND all channel BA checks.
            ba_summary_col = f"c.passed_all_{channel_name}_BA"
            cases.append({
                'template': f'{channel_name}_BA',
                'condition': f'c.passed_all_main_BA = 1 AND {ba_summary_col} = 1'
            })

            # Case 2: The non-BA templates, processed in a specific order.
            if channel_elig_cfg.others:
                # Process segments in a defined order (e.g., alphabetical) for deterministic results
                for segment_name, segment_checks in sorted(channel_elig_cfg.others.items()):
                    segment_summary_col = f"c.passed_all_{channel_name}_{segment_name}"

                    # The condition requires passing main, channel BA, and the current segment's checks...
                    current_conditions = [
                        'c.passed_all_main_BA = 1',
                        f'{ba_summary_col} = 1',
                        f'{segment_summary_col} = 1'
                    ]
                    # ...AND failing all *previous* non-BA segments.
                    current_conditions.extend(exclusion_conditions)

                    cases.append({
                        'template': f'{channel_name}_{segment_name}',
                        'condition': " AND ".join(current_conditions)
                    })

                    # Add a failure condition for this segment to the exclusion list for the NEXT loop.
                    exclusion_conditions.append(f"{segment_summary_col} = 0")

            # --- Prepare the full context for the SQL template ---
            context = {
                'eligibility_table': elig_cfg.eligibility_table,
                'columns': out_cfg.columns,
                'unique_on': out_cfg.unique_on,
                'cases': cases
            }

            # Render the SQL using the new, powerful output template
            sql = gen.render('output.sql.j2', context)
            self.logger.info(f"Running output SQL for channel {channel_name}")
            self.logger.debug(sql)

            # Fetch the final, clean data
            df = self.runner.to_df(sql)
            self.logger.info(f"Fetched {len(df)} rows for channel {channel_name}")

            # The deduplication is now done in SQL. The old pandas logic is removed.

            # Apply custom function if provided
            cf = out_cfg.output_options.custom_function
            if cf:
                module_name, fn_name = cf.rsplit('.', 1)
                mod = importlib.import_module(module_name)
                func = getattr(mod, fn_name)
                self.logger.info(f"Applying custom function {fn_name} to channel {channel_name}")
                df = func(df)

            # Write file
            fmt = out_cfg.output_options.format
            # Ensure output directory exists
            os.makedirs(out_cfg.file_location, exist_ok=True)
            path = f"{out_cfg.file_location}/{out_cfg.file_base_name}.{fmt}"
            self.logger.info(f"Writing output file for channel {channel_name} to {path}")
            write_dataframe(df, path, fmt, **(out_cfg.output_options.additional_arguments or {}))

            if progress:
                progress.update("Output")