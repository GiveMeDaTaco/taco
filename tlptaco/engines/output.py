"""
Output engine: exports final data to files with optional transforms.
"""
from tlptaco.config.schema import OutputConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
import tlptaco.iostream.writer as io_writer
from tlptaco.sql.generator import SQLGenerator
import os
import importlib


class OutputEngine:
    def __init__(self, cfg: OutputConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("output")
        # Cache for prepared jobs and the eligibility engine
        self._output_jobs = None
        self._eligibility_engine = None

    def _prepare_output_steps(self, eligibility_engine):
        """
        Prepares all output jobs, including SQL generation, without executing them.
        The results are cached to avoid redundant work.
        """
        self._eligibility_engine = eligibility_engine

        if self._output_jobs is not None:
            self.logger.info("Using cached output steps.")
            return

        self.logger.info("No cached steps found. Preparing output jobs and SQL.")
        self._output_jobs = []
        elig_cfg = eligibility_engine.cfg
        templates_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(templates_dir)

        # --- START MODIFICATION ---
        def create_sql_condition(check_list):
            """Helper function to create a combined SQL AND condition."""
            if not check_list:
                return "1 = 1"  # Return a tautology if the list is empty
            return " AND ".join([f"c.{check.name} = 1" for check in check_list])

        for channel_name, out_cfg in self.cfg.channels.items():
            self.logger.info(f"Preparing logic for channel '{channel_name}'")

            if channel_name not in elig_cfg.conditions.channels:
                self.logger.warning(f"Channel '{channel_name}' in output config but not eligibility. Skipping.")
                continue

            channel_elig_cfg = elig_cfg.conditions.channels[channel_name]
            cases = []
            exclusion_conditions = []

            # Condition for main BA checks
            main_ba_condition = create_sql_condition(elig_cfg.conditions.main.BA)

            # Case 1: Channel BA
            channel_ba_checks = channel_elig_cfg.BA
            channel_ba_condition = create_sql_condition(channel_ba_checks)
            cases.append({
                'template': f'{channel_name}_BA',
                'condition': f"({main_ba_condition}) AND ({channel_ba_condition})"
            })

            # Case 2: Other segments (sorted by priority as defined in YAML)
            if channel_elig_cfg.others:
                for segment_name, segment_checks in channel_elig_cfg.others.items():
                    segment_condition = create_sql_condition(segment_checks)

                    # Conditions for this segment include main BA, channel BA, and the segment's own checks
                    current_conditions = [
                        f"({main_ba_condition})",
                        f"({channel_ba_condition})",
                        f"({segment_condition})",
                        *exclusion_conditions
                    ]
                    cases.append({
                        'template': f'{channel_name}_{segment_name}',
                        'condition': " AND ".join(current_conditions)
                    })

                    # Add the inverse of this segment's condition to the exclusion list for the *next* segment
                    # This ensures mutual exclusivity
                    inverse_segment_condition = " OR ".join([f"c.{check.name} = 0" for check in segment_checks])
                    if inverse_segment_condition:
                        exclusion_conditions.append(f"({inverse_segment_condition})")
            # --- END MODIFICATION ---

            context = {'eligibility_table': elig_cfg.eligibility_table, 'columns': out_cfg.columns,
                       'unique_on': out_cfg.unique_on, 'cases': cases}
            sql = gen.render('output.sql.j2', context)

            # Log rendered SQL
            from tlptaco.utils.logging import log_sql_section
            log_sql_section(f'Output - {channel_name}', sql)

            # Determine file extension: use .xlsx for 'excel'
            fmt = out_cfg.output_options.format.lower()

            if fmt == 'table':
                # file_location = schema, file_base_name = table
                full_table = f"{out_cfg.file_location}.{out_cfg.file_base_name}"
                self._output_jobs.append({
                    'channel_name': channel_name,
                    'sql': sql,
                    'fmt': 'table',
                    'table_name': full_table,
                    'unique_on': out_cfg.unique_on,
                })
            else:
                ext = 'xlsx' if fmt == 'excel' else fmt
                path = os.path.join(out_cfg.file_location,
                                    f"{out_cfg.file_base_name}.{ext}")
                self._output_jobs.append({
                    'channel_name': channel_name,
                    'sql': sql,
                    'fmt': fmt,
                    'path': path,
                    'output_options': out_cfg.output_options
                })

    def num_steps(self, eligibility_engine) -> int:
        """
        Calculates the total number of output files to be generated.
        Caches the eligibility_engine for the run() method.
        """
        self.logger.info("Calculating the number of output steps.")
        self._prepare_output_steps(eligibility_engine)
        total_steps = len(self._output_jobs)
        self.logger.info(f"Calculation complete: {total_steps} steps (files).")
        return total_steps

    def run(self, eligibility_engine=None, progress=None):
        """
        For each channel, runs the SQL and writes the final output file.
        The eligibility_engine is optional if already provided to num_steps().
        """
        engine_to_use = eligibility_engine or self._eligibility_engine
        if not engine_to_use:
            raise ValueError(
                "An eligibility_engine instance must be provided either to run() or to a prior num_steps() call.")

        self._prepare_output_steps(engine_to_use)

        for job in self._output_jobs:
            channel_name = job['channel_name']
            self.logger.info(f"Running output job for channel {channel_name}")
            self.logger.debug(job['sql'])

            if job['fmt'] == 'table':
                table_full = job['table_name']
                try:
                    self.logger.info(f"Dropping existing table {table_full} (if any)")
                    self.runner.run(f"DROP TABLE {table_full};")
                except Exception:
                    self.logger.debug("No pre-existing table to drop or driver raised warning")

                create_sql = (
                    f"CREATE MULTISET TABLE {table_full} AS (\n" +
                    job['sql'].rstrip().rstrip(';') +
                    "\n) WITH DATA;"
                )
                self.logger.info(f"Creating output table {table_full}")
                self.runner.run(create_sql)
            else:
                df = self.runner.to_df(job['sql'])
                self.logger.info(f"Fetched {len(df)} rows for channel {channel_name}")

                cf = job['output_options'].custom_function if 'output_options' in job else None
                if cf:
                    module_name, fn_name = cf.rsplit('.', 1)
                    mod = importlib.import_module(module_name)
                    func = getattr(mod, fn_name)
                    self.logger.info(f"Applying custom function {fn_name} to channel {channel_name}")
                    df = func(df)

                os.makedirs(os.path.dirname(job['path']), exist_ok=True)
                self.logger.info(f"Writing output file for channel {channel_name} to {job['path']}")
                # Delegate to io_writer.write_dataframe so tests can monkey-patch
                io_writer.write_dataframe(
                    df,
                    job['path'],
                    job['output_options'].format,
                    **(job['output_options'].additional_arguments or {})
                )

            if progress:
                progress.update("Output")