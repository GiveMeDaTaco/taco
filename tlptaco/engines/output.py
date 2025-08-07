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
from typing import Any


class OutputEngine:
    """Materialise final *output* datasets / tables.

    For each channel in ``output.channels`` the engine renders a SQL SELECT
    against the smart table (optionally runs a user-specified function on
    the resulting DataFrame) and finally persists to:

    * a DB table (`format == 'table'`) **or**
    * a file (CSV / Parquet / Excel) with an *automatic* ``_YYYYMMDD``
      suffix as of the current date.

    Example
    -------
    >>> out_engine = OutputEngine(app_cfg.output, runner)
    >>> out_engine.run(elig_engine)

    Notes
    -----
    Database I/O is delegated to :class:`tlptaco.db.runner.DBRunner`; in your
    unit tests you can monkey-patch its ``to_df`` method to return dummy
    DataFrames and avoid any real DB dependency.
    """
    def __init__(self, cfg: OutputConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("output")
        # Cache for prepared jobs and the eligibility engine
        self._output_jobs = None
        self._eligibility_engine = None

    # ------------------------------------------------------------------
    # Internal helper – append current date YYYYMMDD to a base filename
    # ------------------------------------------------------------------

    @staticmethod
    def _append_today(base: str) -> str:
        from datetime import datetime
        today_str = datetime.now().strftime('%Y%m%d')
        if base.endswith(today_str):
            return base
        return f"{base}_{today_str}"

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
        _date_suffix = self._append_today  # local alias

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

            # Channel BA checks string (re-used by all non-BA templates)
            channel_ba_checks = channel_elig_cfg.BA
            channel_ba_condition = create_sql_condition(channel_ba_checks)

            # Non-BA templates (sorted by YAML order)
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
                    # Use *segment name only* for template_id as requested
                    cases.append({
                        'template': f'{segment_name}',
                        'condition': " AND ".join(current_conditions)
                    })

                    # Add the inverse of this segment's condition to the exclusion list for the *next* segment
                    # This ensures mutual exclusivity
                    inverse_segment_condition = " OR ".join([f"c.{check.name} = 0" for check in segment_checks])
                    if inverse_segment_condition:
                        exclusion_conditions.append(f"({inverse_segment_condition})")
            # --- END MODIFICATION ---

            context = {'eligibility_table': elig_cfg.eligibility_table, 'columns': out_cfg.columns,
                       'unique_on': out_cfg.unique_on,
                       'cases': cases,
                       'column_types': out_cfg.column_types or {},
                       }
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
                                    f"{_date_suffix(out_cfg.file_base_name)}.{ext}")
                self._output_jobs.append({
                    'channel_name': channel_name,
                    'sql': sql,
                    'fmt': fmt,
                    'path': path,
                    'output_options': out_cfg.output_options
                })

        # After processing all channels, append failed-records job if configured
        self._add_failed_records_job(eligibility_engine, gen)

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

        total_rows_written = 0
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

                # Force *all* data to string type to preserve formatting (no numeric coercion)
                df = df.astype(str)

                # accumulate rows for aggregate logging
                total_rows_written += len(df)

                out_dir = os.path.dirname(job['path'])
                os.makedirs(out_dir, exist_ok=True)
                from tlptaco.utils.fs import grant_group_rwx
                grant_group_rwx(out_dir)
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

        # Aggregate summary
        try:
            num_channels = len(self._output_jobs)
            self.logger.info(
                f"Output stage finished: {num_channels} channels, {total_rows_written:,} total rows written"
            )
        except Exception:
            pass


    # ------------------------------------------------------------------
    # Failed Records helper
    # ------------------------------------------------------------------

    def _add_failed_records_job(self, eligibility_engine, gen):
        """Internal helper to build the failed_records job when enabled."""
        if not self.cfg.failed_records or not self.cfg.failed_records.enabled:
            return

        elig_cfg = eligibility_engine.cfg

        # --- gather identifier columns ---------------------------------------------------
        id_cols: list[str] = []
        for chan_cfg in self.cfg.channels.values():
            if chan_cfg.unique_on:
                for col in chan_cfg.unique_on:
                    if col not in id_cols:
                        id_cols.append(col)
        if not id_cols:
            # fallback to eligibility unique identifiers
            id_cols = elig_cfg.unique_identifiers

        # --- gather *all* flag checks ----------------------------------------------------
        all_checks = []
        all_checks.extend(elig_cfg.conditions.main.BA)
        for seg_chk in elig_cfg.conditions.main.segments.values():
            all_checks.extend(seg_chk)
        for ch_cfg in elig_cfg.conditions.channels.values():
            all_checks.extend(ch_cfg.BA)
            for seg_checks in ch_cfg.segments.values():
                all_checks.extend(seg_checks)

        # Preserve order & assign rank
        unique_checks: dict[str, Any] = {}
        for chk in all_checks:
            unique_checks.setdefault(chk.name, chk)

        failed_flag_checks = []
        rank_counter = 1
        for chk_name, chk in unique_checks.items():
            failed_flag_checks.append({'name': chk.name, 'sql': chk.sql, 'rank': rank_counter})
            rank_counter += 1

        # --- build unbucketed channels ---------------------------------------------------
        def _cond(check_list):
            if not check_list:
                return '1=1'
            return ' AND '.join([f"c.{ck.name} = 1" for ck in check_list])

        main_ba_cond = _cond(elig_cfg.conditions.main.BA)

        unbucketed_channels = []

        for ch_name, ch_cfg in elig_cfg.conditions.channels.items():
            channel_ba_cond = _cond(ch_cfg.BA)
            pre_filter = f"({main_ba_cond}) AND ({channel_ba_cond})" if channel_ba_cond != '1=1' else main_ba_cond

            # collect segment conditions
            seg_conditions = []
            for seg_checks in ch_cfg.others.values():
                seg_conditions.append(' AND '.join([f"c.{ck.name} = 1" for ck in seg_checks]))

            if seg_conditions:
                bucket_or = ' OR '.join([f"({sc})" for sc in seg_conditions])
                no_template_cond = f"NOT ({bucket_or})"
            else:
                # No segments defined – everything that passes BA but hasn't been bucketed is effectively all rows
                no_template_cond = '1=1'

            unbucketed_channels.append({
                'channel': ch_name,
                'pre_filter': pre_filter,
                'no_template_condition': no_template_cond,
                'rank': rank_counter
            })
            rank_counter += 1

        # --- Render SQL ------------------------------------------------------------------
        context = {
            'eligibility_table': elig_cfg.eligibility_table,
            'unique_identifiers': id_cols,
            'failed_flag_checks': failed_flag_checks,
            'unbucketed_channels': unbucketed_channels,
            'first_reason_only': self.cfg.failed_records.first_reason_only,
        }

        sql_failed = gen.render('failed_records.sql.j2', context)

        from tlptaco.utils.logging import log_sql_section
        log_sql_section('Failed Records', sql_failed)

        # Determine output path or table
        fmt = self.cfg.failed_records.output_options.format.lower()
        job = {
            'channel_name': 'failed_records',
            'sql': sql_failed,
            'fmt': fmt,
            'output_options': self.cfg.failed_records.output_options,
        }

        if fmt == 'table':
            full_table = f"{self.cfg.failed_records.file_location}.{self.cfg.failed_records.file_base_name}"
            job['table_name'] = full_table
        else:
            ext = 'xlsx' if fmt == 'excel' else fmt
            path = os.path.join(
                self.cfg.failed_records.file_location,
                f"{self._append_today(self.cfg.failed_records.file_base_name)}.{ext}"
            )
            job['path'] = path

        self._output_jobs.append(job)