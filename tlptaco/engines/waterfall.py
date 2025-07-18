"""
Waterfall engine: computes waterfall metrics from the smart eligibility table.
"""
from tlptaco.config.schema import WaterfallConfig, EligibilityConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.sql.generator import SQLGenerator
import os
import pandas as pd
from datetime import datetime


class WaterfallEngine:
    def __init__(self, cfg: WaterfallConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("waterfall")
        # Metadata (to be set by CLI)
        self.offer_code: str = ''
        self.campaign_planner: str = ''
        self.lead: str = ''
        # Cache for prepared steps and the eligibility engine
        self._waterfall_groups = None
        self._eligibility_engine = None

    def _prepare_waterfall_steps(self, eligibility_engine):
        """
        Prepares all the groups and SQL generation steps without executing them.
        The results are cached to avoid redundant work.
        """
        self._eligibility_engine = eligibility_engine

        if self._waterfall_groups is not None:
            self.logger.info("Using cached waterfall steps.")
            return

        self.logger.info("No cached steps found. Preparing waterfall groups and SQL.")
        self._waterfall_groups = []
        elig_cfg: EligibilityConfig = eligibility_engine.cfg

        # 1. Determine the grouping columns
        groups = []
        for item in self.cfg.count_columns:
            raw_cols = [item] if isinstance(item, str) else list(item)
            grp_name = '_'.join([col.split('.')[-1] for col in raw_cols])
            cols = [f"c.{col.split('.')[-1]}" for col in raw_cols]
            groups.append({'name': grp_name, 'cols': cols})

        templates_dir = os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates')
        gen = SQLGenerator(templates_dir)

        def create_sql_condition(check_list, operator='AND'):
            """Helper function to create a combined SQL condition."""
            if not check_list:
                return "1=1"
            op = f" {operator.strip()} "
            conditions = [f"c.{check.name} = 1" for check in check_list]
            return f"({op.join(conditions)})"

        # 2. For each group, prepare the SQL and metadata for each report section
        for grp in groups:
            name, uniq_ids = grp['name'], grp['cols']
            sql_jobs = []

            # --- SECTION 1: MAIN/BASE WATERFALL ---
            main_ba_checks = [chk.name for chk in elig_cfg.conditions.main.BA]
            ctx_main = {
                'eligibility_table': elig_cfg.eligibility_table,
                'unique_identifiers': uniq_ids,
                'check_columns': main_ba_checks,
                'pre_filter': None,
                'segments': []  # no non-BA segments in base waterfall
            }
            sql_main = gen.render('waterfall_full.sql.j2', ctx_main)
            sql_jobs.append({'type': 'standard', 'sql': sql_main, 'section_name': 'Base'})

            # --- SECTION 2: PER-CHANNEL WATERFALLS ---
            for channel_name, channel_cfg in elig_cfg.conditions.channels.items():
                # Prepare per-channel non-BA segments list for Regain logic
                segments_to_process = []
                # Base filter: passed all main BA checks
                base_filter = create_sql_condition(elig_cfg.conditions.main.BA)

                # Channel BA checks
                channel_ba_checks_list = channel_cfg.BA
                channel_ba_check_names = [chk.name for chk in channel_ba_checks_list]
                # CHANNEL BA WATERFALL
                if channel_ba_check_names:
                    ctx_chan_ba = {
                        'eligibility_table': elig_cfg.eligibility_table,
                        'unique_identifiers': uniq_ids,
                        'check_columns': channel_ba_check_names,
                        'pre_filter': base_filter,
                        'segments': segments_to_process
                    }
                    sql_chan_ba = gen.render('waterfall_full.sql.j2', ctx_chan_ba)
                    sql_jobs.append({'type': 'standard', 'sql': sql_chan_ba, 'section_name': f'{channel_name} - BA'})

                # CHANNEL non-BA segments
                if channel_cfg.others:
                    channel_ba_condition = create_sql_condition(channel_ba_checks_list)
                    segment_base_filter = f"{base_filter} AND {channel_ba_condition}"

                    for s_name, s_checks in sorted(channel_cfg.others.items()):
                        segment_condition = create_sql_condition(s_checks, operator='OR')
                        segments_to_process.append({
                            'name': f'{channel_name} - {s_name}',
                            'checks': [c.name for c in s_checks],
                            'summary_column': segment_condition
                        })

                    ctx_segments = {
                        'eligibility_table': elig_cfg.eligibility_table,
                        'unique_identifiers': uniq_ids,
                        'pre_filter': segment_base_filter,
                        'segments': segments_to_process
                    }
                    sql_segments = gen.render('waterfall_segments.sql.j2', ctx_segments)
                    sql_jobs.append({'type': 'segments', 'sql': sql_segments})

            out_path = os.path.join(self.cfg.output_directory,
                                    f"waterfall_report_{elig_cfg.eligibility_table}_{name}.xlsx")
            self._waterfall_groups.append({'name': name, 'jobs': sql_jobs, 'output_path': out_path})

    def num_steps(self, eligibility_engine) -> int:
        """
        Calculates the total number of waterfall reports (groups) to be generated.
        Caches the eligibility_engine for the run() method.
        """
        self.logger.info("Calculating the number of waterfall steps.")
        self._prepare_waterfall_steps(eligibility_engine)
        total_steps = len(self._waterfall_groups)
        self.logger.info(f"Calculation complete: {total_steps} steps (reports).")
        return total_steps

    def _pivot_waterfall_df(self, df, section_name):
        """Pivots the long-format waterfall data into a wide-format DataFrame."""
        metric_df = df[df['stat_name'] != 'Records Claimed']
        if metric_df.empty:
            return pd.DataFrame()
        pivoted = metric_df.pivot_table(index='check_name', columns='stat_name', values='cntr').reset_index()
        pivoted['section'] = section_name
        return pivoted

    def run(self, eligibility_engine=None, progress=None):
        """
        Orchestrates the waterfall report. The eligibility_engine is optional
        if it was already provided in a prior call to num_steps().
        """
        # Determine which eligibility engine to use
        engine_to_use = eligibility_engine or self._eligibility_engine

        # If no engine is available from either the argument or the cache, raise an error.
        if not engine_to_use:
            raise ValueError(
                "An eligibility_engine instance must be provided either to run() or to a prior num_steps() call.")

        self._prepare_waterfall_steps(engine_to_use)
        os.makedirs(self.cfg.output_directory, exist_ok=True)

        for group in self._waterfall_groups:
            self.logger.info(f"Processing waterfall grouping '{group['name']}'")
            all_report_sections = []
            try:
                for job in group['jobs']:
                    # Fetch raw results and normalize metric column ('cntr' vs. legacy 'value')
                    df_raw = self.runner.to_df(job['sql'])
                    if 'cntr' not in df_raw.columns and 'value' in df_raw.columns:
                        df_raw = df_raw.rename(columns={'value': 'cntr'})

                    if job['type'] == 'standard':
                        df_pivoted = self._pivot_waterfall_df(df_raw, job['section_name'])
                        all_report_sections.append(df_pivoted)

                    elif job['type'] == 'segments':
                        summary_rows = df_raw[df_raw['stat_name'] == 'Records Claimed'].copy()
                        detail_rows = df_raw[df_raw['stat_name'] != 'Records Claimed'].copy()
                        for section_name in detail_rows['section'].unique():
                            section_df = self._pivot_waterfall_df(detail_rows[detail_rows['section'] == section_name],
                                                                  section_name)
                            all_report_sections.append(section_df)
                        all_report_sections.append(summary_rows[['section', 'stat_name', 'cntr']])

                if all_report_sections:
                    # Combine all sections into a mapping for Excel writer
                    # Preserve ordering of sections, including summary vs detail
                    compiled = []  # list of (section_name, DataFrame)
                    for df in all_report_sections:
                        sec = df['section'].iat[0]
                        section_df = df.drop(columns='section').reset_index(drop=True)
                        compiled.append((sec, section_df))

                    # Build conditions DataFrame
                    conds = engine_to_use.cfg.conditions
                    cond_rows = []
                    # main BA
                    for chk in conds.main.BA:
                        cond_rows.append({'check_name': chk.name, 'sql': chk.sql, 'description': chk.description})
                    # channel BA and segments
                    for chname, chcfg in conds.channels.items():
                        for chk in chcfg.BA:
                            cond_rows.append({'check_name': chk.name, 'sql': chk.sql, 'description': chk.description})
                        for seg_checks in chcfg.segments.values():
                            for chk in seg_checks:
                                cond_rows.append({'check_name': chk.name, 'sql': chk.sql, 'description': chk.description})
                    conditions_df = pd.DataFrame(cond_rows).set_index('check_name')

                    # Write Excel with waterfall formatting
                    import tlptaco.engines.waterfall_excel as wf_excel_mod
                    now = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
                    wf_excel_mod.write_waterfall_excel(
                        conditions_df,
                        compiled,
                        group['output_path'],
                        group['name'],
                        self.offer_code,
                        self.campaign_planner,
                        self.lead,
                        now
                    )
                    self.logger.info(f"Waterfall report for '{group['name']}' saved to {group['output_path']}")

            except Exception as e:
                self.logger.exception(f"Waterfall grouping '{group['name']}' failed: {e}")
            finally:
                if progress:
                    progress.update('Waterfall')