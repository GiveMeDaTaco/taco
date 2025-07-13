"""
Waterfall engine: computes waterfall metrics from the smart eligibility table.
"""
from tlptaco.config.schema import WaterfallConfig, EligibilityConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.sql.generator import SQLGenerator
from tlptaco.iostream.writer import write_dataframe
import os
import pandas as pd

class WaterfallEngine:
    def __init__(self, cfg: WaterfallConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("waterfall")

    def _pivot_waterfall_df(self, df, section_name):
        """Pivots the long-format waterfall data into a wide-format DataFrame."""
        # Filter for the detailed metrics, excluding summary rows
        metric_df = df[df['stat_name'] != 'Records Claimed']
        if metric_df.empty:
            return pd.DataFrame() # Return empty if no detail rows

        pivoted = metric_df.pivot_table(
            index='check_name',
            columns='stat_name',
            values='value'
        ).reset_index()
        pivoted['section'] = section_name
        return pivoted

    def run(self, eligibility_engine, progress=None):
        """
        Orchestrates the multi-part waterfall report, using specialized templates
        for standard and "Claim and Exclude" logic.
        """
        elig_cfg = eligibility_engine.cfg
        groups = []
        for item in self.cfg.count_columns:
            raw_cols = [item] if isinstance(item, str) else list(item)
            grp_name = '_'.join([col.split('.')[-1] for col in raw_cols])
            cols = [f"c.{col.split('.')[-1]}" for col in raw_cols]
            groups.append({'name': grp_name, 'cols': cols})

        templates_dir = os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates')
        gen = SQLGenerator(templates_dir)
        out_dir = self.cfg.output_directory
        os.makedirs(out_dir, exist_ok=True)

        for grp in groups:
            name, uniq_ids = grp['name'], grp['cols']
            self.logger.info(f"Processing waterfall grouping '{name}'")

            all_report_sections = []

            try:
                # --- SECTION 1: MAIN/BASE WATERFALL (Standard Logic) ---
                main_ba_checks = [chk.name for chk in elig_cfg.conditions.main.BA]
                ctx_main = {'eligibility_table': elig_cfg.eligibility_table, 'unique_identifiers': uniq_ids, 'check_columns': main_ba_checks, 'pre_filter': None}
                sql_main = gen.render('waterfall_full.sql.j2', ctx_main)
                df_main_raw = self.runner.to_df(sql_main)
                df_main = self._pivot_waterfall_df(df_main_raw, 'Base')
                all_report_sections.append(df_main)

                # --- SECTION 2: PER-CHANNEL WATERFALLS ---
                for channel_name, channel_cfg in elig_cfg.conditions.channels.items():
                    # -- Channel BA Waterfall (Standard Logic) --
                    base_filter = "c.passed_all_main_BA = 1"
                    channel_ba_checks = [chk.name for chk in channel_cfg.BA]
                    ctx_chan_ba = {'eligibility_table': elig_cfg.eligibility_table, 'unique_identifiers': uniq_ids, 'check_columns': channel_ba_checks, 'pre_filter': base_filter}
                    sql_chan_ba = gen.render('waterfall_full.sql.j2', ctx_chan_ba)
                    df_chan_ba_raw = self.runner.to_df(sql_chan_ba)
                    df_chan_ba = self._pivot_waterfall_df(df_chan_ba_raw, f'{channel_name} - BA')
                    all_report_sections.append(df_chan_ba)

                    # -- Channel 'Other Segments' ("Claim and Exclude" Logic) --
                    if channel_cfg.others:
                        segment_base_filter = f"{base_filter} AND c.passed_all_{channel_name}_BA = 1"

                        # Prepare the list of segment objects for the new template
                        segments_to_process = []
                        for segment_name, segment_checks_list in sorted(channel_cfg.others.items()):
                            segments_to_process.append({
                                'name': f'{channel_name} - {segment_name}',
                                'checks': [chk.name for chk in segment_checks_list],
                                'summary_column': f'passed_all_{channel_name}_{segment_name}'
                            })

                        ctx_segments = {
                            'eligibility_table': elig_cfg.eligibility_table,
                            'unique_identifiers': uniq_ids,
                            'pre_filter': segment_base_filter,
                            'segments': segments_to_process
                        }
                        # Call the new, specialized template
                        sql_segments = gen.render('waterfall_segments.sql.j2', ctx_segments)
                        df_segments_raw = self.runner.to_df(sql_segments)

                        # Separate the summary rows from the detailed rows and process them
                        summary_rows = df_segments_raw[df_segments_raw['stat_name'] == 'Records Claimed'].copy()
                        detail_rows = df_segments_raw[df_segments_raw['stat_name'] != 'Records Claimed'].copy()

                        # Pivot the detailed data section by section
                        for section_name in detail_rows['section'].unique():
                            section_df = self._pivot_waterfall_df(detail_rows[detail_rows['section'] == section_name], section_name)
                            all_report_sections.append(section_df)

                        # Add the summary rows (unpivoted)
                        all_report_sections.append(summary_rows[['section', 'stat_name', 'value']])

                # --- SECTION 3: COMBINE AND WRITE FINAL REPORT ---
                if all_report_sections:
                    final_df = pd.concat(all_report_sections, ignore_index=True)
                    out_path = os.path.join(out_dir, f"waterfall_report_{elig_cfg.eligibility_table}_{name}.xlsx")
                    # Replace with your formatted Excel writer if desired
                    final_df.to_excel(out_path, index=False)
                    self.logger.info(f"Waterfall report for '{name}' saved to {out_path}")

            except Exception as e:
                self.logger.exception(f"Waterfall grouping '{name}' failed: {e}")
            finally:
                if progress:
                    progress.update('Waterfall')