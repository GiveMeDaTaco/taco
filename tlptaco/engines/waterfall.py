"""
Waterfall engine: computes waterfall metrics from eligibility table.
"""
from tlptaco.config.schema import WaterfallConfig, EligibilityConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.sql.generator import SQLGenerator
from tlptaco.iostream.writer import write_dataframe
import os

class WaterfallEngine:
    def __init__(self, cfg: WaterfallConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("waterfall")

    def num_steps(self) -> int:
        """Return the number of grouping tasks that run() will execute."""
        return len(self.cfg.count_columns)

    def _extract_check_columns(self, conditions_cfg) -> list:
        """
        Flatten and sort all check names from conditions config.
        """
        cols = []
        # main BA
        for chk in conditions_cfg.main.BA:
            cols.append(chk.name)
        # main others
        if conditions_cfg.main.others:
            for templ, checks in conditions_cfg.main.others.items():
                for chk in checks:
                    cols.append(chk.name)
        # channel templates
        for tmpl_cfg in conditions_cfg.channels.values():
            for chk in tmpl_cfg.BA:
                cols.append(chk.name)
            if tmpl_cfg.others:
                for templ, checks in tmpl_cfg.others.items():
                    for chk in checks:
                        cols.append(chk.name)
        # sort by trailing numeric
        return sorted(cols, key=lambda x: int(x.rsplit('_', 1)[-1]))

    def run(self, eligibility_engine, progress=None):
        """
        Compute waterfall metrics entirely in-database, then export report.
        """
        # Compute waterfall for each group of identifier columns
        elig_cfg = eligibility_engine.cfg  # type: EligibilityConfig
        # Build grouping definitions (map config columns to eligibility table alias 'c')
        groups = []
        for item in self.cfg.count_columns:
            raw_cols = [item] if isinstance(item, str) else list(item)
            # Name for this grouping (concatenate base column names)
            grp_name = '_'.join([col.split('.')[-1] for col in raw_cols])
            # In waterfall CTE, always reference eligibility table alias 'c'
            cols = [f"c.{col.split('.')[-1]}" for col in raw_cols]
            groups.append({'name': grp_name, 'cols': cols})
        # Precompute check column order
        check_cols = self._extract_check_columns(elig_cfg.conditions)
        # Template generator
        templates_dir = os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates')
        gen = SQLGenerator(templates_dir)
        # Ensure output directory exists
        out_dir = self.cfg.output_directory
        os.makedirs(out_dir, exist_ok=True)

        import pandas as pd
        # Build list of base checks
        base_checks = [chk.name for chk in elig_cfg.conditions.main.BA]
        if elig_cfg.conditions.main.others:
            for lst in elig_cfg.conditions.main.others.values():
                for chk in lst:
                    base_checks.append(chk.name)
        # Build mapping of check names to SQL logic for labels
        check_map = {}
        # main BA checks
        for chk in elig_cfg.conditions.main.BA:
            check_map[chk.name] = chk.sql
        # main others
        if elig_cfg.conditions.main.others:
            for lst in elig_cfg.conditions.main.others.values():
                for chk in lst:
                    check_map[chk.name] = chk.sql
        # channel checks
        for tmpl_cfg in elig_cfg.conditions.channels.values():
            for chk in tmpl_cfg.BA:
                check_map[chk.name] = chk.sql
            if tmpl_cfg.others:
                for lst in tmpl_cfg.others.values():
                    for chk in lst:
                        check_map[chk.name] = chk.sql
        # Loop through each grouping of identifiers
        for grp in groups:
            name = grp['name']
            uniq_ids = grp['cols']
            self.logger.info(f"Starting waterfall grouping '{name}'")
            try:
                # Collect DataFrames for each section
                df_sections = []
                # 1) Base waterfall
                ctx_base = {
                    'eligibility_table': elig_cfg.eligibility_table,
                    'unique_identifiers': uniq_ids,
                    'unique_without_aliases': [u.split('.')[-1] for u in uniq_ids],
                    'check_columns': base_checks,
                    'pre_filter': None
                }
                df_base = self.runner.to_df(gen.render('waterfall_full.sql.j2', ctx_base))
                # Normalize column names to lowercase for consistent pivoting
                df_base.columns = [c.lower() for c in df_base.columns]
                # Pivot base metrics if stat_name column present
                if 'stat_name' in df_base.columns:
                    df_base = df_base.set_index('stat_name').T.reset_index().rename(columns={'index': 'check_name'})
                df_base['section'] = 'base'
                df_sections.append(df_base)
                # 2) Per-channel waterfalls
                for channel, tmpl_cfg in elig_cfg.conditions.channels.items():
                    chks = [chk.name for chk in tmpl_cfg.BA] + [c.name for lst in (tmpl_cfg.others or {}).values() for c in lst]
                    pf = ' AND '.join([f"c.{c}=1" for c in base_checks])
                    df_ch = self.runner.to_df(gen.render('waterfall_full.sql.j2', {
                        'eligibility_table': elig_cfg.eligibility_table,
                        'unique_identifiers': uniq_ids,
                        'unique_without_aliases': [u.split('.')[-1] for u in uniq_ids],
                        'check_columns': chks,
                        'pre_filter': pf
                    }))
                    # Normalize column names to lowercase and pivot channel metrics
                    df_ch.columns = [c.lower() for c in df_ch.columns]
                    if 'stat_name' in df_ch.columns:
                        df_ch = df_ch.set_index('stat_name').T.reset_index().rename(columns={'index': 'check_name'})
                    df_ch['section'] = channel
                    df_sections.append(df_ch)
                # Concatenate sections and map check_name to SQL
                result_df = pd.concat(df_sections, ignore_index=True)
                if 'check_name' in result_df.columns:
                    result_df['check_name'] = result_df['check_name'].map(lambda nm: check_map.get(nm, nm))
                cols_new = ['check_name', 'unique_drops', 'incremental_drops', 'remaining', 'cumulative_drops', 'section']
                if len(result_df.columns) == len(cols_new):
                    result_df.columns = cols_new
                else:
                    self.logger.warning(f"Expected {len(cols_new)} columns but got {len(result_df.columns)}; skipping rename")
                # Write report
                out_path = os.path.join(out_dir, f"waterfall_{elig_cfg.eligibility_table}_{name}.xlsx")
                write_dataframe(result_df, out_path, fmt='excel')
                rows, cols = result_df.shape
                self.logger.info(f'Waterfall report saved to {out_path} ({rows} rows, {cols} columns)')
            except Exception:
                self.logger.exception(f"Waterfall grouping '{name}' failed")
            finally:
                if progress:
                    progress.update('Waterfall')
                self.logger.info(f"Finished waterfall grouping '{name}'")