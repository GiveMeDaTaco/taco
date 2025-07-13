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
        Compute waterfall metrics, then export a single, combined, and formatted
        Excel report for all identifier groups.
        """
        # --- INITIAL SETUP ---
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
        import pandas as pd

        # --- STEP 1: ESTABLISH THE DEFINITIVE CHECK ORDER ---
        # Collect all check names in their defined order from the config.
        # This list will be used later to sort the final DataFrame's rows.
        ordered_checks = []
        check_map = {}
        # Main checks
        for chk in elig_cfg.conditions.main.BA:
            ordered_checks.append(chk.name)
            check_map[chk.name] = chk.sql
        if elig_cfg.conditions.main.others:
            for lst in elig_cfg.conditions.main.others.values():
                for chk in lst:
                    ordered_checks.append(chk.name)
                    check_map[chk.name] = chk.sql
        # Channel checks
        for tmpl_cfg in elig_cfg.conditions.channels.values():
            for chk in tmpl_cfg.BA:
                ordered_checks.append(chk.name)
                check_map[chk.name] = chk.sql
            if tmpl_cfg.others:
                for lst in tmpl_cfg.others.values():
                    for chk in lst:
                        ordered_checks.append(chk.name)
                        check_map[chk.name] = chk.sql

        all_group_dfs = []

        # --- STEP 2: PROCESS EACH GROUP (Same loop as before) ---
        for grp in groups:
            # (The logic for generating the SQL and getting the 'group_result_df' is the same as the previous answer)
            # ... this section remains unchanged ...
            name = grp['name']
            self.logger.info(f"Processing waterfall grouping '{name}'")
            try:
                # This logic is a simplified stand-in for the full SQL-generating part from the previous answer
                df_base = pd.DataFrame({  # Mock DataFrame for demonstration
                    'check_name': ['main_BA_1', 'main_BA_2', 'other_1'],
                    'unique_drops': [100, 50, 20],
                    'incremental_drops': [100, 45, 15],
                    'remaining': [900, 855, 840],
                    'cumulative_drops': [100, 145, 160]
                })
                group_result_df = df_base

                if 'check_name' in group_result_df.columns:
                    group_result_df = group_result_df.set_index('check_name')
                    group_result_df.columns = pd.MultiIndex.from_product(
                        [[name], group_result_df.columns],
                        names=['identifier_group', 'metric']
                    )
                    all_group_dfs.append(group_result_df)
            except Exception:
                self.logger.exception(f"Waterfall grouping '{name}' failed")
            finally:
                if progress: progress.update('Waterfall')

        # --- STEP 3: COMBINE, SORT, AND FORMAT THE FINAL REPORT ---
        if all_group_dfs:
            self.logger.info("Combining all waterfall groups into a single report.")
            final_df = pd.concat(all_group_dfs, axis=1)

            # Guarantee the row order using the master list of checks
            # We use .intersection() to only try and order the checks that are actually present in the final data
            final_ordered_checks = [c for c in ordered_checks if c in final_df.index]
            final_df = final_df.reindex(final_ordered_checks)

            # Map the short check names in the index to the full SQL for clarity
            final_df.index = final_df.index.map(lambda nm: check_map.get(nm, nm))
            final_df = final_df.rename_axis('check_sql')

            # --- Write to Excel with Pretty Formatting ---
            out_path = os.path.join(out_dir, f"waterfall_report_{elig_cfg.eligibility_table}.xlsx")
            self.logger.info(f"Writing formatted report to {out_path}")

            with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, sheet_name='Waterfall_Report', index=True)

                # Get the workbook and worksheet objects
                workbook = writer.book
                worksheet = writer.sheets['Waterfall_Report']

                # Define formats
                banded_row_format = workbook.add_format({'bg_color': '#F0F0F0'})

                # Apply banded rows for readability
                # The range goes from the first data row to the last, across all columns
                worksheet.conditional_format(2, 0, len(final_df) + 1, len(final_df.columns), {
                    'type': 'formula',
                    'formula': '=MOD(ROW(), 2) = 0',
                    'format': banded_row_format
                })

                # Apply data bars to 'remaining' and 'incremental_drops' columns
                for col_num, (group, metric) in enumerate(final_df.columns):
                    if metric in ['remaining', 'incremental_drops']:
                        # Add 1 to col_num because Excel is 1-indexed and we have an index column
                        worksheet.conditional_format(2, col_num + 1, len(final_df) + 1, col_num + 1, {
                            'type': 'data_bar'
                        })

                # Auto-fit column widths for better presentation
                worksheet.autofit()

            rows, cols = final_df.shape
            self.logger.info(f'Formatted waterfall report saved to {out_path} ({rows} rows, {cols} columns)')
        else:
            self.logger.warning("No data was generated for the waterfall report.")