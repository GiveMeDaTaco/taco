"""
Waterfall engine: computes waterfall metrics from the smart eligibility table.
"""
# Allow | union type hints on Python ≤3.10
from __future__ import annotations
from tlptaco.config.schema import WaterfallConfig, EligibilityConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.sql.generator import SQLGenerator
import os
import pandas as pd
from datetime import datetime


class WaterfallEngine:
    """Generate grouped *waterfall* metrics and Excel reports.

    The engine expects an *already executed* EligibilityEngine instance so
    that the smart table exists with all flag columns.

    Workflow
    --------
    1. Build SQL jobs for each *group* declared in
       ``waterfall.count_columns``.
    2. Execute those jobs, pivot results, and store in-memory DataFrames.
    3. Feed the data to :pyfunc:`tlptaco.engines.waterfall_excel.write_waterfall_excel`
       for a consolidated workbook (plus per-group sheets).

    Example
    -------
    >>> wf_engine = WaterfallEngine(app_cfg.waterfall, runner)
    >>> wf_engine.run(elig_engine)   # writes Excel under cfg.output_directory
    """
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
        # Name of the session-local volatile base table once created
        self._base_table: str | None = None

    # ------------------------------------------------------------------
    # Internal helper – build *one* volatile, deduplicated base table
    # ------------------------------------------------------------------

    def _create_volatile_base(self, eligibility_engine):
        """Create a deduplicated VOLATILE table that holds only the columns
        required by all subsequent waterfall queries.

        The table is created **once per run** and dropped automatically
        when the session ends.  When the method has already been executed
        (``self._base_table`` not ``None``) it becomes a no-op so that
        :py:meth:`run` can call it idempotently.
        """

        if self._base_table is not None:
            # Already prepared in this session
            return

        self._base_table = 'vt_wf_base'

        elig_cfg: EligibilityConfig = eligibility_engine.cfg

        # Use session-local volatile table when available; fall back to the
        # original eligibility table when the volatile table has not yet
        # been created (e.g. during unit tests that call _prepare_* directly).
        table_name = self._base_table or elig_cfg.eligibility_table
        # Use the session-local deduplicated volatile table for all queries
        table_name = self._base_table or elig_cfg.eligibility_table

        # 1. Determine *all* flag columns referenced anywhere
        flag_cols: list[str] = []

        def _add_checks(checks):
            for chk in checks:
                if chk.name not in flag_cols:
                    flag_cols.append(chk.name)

        conds = elig_cfg.conditions
        _add_checks(conds.main.BA)
        for seg_checks in conds.main.segments.values():
            _add_checks(seg_checks)
        for ch_cfg in conds.channels.values():
            _add_checks(ch_cfg.BA)
            for seg_checks in ch_cfg.segments.values():
                _add_checks(seg_checks)

        # 2. Build list of unique-identifier columns **with aliases** so the
        #    volatile table uses simple names (no schema/alias prefixes).
        uid_cols_sql: list[str] = []  # expressions in SELECT list
        uid_cols_pi: list[str] = []   # plain names for PRIMARY INDEX
        for uid in elig_cfg.unique_identifiers:
            if '.' in uid:
                plain = uid.split('.')[-1]
                uid_cols_sql.append(f"{uid} AS {plain}")
                uid_cols_pi.append(plain)
            else:
                uid_cols_sql.append(uid)
                uid_cols_pi.append(uid)

        # 3. Flag columns as simple names (smart table already has them)
        flag_cols_sql = ', '.join(flag_cols)

        # 4. Build expressions for pass_cnt and streak_len (needed for
        #    deduplication ranking)
        pass_cnt_expr = ' + '.join(flag_cols) if flag_cols else '0'
        # streak: sum of cumulative products
        streak_parts: list[str] = []
        for i in range(len(flag_cols)):
            inner = ' * '.join(flag_cols[: i + 1])
            streak_parts.append(inner)
        streak_expr = ' + '.join(streak_parts) if streak_parts else '0'

        # 5. Render SQL from template -------------------------------------------------
        select_cols_alias = uid_cols_sql + flag_cols
        select_cols_inner = uid_cols_sql + flag_cols + [
            f"{pass_cnt_expr} AS pass_cnt",
            f"{streak_expr} AS streak_len",
            (
                "ROW_NUMBER() OVER (PARTITION BY "
                + ', '.join(uid_cols_pi)
                + " ORDER BY pass_cnt DESC, streak_len DESC) AS _rn"
            ),
        ]

        tmpl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(tmpl_dir)
        context = {
            'base_table': self._base_table,
            'eligibility_table': elig_cfg.eligibility_table,
            'select_cols_alias': select_cols_alias,
            'select_cols_inner': select_cols_inner,
            'uid_cols_pi': uid_cols_pi,
            'collect_pi_cols': uid_cols_pi[:3],
            'collect_flag_cols': flag_cols[:10],
        }

        sql_script = gen.render('waterfall_base_table.sql.j2', context)

        # Log rendered SQL for visibility/copy-paste
        from tlptaco.utils.logging import log_sql_section
        log_sql_section('Waterfall BaseTable', sql_script)

        # Split into individual statements on semicolon but keep order
        sql_statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

        # Execute all statements; ignore DROP failures
        for stmt in sql_statements:
            try:
                self.runner.run(stmt)
            except Exception as ex:
                if 'DROP TABLE' in stmt:
                    # VT likely didn't exist – fine
                    continue
                raise

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

        # Determine which physical table the templates should query
        table_name = self._base_table or elig_cfg.eligibility_table

        # 1. Determine the grouping columns
        groups = []
        for item in self.cfg.count_columns:
            raw_cols = [item] if isinstance(item, str) else list(item)
            grp_name = '_'.join([col.split('.')[-1] for col in raw_cols])
            cols = [f"c.{col.split('.')[-1]}" for col in raw_cols]
            groups.append({'name': grp_name, 'cols': cols, 'raw_cols': [col.split('.')[-1] for col in raw_cols]})

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
            # Store raw column names for future index creation
            grp['base_cols'] = [c.split('.')[-1] for c in uniq_ids]
            sql_jobs = []

            # --- SECTION 1: MAIN/BASE WATERFALL ---
            main_ba_checks = [chk.name for chk in elig_cfg.conditions.main.BA]
            # Base waterfall (main BA) has no bucketable filter
            ctx_main = {
                'eligibility_table': table_name,
                'unique_identifiers': uniq_ids,
                'check_columns': main_ba_checks,
                'aux_columns': [],
                'pre_filter': None,
                'segments': [],  # not used in full template
                'bucketable_condition': None
            }

            sql_main = gen.render('waterfall_full.sql.j2', ctx_main)
            from tlptaco.utils.logging import log_sql_section
            log_sql_section(f'Waterfall {name} - Base', sql_main)
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
                    # Build OR-list of segment summary conditions for bucketable filter
                    if channel_cfg.others:
                        # Preserve original segment order as defined in YAML
                        seg_conds = [create_sql_condition(s_checks) for _, s_checks in channel_cfg.others.items()]
                        bucketable = ' OR '.join([f'({c})' for c in seg_conds])

                        # Collect additional flag columns referenced by bucketable filter
                        aux_cols: list[str] = []
                        for _, s_checks in channel_cfg.others.items():
                            aux_cols.extend([chk.name for chk in s_checks])
                        aux_cols = [c for c in aux_cols if c not in channel_ba_check_names]
                    else:
                        bucketable = None
                        aux_cols = []

                    ctx_chan_ba = {
                        'eligibility_table': table_name,
                        'unique_identifiers': uniq_ids,
                        'check_columns': channel_ba_check_names,
                        'aux_columns': aux_cols,
                        'pre_filter': base_filter,
                        'segments': segments_to_process,
                        'bucketable_condition': bucketable
                    }
                    sql_chan_ba = gen.render('waterfall_full.sql.j2', ctx_chan_ba)
                    sql_jobs.append({'type': 'standard', 'sql': sql_chan_ba, 'section_name': f'{channel_name} - BA'})
                    log_sql_section(f'Waterfall {name} - {channel_name} BA', sql_chan_ba)

                # CHANNEL non-BA segments
                if channel_cfg.others:
                    channel_ba_condition = create_sql_condition(channel_ba_checks_list)
                    segment_base_filter = f"{base_filter} AND {channel_ba_condition}"

                    # For each non-BA segment, prepare summary and detailed SQL
                    for s_name, s_checks in channel_cfg.others.items():
                        # Summary condition: pass all checks in this segment
                        segment_condition = create_sql_condition(s_checks)
                        segments_to_process.append({
                            'name': f'{channel_name} - {s_name}',
                            'checks': [c.name for c in s_checks],
                            'summary_column': segment_condition
                        })

                    ctx_segments = {
                        'eligibility_table': table_name,
                        'unique_identifiers': uniq_ids,
                        'pre_filter': segment_base_filter,
                        'segments': segments_to_process
                    }
                    sql_segments = gen.render('waterfall_segments.sql.j2', ctx_segments)
                    sql_jobs.append({'type': 'segments', 'sql': sql_segments})
                    log_sql_section(f'Waterfall {name} - {channel_name} Segments', sql_segments)

            out_path = os.path.join(self.cfg.output_directory,
                                    f"waterfall_report_{elig_cfg.eligibility_table}_{name}.xlsx")

            self._waterfall_groups.append({'name': name,
                                           'jobs': sql_jobs,
                                           'output_path': out_path,
                                           'raw_cols': grp['raw_cols']})

        # No secondary indexes are created: the volatile table has a PI and
        # aggregates perform full-table scans that do not benefit from SI.

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
        # Exclude summary rows and any initial-population rows
        metric_df = df[~df['stat_name'].isin(['Records Claimed', 'initial_population'])]
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

        if not engine_to_use:
            raise ValueError(
                "An eligibility_engine instance must be provided either to run() or to a prior num_steps() call.")

        # --------------------------------------------------------------
        # Create the session-local volatile base table (idempotent)
        # --------------------------------------------------------------
        self._create_volatile_base(engine_to_use)

        # Invalidate any cached preparation (built perhaps by num_steps())
        # so that subsequent SQL uses the new base table name.
        self._waterfall_groups = None

        # Prepare SQL jobs (use the volatile table)
        self._prepare_waterfall_steps(engine_to_use)

        # No secondary indexes are created – volatile table and full-table
        # aggregates do not benefit from them.
        os.makedirs(self.cfg.output_directory, exist_ok=True)
        from tlptaco.utils.fs import grant_group_rwx
        grant_group_rwx(self.cfg.output_directory)

        # Collect compiled metrics for *all* groups. Each item will be a tuple
        # (group_name, compiled_sections)
        compiled_groups: list[tuple[str, list[tuple[str, pd.DataFrame]]]] = []
        # Starting population per group (group_name -> int)
        starting_pops: dict[str, int] = {}

        # Holder for *previous* runs fetched from SQLite history so that the
        # Excel writer can render side-by-side comparison tabs.
        previous_groups: dict[str, list[tuple[str, pd.DataFrame]]] = {}

        # Pre-compute condition rows once (shared across groups)
        conds = engine_to_use.cfg.conditions
        cond_rows: list[dict] = []
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

        # ------------------------------------------------------------------
        # Enhance conditions dataframe with Section / Template / # columns for
        # the revamped Excel layout.
        # ------------------------------------------------------------------
        import re

        def _parse_check_name(name: str):
            """Split a check name like 'email_loyalty_B_1' into (section, template, #)."""
            parts = name.split('_')
            if len(parts) < 2:
                return name, '', ''
            section = parts[0]
            # Last numeric part (if any)
            num_match = re.match(r'^(\d+)$', parts[-1])
            if num_match:
                num = int(parts[-1])
                mid = parts[1:-1]
            else:
                num = ''
                mid = parts[1:]
            template = mid[0] if mid else ''
            return section, template, num

        enriched_rows = []
        for row in cond_rows:
            sec, tpl, num = _parse_check_name(row['check_name'])
            enriched_rows.append({
                'check_name': row['check_name'],
                'Section': sec,
                'Template': tpl,
                '#': num,
                'sql': row['sql'],
                'description': row['description']
            })

        conditions_df = pd.DataFrame(enriched_rows).set_index('check_name')

        for group in self._waterfall_groups:
            all_report_sections = []
            try:
                for job in group['jobs']:
                    df_raw = self.runner.to_df(job['sql'])
                    if 'cntr' not in df_raw.columns and 'value' in df_raw.columns:
                        df_raw = df_raw.rename(columns={'value': 'cntr'})

                    if job['type'] == 'standard':
                        df_pivoted = self._pivot_waterfall_df(df_raw, job['section_name'])
                        all_report_sections.append(df_pivoted)

                        # Capture starting population if not yet stored for this group
                        if group['name'] not in starting_pops:
                            sp = df_raw.loc[df_raw['stat_name'] == 'initial_population', 'cntr']
                            if not sp.empty:
                                starting_pops[group['name']] = int(sp.iloc[0])

                    elif job['type'] == 'segments':
                        detail_rows = df_raw[df_raw['stat_name'] != 'Records Claimed'].copy()
                        for section_name in detail_rows['section'].unique():
                            section_df = self._pivot_waterfall_df(
                                detail_rows[detail_rows['section'] == section_name],
                                section_name
                            )
                            if not section_df.empty:
                                all_report_sections.append(section_df)

                if all_report_sections:
                    compiled = []
                    for df in all_report_sections:
                        sec = df['section'].iat[0]
                        section_df = df.drop(columns='section').reset_index(drop=True)
                        compiled.append((sec, section_df))
                    compiled_groups.append((group['name'], compiled))

                    # --------------------------------------------------
                    # Attempt to fetch a *previous* snapshot for this
                    # group from the history DB (if available).
                    # --------------------------------------------------
                    prev_result = self._fetch_previous_group_metrics(group['name'])
                    if prev_result:
                        prev_date, prev_compiled, prev_start_pop = prev_result
                        previous_groups[group['name']] = {
                            'date': prev_date,
                            'compiled': prev_compiled,
                            'start_pop': prev_start_pop,
                        }

            except Exception as e:
                self.logger.exception(f"Waterfall grouping '{group['name']}' failed: {e}")
            finally:
                if progress:
                    progress.update('Waterfall')

        # ------------------------------------------------------------------
        # After processing *all* groups, write a single consolidated workbook
        # ------------------------------------------------------------------
        if compiled_groups:
            import tlptaco.engines.waterfall_excel as wf_excel_mod
            # Timestamped filename using offer_code_YYYY_MM_DD_HH:MM:SS.xlsx
            timestamp_str = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")

            # Sanitize offer code for filesystem safety (letters, numbers, _ -)
            import re
            safe_offer = re.sub(r'[^A-Za-z0-9_-]+', '_', self.offer_code or 'run')

            file_name = f"{safe_offer}_{timestamp_str}.xlsx"

            out_path = os.path.join(self.cfg.output_directory, file_name)

            wf_excel_mod.write_waterfall_excel(
                conditions_df,
                compiled_groups,
                out_path,
                previous=previous_groups,
                offer_code=self.offer_code,
                campaign_planner=self.campaign_planner,
                lead=self.lead,
                current_date=timestamp_str.replace('_', '-') ,
                starting_pops=starting_pops,
            )
            self.logger.info(f"Consolidated waterfall report written to {out_path}")

            # ------------------------------------------------------------------
            # Persist results to history database if enabled in configuration
            # ------------------------------------------------------------------
            try:
                self._log_history(conditions_df, compiled_groups)
            except Exception:
                # History logging should never crash the main pipeline – log & continue
                self.logger.exception("Failed to write waterfall history")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_history_db_path(self) -> str:
        """Return absolute path to the SQLite history DB based on config."""
        hist_cfg = self.cfg.history
        if hist_cfg.db_path:
            return os.path.abspath(hist_cfg.db_path)
        # default inside the output directory
        return os.path.join(self.cfg.output_directory, 'waterfall_history.sqlite')

    def _log_history(self, conditions_df, compiled_groups):
        """Insert results of this run into a SQLite history table.

        Parameters
        ----------
        conditions_df : pandas.DataFrame
            DataFrame indexed by check_name containing `sql` and `description`.
        compiled_groups : list[tuple[str, list[tuple[str, pandas.DataFrame]]]]
            Output structure from WaterfallEngine containing metrics per group.
        """
        # Guard clause – skip if tracking disabled
        if not self.cfg.history.track:
            return

        import sqlite3
        from datetime import datetime as _dt

        db_path = self._get_history_db_path()
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS waterfall_history (
                run_datetime      TEXT,
                group_name        TEXT,
                check_name        TEXT,
                criteria          TEXT,
                description       TEXT,
                unique_drops      INTEGER,
                regain            INTEGER,
                incremental_drops INTEGER,
                cumulative_drops  INTEGER,
                remaining         INTEGER
            );
            """
        )

        run_dt = _dt.now().isoformat(timespec='seconds')

        metric_cols = [
            'unique_drops',
            'regain',
            'incremental_drops',
            'cumulative_drops',
            'remaining',
        ]

        insert_sql = (
            "INSERT INTO waterfall_history (run_datetime, group_name, check_name, "
            "criteria, description, unique_drops, regain, incremental_drops, cumulative_drops, remaining) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)"
        )

        for group_name, compiled in compiled_groups:
            for _, df in compiled:
                for _, row in df.iterrows():
                    check_name = row.get('check_name')
                    try:
                        crit = conditions_df.loc[check_name, 'sql']
                        desc = conditions_df.loc[check_name, 'description']
                    except Exception:
                        crit = None
                        desc = None

                    metrics = [row.get(col) if col in row else None for col in metric_cols]

                    cur.execute(
                        insert_sql,
                        (
                            run_dt,
                            group_name,
                            check_name,
                            crit,
                            desc,
                            *metrics,
                        ),
                    )

        conn.commit()
        conn.close()
        self.logger.info(f"Waterfall run history appended to {db_path}")

        # Ensure group permissions (rwx) on the DB file
        try:
            from tlptaco.utils.fs import grant_group_rwx
            grant_group_rwx(db_path)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # History *read* helper – fetch latest snapshot for a group
    # ------------------------------------------------------------------

    def _fetch_previous_group_metrics(self, group_name: str):
        """Return the most recent waterfall metrics for *group_name* within
        the configured look-back window.

        Returns
        -------
        list[tuple[str, pandas.DataFrame]] | None
            A list in the same structure as the *compiled* argument used by
            the Excel writer: ``[(section_name, df), ...]``.  ``None`` if no
            history rows are available.
        """
        # Ensure history DB path exists
        db_path = self._get_history_db_path()
        if not os.path.isfile(db_path):
            return None

        import sqlite3
        import pandas as pd
        from datetime import datetime as _dt, timedelta as _td

        # ------------------------------------------------------------------
        # Determine selection strategy: legacy *lookback_days* vs the new
        # *days_ago_to_compare* parameter.  When the latter is provided we
        # ignore look-back logic and instead fetch **all** rows for the group
        # (we will pick the snapshot closest to the target point-in-time in
        # Python).  This avoids overly complex SQL and keeps the behaviour
        # deterministic even if the window is wider than the history range.
        # ------------------------------------------------------------------

        days_ago = self.cfg.history.compare_offset_days

        conn = sqlite3.connect(db_path)
        prev_start_pop: int | None = None
        try:
            if days_ago is not None:
                # Fetch *all* rows for this group – volume is expected to be
                # small (one row per check per historic run).
                query = (
                    "SELECT * FROM waterfall_history "
                    "WHERE group_name = ?"
                )
                df_raw = pd.read_sql(query, conn, params=(group_name,))
            else:
                # Legacy behaviour: windowed look-back then pick newest.
                lookback_days = self.cfg.history.recent_window_days or 30
                query = (
                    "SELECT * FROM waterfall_history "
                    "WHERE group_name = ? "
                    "AND run_datetime >= datetime('now', ?) "
                    "ORDER BY run_datetime DESC"
                )
                offset = f'-{int(lookback_days)} days'
                df_raw = pd.read_sql(query, conn, params=(group_name, offset))
        except Exception as ex:
            self.logger.debug(f"Unable to read prior history for group '{group_name}': {ex}")
            return None
        finally:
            conn.close()

        if df_raw.empty:
            return None

        # Ensure run_datetime parsed to datetime objects for comparison logic
        df_raw['run_dt_obj'] = pd.to_datetime(df_raw['run_datetime'])

        if days_ago is not None:
            # Target date/time – midnight-ish exact time not critical because
            # we will measure absolute delta.
            target_dt = _dt.now() - _td(days=int(days_ago))

            # Compute absolute time delta (in seconds) per row then pick the
            # minimal delta *per run*, finally choose the run with smallest
            # delta overall.
            df_raw['abs_delta'] = (df_raw['run_dt_obj'] - target_dt).abs()

            # Identify the run_datetime (timestamp) with the smallest delta
            nearest_idx = df_raw['abs_delta'].idxmin()
            nearest_dt = df_raw.loc[nearest_idx, 'run_datetime']

            df_latest = df_raw[df_raw['run_datetime'] == nearest_dt].copy()
            # Determine starting population for this historic run
            if 'stat_name' in df_raw.columns:
                sp_series = df_raw[(df_raw['run_datetime'] == nearest_dt) &
                                   (df_raw['check_name'] == 'Total') &
                                   (df_raw['stat_name'] == 'initial_population')]['cntr']
                if not sp_series.empty:
                    prev_start_pop = int(sp_series.iloc[0])
        else:
            # Legacy: most recent inside window
            latest_dt = df_raw['run_datetime'].max()
            df_latest = df_raw[df_raw['run_datetime'] == latest_dt].copy()
            if 'stat_name' in df_raw.columns:
                sp_series = df_raw[(df_raw['run_datetime'] == latest_dt) &
                                   (df_raw['check_name'] == 'Total') &
                                   (df_raw['stat_name'] == 'initial_population')]['cntr']
                if not sp_series.empty:
                    prev_start_pop = int(sp_series.iloc[0])

        metric_cols = [
            'unique_drops',
            'regain',
            'incremental_drops',
            'cumulative_drops',
            'remaining',
        ]

        # Keep only relevant columns to match the pivoted structure used by
        # the writer.
        cols_available = [c for c in metric_cols if c in df_latest.columns]
        if not cols_available:
            return None

        df_wide = df_latest[['check_name', *cols_available]].copy()
        # Add a dummy 'section' column so downstream code can reuse the same
        # handling logic.  We flag it as 'Previous'.
        df_wide['section'] = 'Historical'

        # Reset index order similar to pivoting routine
        df_wide = df_wide.reset_index(drop=True)

        return (nearest_dt if days_ago is not None else latest_dt,
                [('Historical', df_wide)],
                prev_start_pop)