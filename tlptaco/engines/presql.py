"""
Pre-SQL engine – executes user-supplied *.sql* scripts **before** the main
tlptaco pipeline and (optionally) performs simple analytics queries defined in
the configuration.

This logic was originally implemented in an ad-hoc way inside *cli.py*.
Moving it here makes it reusable (e.g. when tlptaco is used as a library) and
keeps the CLI cleaner.
"""

from __future__ import annotations

import os
from typing import List, Tuple, Union, Sequence

import pandas as pd

from tlptaco.config.schema import PreSQLFile
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger


class PreSQLEngine:
    """Execute user-supplied *pre-SQL* scripts and (optionally) quick
    analytics queries.

    The engine is a thin convenience wrapper around
    :pymeth:`tlptaco.db.runner.DBRunner.run` / ``.to_df`` that makes it easy
    to plug *arbitrary* setup SQL into the pipeline **before** the
    eligibility / waterfall stages.

    Example
    -------
    >>> from tlptaco.engines.presql import PreSQLEngine
    >>> presql_engine = PreSQLEngine(app_cfg.pre_sql, runner)
    >>> presql_engine.run()  # executes all statements

    When used by the CLI the engine is created automatically so most users
    will never import it directly – the example is helpful for unit tests
    or when tlptaco is embedded as a library.

    Parameters
    ----------
    files_cfg
        Parsed list of :class:`tlptaco.config.schema.PreSQLFile` objects
        (``AppConfig.pre_sql``).
    runner
        Live :class:`tlptaco.db.runner.DBRunner` instance.
    logger
        Optional custom logger; defaults to a child logger named
        ``tlptaco.presql``.
    """

    def __init__(self,
                 files_cfg: Sequence[PreSQLFile] | None,
                 runner: DBRunner,
                 logger=None,
                 user_list: list[str] | None = None,
                 *,
                 layer_name: str = "Pre-SQL"):
        self.files_cfg: List[PreSQLFile] = list(files_cfg or [])
        self.runner = runner
        self.logger = logger or get_logger("presql")
        self._grant_users = user_list or []
        # Progress layer label ("Pre-SQL" or "Post-SQL") used when updating
        # the shared ProgressManager.  Making this configurable avoids hard
        # coding a name that might not exist in the progress layout defined
        # by the CLI.
        self._layer_name = layer_name

        # Prepared lists filled by _prepare()
        #   • _sql_tasks : list of (file_path, sql_statement, error_flag)
        #   • _py_tasks  : list of (file_path, error_flag)
        #   • _sas_tasks : list of (file_path, error_flag)
        self._sql_tasks: List[Tuple[str, str, bool]] | None = None
        self._py_tasks: List[Tuple[str, bool]] | None = None
        self._sas_tasks: List[Tuple[str, bool]] | None = None
        # Distinct-count analytic tasks
        # tuple: (file_path, table, column_tuple, error_flag, grant_flag)
        self._distinct_tasks: List[Tuple[str, str, Tuple[str, ...], bool, bool]] | None = None

        # Group-count tasks – (file_path, table, column_tuple, error_flag)
        self._group_tasks: List[Tuple[str, str, Tuple[str, ...], bool]] | None = None

        # Preview tasks – (file_path, table, rows, columns_opt, error_flag)
        self._preview_tasks: List[Tuple[str, str, int, tuple[str, ...] | None, bool]] | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_sql(text: str) -> List[str]:
        """Very naïve SQL splitter – *identical* to the previous CLI logic.

        It simply splits on semicolons and strips whitespace, skipping blank
        parts.  Good enough for flat DDL / DML scripts but **not** PL/SQL or
        Teradata BTEQ blocks containing embedded semicolons.
        """

        parts = [s.strip() for s in text.split(";")]
        return [s for s in parts if s]

    def _prepare(self):
        """Read files, split into statements and build analytics task list."""

        if self._sql_tasks is not None:
            # Already prepared (cached) – nothing to do
            return

        self._sql_tasks = []
        self._py_tasks = []
        self._sas_tasks = []
        self._distinct_tasks = []
        self._group_tasks = []
        self._preview_tasks = []

        for item in self.files_cfg:
            path = item.path
            # Branch on file extension – .sql (default) versus .py (new behaviour)
            lowered = path.lower()
            if lowered.endswith(".py"):
                # No SQL parsing – just schedule python execution.
                self._py_tasks.append((path, item.error))

                # Analytics with a .py script make little sense; warn once.
                if item.analytics:
                    self.logger.warning(
                        f"Analytics configuration ignored for Python pre/post script {path} – not applicable.")
                # Skip further processing for this file
                continue

            if lowered.endswith(".sas"):
                # Schedule SAS execution via external command.
                self._sas_tasks.append((path, item.error))

                # Analytics with a .py script make little sense; warn once.
                if item.analytics:
                    self.logger.warning(
                        f"Analytics configuration ignored for SAS pre/post script {path} – not applicable.")
                # Skip further processing for this file
                continue

            # ----------------- existing .sql behaviour -------------------
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception as e:
                self.logger.error(f"Failed reading pre-SQL file {path}: {e}")
                raise

            # Record each individual SQL statement with the file's error flag
            for stmt in self._split_sql(content):
                self._sql_tasks.append((path, stmt, item.error))

            # Build analytics tasks (distinct counts, group counts, preview)
            if item.analytics:
                table = item.analytics.table

                # --- distinct (unique) counts ---------------------------------------
                for cols in item.analytics.unique_counts or []:
                    col_tuple = (cols,) if isinstance(cols, str) else tuple(cols)
                    grant_flag = bool(getattr(item.analytics, 'grant_access', False))
                    self._distinct_tasks.append((path, table, col_tuple, item.error, grant_flag))

                # --- group counts ----------------------------------------------------
                for cols in item.analytics.group_counts or []:
                    col_tuple = (cols,) if isinstance(cols, str) else tuple(cols)
                    self._group_tasks.append((path, table, col_tuple, item.error))

                # --- preview sample rows -------------------------------------------
                if item.analytics.preview is not None:
                    rows = item.analytics.preview.rows
                    cols = None
                    if item.analytics.preview.columns:
                        cols = tuple(item.analytics.preview.columns)
                    self._preview_tasks.append((path, table, rows, cols, item.error))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def num_steps(self) -> int:
        """Total number of *individual* tasks (SQL statements + analytics)."""
        self._prepare()
        sql_cnt = len(self._sql_tasks or [])
        py_cnt = len(self._py_tasks or [])
        sas_cnt = len(self._sas_tasks or [])
        ana_cnt = (
            len(self._distinct_tasks or []) +
            len(self._group_tasks or []) +
            len(self._preview_tasks or [])
        )
        return sql_cnt + py_cnt + sas_cnt + ana_cnt

    def run(self, progress=None):
        """Execute all tasks in order.

        The *progress* argument is the shared ``ProgressManager`` instance
        used by the CLI; it must expose ``update(layer_name, advance=1)``.
        """

        self._prepare()

        layer_name = self._layer_name

        # 1. Execute SQL statements
        for _file, stmt, err_flag in self._sql_tasks or []:
            self.logger.info(f"Executing pre-SQL from {_file}")
            try:
                self.runner.run(stmt)
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"Pre-SQL file {_file} failed and error flag is True – aborting execution: {e}")
                    raise
                # With error=False we log a warning and continue
                self.logger.warning(
                    f"Pre-SQL file {_file} statement failed but 'error' is False – continuing. Error: {e}")
            finally:
                if progress:
                    progress.update(layer_name)

        # 1b. Execute Python scripts (".py" paths)
        import subprocess, sys
        for py_path, err_flag in self._py_tasks or []:
            self.logger.info(f"Executing pre-SQL Python script {py_path}")
            try:
                # Run script in its directory so relative imports / file paths work naturally
                subprocess.run([sys.executable, py_path], check=True, cwd=os.path.dirname(py_path))
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"Python script {py_path} failed and error flag is True – aborting execution: {e}")
                    raise
                self.logger.warning(
                    f"Python script {py_path} failed but 'error' is False – continuing. Error: {e}")
            finally:
                if progress:
                    progress.update(layer_name)

        # 1c. Execute SAS scripts (.sas) using external SAS command if available
        import shutil
        sas_cmd_default = os.environ.get('TLPTACO_SAS_CMD', 'sas')
        sas_available = shutil.which(sas_cmd_default) is not None
        for sas_path, err_flag in self._sas_tasks or []:
            self.logger.info(f"Executing pre-SQL SAS script {sas_path}")
            if not sas_available:
                msg = f"SAS command '{sas_cmd_default}' not found in PATH"
                if err_flag:
                    self.logger.error(msg)
                    raise RuntimeError(msg)
                self.logger.warning(msg + " – skipping script as error flag is False")
                if progress:
                    progress.update(layer_name)
                continue

            cmd = [sas_cmd_default, sas_path, '-nosplash', '-noterminal']
            try:
                subprocess.run(cmd, check=True, cwd=os.path.dirname(sas_path))
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"SAS script {sas_path} failed and error flag is True – aborting execution: {e}")
                    raise
                self.logger.warning(
                    f"SAS script {sas_path} failed but 'error' is False – continuing. Error: {e}")
            finally:
                if progress:
                    progress.update(layer_name)

        # 2a. Execute distinct count analytics
        for _file, table, cols, err_flag, grant_flag in self._distinct_tasks or []:
            col_list = ", ".join(cols)
            sql = f"SELECT COUNT(DISTINCT {col_list}) AS cnt FROM {table}"
            try:
                df: pd.DataFrame = self.runner.to_df(sql)
                cnt = int(df.iloc[0, 0]) if not df.empty else None
                cols_disp = ", ".join(cols)
                if cnt is not None:
                    self.logger.info(
                        f"[Pre-SQL analytics] {_file}: unique({cols_disp}) in {table} = {cnt:,}"
                    )
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"Analytics query from {_file} failed and error flag is True – aborting: {e}")
                    raise
                self.logger.warning(
                    f"Analytics query from {_file} failed but 'error' is False – continuing. Error: {e}")
            if progress:
                progress.update(layer_name)

            if grant_flag:
                self._grant_table_access(table)

        # 2b. Execute group count analytics
        for _file, table, cols, err_flag in self._group_tasks or []:
            col_list = ", ".join(cols)
            sql = (
                f"SELECT {col_list}, COUNT(*) AS cnt FROM {table} "
                f"GROUP BY {col_list} ORDER BY cnt DESC"
            )
            try:
                df: pd.DataFrame = self.runner.to_df(sql)
                self.logger.info(
                    f"[Pre-SQL analytics] group counts by ({col_list}) – {len(df)} groups"
                )
                # Log first 20 rows as markdown table for readability
                head = df.head(20)
                try:
                    md = head.to_markdown(index=False)
                except Exception:
                    md = head.to_string(index=False)
                self.logger.info("\n" + md)
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"Group-count query from {_file} failed and error flag is True – aborting: {e}")
                    raise
                self.logger.warning(
                    f"Group-count query from {_file} failed but 'error' is False – continuing. Error: {e}")
            finally:
                if progress:
                    progress.update(layer_name)

        # 2c. Execute preview tasks
        for _file, table, rows, cols, err_flag in self._preview_tasks or []:
            col_expr = "*" if cols is None else ", ".join(cols)
            # Use SAMPLE for Teradata; fallback generic TOP N
            sql = f"SELECT {col_expr} FROM {table} SAMPLE {rows}"
            try:
                df: pd.DataFrame = self.runner.to_df(sql)
                self.logger.info(
                    f"[Pre-SQL analytics] preview ({rows} rows) of {table}"
                )
                try:
                    md = df.head(rows).to_markdown(index=False)
                except Exception:
                    md = df.head(rows).to_string(index=False)
                self.logger.info("\n" + md)
            except Exception as e:
                if err_flag:
                    self.logger.error(
                        f"Preview query from {_file} failed and error flag is True – aborting: {e}")
                    raise
                self.logger.warning(
                    f"Preview query from {_file} failed but 'error' is False – continuing. Error: {e}")
            finally:
                if progress:
                    progress.update(layer_name)

    # ------------------------------------------------------------------
    # Grants helper
    # ------------------------------------------------------------------
    def _grant_table_access(self, table: str):
        if not self._grant_users:
            return
        for user in self._grant_users:
            try:
                self.runner.run(f"GRANT SELECT,INSERT,UPDATE,DELETE ON {table} TO {user};")
                self.runner.run(f"GRANT DROP ON {table} TO {user};")
            except Exception as e:
                self.logger.warning(f"Failed granting privileges on {table} to {user}: {e}")
