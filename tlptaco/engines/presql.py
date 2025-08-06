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
                 logger=None):
        self.files_cfg: List[PreSQLFile] = list(files_cfg or [])
        self.runner = runner
        self.logger = logger or get_logger("presql")

        # Prepared lists filled by _prepare()
        self._sql_tasks: List[Tuple[str, str]] | None = None  # (file, stmt)
        self._analytic_tasks: List[Tuple[str, str, Tuple[str, ...]]] | None = None  # (file, table, cols)

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
        self._analytic_tasks = []

        for item in self.files_cfg:
            path = item.path
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception as e:
                self.logger.error(f"Failed reading pre-SQL file {path}: {e}")
                raise

            for stmt in self._split_sql(content):
                self._sql_tasks.append((path, stmt))

            # Build analytics tasks (if any)
            if item.analytics and item.analytics.unique_counts:
                table = item.analytics.table
                for cols in item.analytics.unique_counts:
                    if isinstance(cols, str):
                        col_tuple = (cols,)
                    else:
                        col_tuple = tuple(cols)
                    self._analytic_tasks.append((path, table, col_tuple))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def num_steps(self) -> int:
        """Total number of *individual* tasks (SQL statements + analytics)."""
        self._prepare()
        sql_cnt = len(self._sql_tasks or [])
        ana_cnt = len(self._analytic_tasks or [])
        return sql_cnt + ana_cnt

    def run(self, progress=None):
        """Execute all tasks in order.

        The *progress* argument is the shared ``ProgressManager`` instance
        used by the CLI; it must expose ``update(layer_name, advance=1)``.
        """

        self._prepare()

        layer_name = "Pre-SQL"

        # 1. Execute SQL statements
        for _file, stmt in self._sql_tasks or []:
            self.logger.info(f"Executing pre-SQL from {_file}")
            self.runner.run(stmt)
            if progress:
                progress.update(layer_name)

        # 2. Execute analytics queries (distinct counts)
        for _file, table, cols in self._analytic_tasks or []:
            col_list = ", ".join(cols)
            sql = f"SELECT COUNT(DISTINCT {col_list}) AS cnt FROM {table}"
            df: pd.DataFrame = self.runner.to_df(sql)
            cnt = int(df.iloc[0, 0]) if not df.empty else None
            cols_disp = ", ".join(cols)
            if cnt is not None:
                self.logger.info(
                    f"[Pre-SQL analytics] {_file}: unique({cols_disp}) in {table} = {cnt:,}"
                )
            if progress:
                progress.update(layer_name)
