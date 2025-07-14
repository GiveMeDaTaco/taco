"""
Eligibility engine: runs work tables and eligibility SQL.
"""
from tlptaco.config.schema import EligibilityConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.sql.generator import SQLGenerator
import os

class EligibilityEngine:
    def __init__(self, cfg: EligibilityConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("eligibility")
        self._sql_statements = None

    def _prepare_sql(self):
        """
        Prepares the SQL statements for execution.
        This method builds the context and renders the SQL template, but only once.
        The generated statements are cached in self._sql_statements.
        """
        # If the SQL has already been generated, do nothing.
        if self._sql_statements is not None:
            self.logger.info("Using cached SQL statements.")
            return

        self.logger.info("No cached SQL found. Generating new SQL statements.")
        cfg = self.cfg
        tables = []
        where_clauses = []
        for t in cfg.tables:
            tables.append({
                'name': t.name,
                'alias': t.alias,
                'join_type': t.join_type or '',
                'join_conditions': t.join_conditions or ''
            })
            if t.where_conditions:
                where_clauses.append(t.where_conditions)

        context = {
            'eligibility_table': cfg.eligibility_table,
            'unique_identifiers': cfg.unique_identifiers,
            'unique_without_aliases': [u.split('.')[-1] for u in cfg.unique_identifiers],
            'tables': tables,
            'where_clauses': where_clauses,
            'checks': [
                {'name': chk.name, 'sql': chk.sql}
                for chk in cfg.conditions.main.BA
            ],
        }

        tmpl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(tmpl_dir)  # Using SQLGenerator for example
        sql = gen.render('eligibility.sql.j2', context)

        # Cache the generated statements after stripping any whitespace
        self._sql_statements = [stmt for stmt in sql.split(';') if stmt.strip()]

    def num_steps(self) -> int:
        """
        Calculates the total number of SQL statements that will be executed by the run() method.
        Uses the cached SQL if available.

        Returns:
            int: The total number of steps (SQL statements).
        """
        self.logger.info("Calculating the number of steps for the eligibility run.")
        # Ensure the SQL statements are prepared and cached
        self._prepare_sql()

        # The total number of steps is the count of main statements + 1 for the DROP TABLE command.
        total_steps = 1 + len(self._sql_statements)

        self.logger.info(f"Calculation complete: {total_steps} steps.")
        return total_steps

    def run(self, progress=None):
        """
        Render and execute eligibility SQL to create a "smart" table.
        Uses cached SQL if num_steps() was called first.

        Args:
            progress: An optional progress tracking object with an update() method.
        """
        # Ensure the SQL statements are prepared and cached
        self._prepare_sql()

        # Step 1: Attempt to drop the existing table.
        try:
            self.logger.info(f"Dropping existing table {self.cfg.eligibility_table}")
            self.runner.run(f"DROP TABLE {self.cfg.eligibility_table};")
        except Exception:
            self.logger.info("No existing eligibility table to drop")

        if progress:
            progress.update("Eligibility")

        # Step 2: Execute each of the cached statements.
        for stmt in self._sql_statements:
            self.logger.info('Executing eligibility SQL statement')
            self.runner.run(stmt)
            if progress:
                progress.update("Eligibility")
