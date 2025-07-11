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

    def run(self):
        """
        Render and execute eligibility SQL (including work tables if configured).
        """
        # Build context for Jinja
        cfg = self.cfg
        # Flatten checks: main BA, main others, then channel BA & others
        checks = []
        # main BA
        for chk in cfg.conditions.main.BA:
            checks.append({'name': chk.name, 'sql': chk.sql})
        # main others
        if cfg.conditions.main.others:
            for lst in cfg.conditions.main.others.values():
                for chk in lst:
                    checks.append({'name': chk.name, 'sql': chk.sql})
        # channels
        for tmpl in cfg.conditions.channels.values():
            for chk in tmpl.BA:
                checks.append({'name': chk.name, 'sql': chk.sql})
            if tmpl.others:
                for lst in tmpl.others.values():
                    for chk in lst:
                        checks.append({'name': chk.name, 'sql': chk.sql})

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
            'checks': checks,
            'tables': tables,
            'where_clauses': where_clauses
        }
        # Drop existing eligibility table if exists to allow re-run
        try:
            self.logger.info(f"Dropping existing table {cfg.eligibility_table}")
            self.runner.run(f"DROP TABLE {cfg.eligibility_table};")
        except Exception:
            self.logger.info("No existing eligibility table to drop")
        # Render SQL
        tmpl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(tmpl_dir)
        sql = gen.render('eligibility.sql.j2', context)
        # Execute each statement
        for stmt in sql.split(';'):
            stmt = stmt.strip()
            if not stmt:
                continue
            self.logger.info('Executing eligibility SQL statement')
            self.runner.run(stmt)