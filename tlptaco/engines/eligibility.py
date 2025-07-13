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
        
    def run(self, progress=None):
        """
        Render and execute eligibility SQL to create a "smart" table with
        both individual and summary flags.
        """
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
            # Pass the entire nested conditions object to the template
            'conditions': cfg.conditions
        }

        try:
            self.logger.info(f"Dropping existing table {cfg.eligibility_table}")
            self.runner.run(f"DROP TABLE {cfg.eligibility_table};")
        except Exception:
            self.logger.info("No existing eligibility table to drop")

        tmpl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'templates'))
        gen = SQLGenerator(tmpl_dir)
        sql = gen.render('eligibility.sql.j2', context)

        for stmt in sql.split(';'):
            stmt = stmt.strip()
            if not stmt:
                continue
            self.logger.info('Executing eligibility SQL statement')
            self.runner.run(stmt)
            if progress:
                progress.update("Eligibility")