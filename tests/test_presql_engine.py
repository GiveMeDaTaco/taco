"""Unit tests for the PreSQLEngine."""

from pathlib import Path

import pandas as pd

from tlptaco.engines.presql import PreSQLEngine
from tlptaco.config.schema import PreSQLFile, PreSQLAnalytics


class DummyRunner:
    """A minimal stub of DBRunner collecting executed SQL statements."""

    def __init__(self):
        self.executed = []
        self.queried = []

    # These mimic DBRunner API used by PreSQLEngine
    def run(self, sql: str):  # noqa: D401
        """Record executed statement (DDL/DML)."""
        self.executed.append(sql)

    def to_df(self, sql: str):
        self.queried.append(sql)
        # Return one-row DataFrame with dummy count 42
        return pd.DataFrame([{ 'cnt': 42 }])


def test_presql_engine(tmp_path):
    # Create two small SQL files
    f1 = tmp_path / 'a.sql'
    f1.write_text('CREATE TABLE x AS SELECT 1; INSERT INTO x VALUES (2);')

    f2 = tmp_path / 'b.sql'
    f2.write_text('DELETE FROM x;')

    analytics = PreSQLAnalytics(
        table='x',
        unique_counts=['col1', ['col2', 'col3']]
    )

    cfg_items = [
        PreSQLFile(path=str(f1)),  # legacy bare execution
        PreSQLFile(path=str(f2), analytics=analytics),
    ]

    runner = DummyRunner()

    engine = PreSQLEngine(cfg_items, runner, logger=None)

    # Tasks: f1 (2 statements) + f2 (1 statement) + analytics (2 distinct counts) = 5
    assert engine.num_steps() == 5

    engine.run()

    # Expect 3 executed DDL/DML statements
    assert len(runner.executed) == 3

    # Expect 2 analytic SELECTs
    assert len(runner.queried) == 2
