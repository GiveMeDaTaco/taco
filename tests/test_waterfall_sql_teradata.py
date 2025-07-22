"""Validate that all waterfall SQL generated from the example campaign parses
successfully under the Teradata dialect using *sqlglot* (pure-Python, no DB
connection required).

The test mimics the CLI flow, but swaps the DB runner for a dummy that merely
captures SQL strings.  Each captured statement is then fed through
``sqlglot.parse_one(..., read="teradata")`` which raises ``ParseError`` on any
syntax issue (e.g., the dreaded "FROM flagsUNION ALL").
"""

from pathlib import Path

import pandas as pd  # noqa – used in DummyRunner.to_df
import pytest

try:
    import sqlglot
except ImportError:  # pragma: no cover – guard if dependency missing
    sqlglot = None

from tlptaco.config.loader import load_config
from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.db.runner import DBRunner


class DummyRunner(DBRunner):
    """Runner stub that records SQL instead of executing it."""

    def __init__(self):
        # Skip parent init to avoid a real DBConnection
        self.logger = None
        self.conn = None
        self.statements: list[str] = []

    # override methods that engines call
    def run(self, sql: str):  # noqa: D401 – simple verb
        self.statements.append(sql)
        return None

    def to_df(self, sql: str):  # noqa: D401 – simple verb
        self.statements.append(sql)
        return pd.DataFrame()  # content is irrelevant for parsing

    # cleanup is a noop
    def cleanup(self):
        pass


@pytest.mark.skipif(sqlglot is None, reason="sqlglot not installed")
def test_waterfall_sql_parses_for_teradata():
    """Render waterfall SQL for the POC campaign and parse with sqlglot."""

    # Path to the POC config located at project root
    cfg_path = Path(__file__).resolve().parent.parent / "example_campaign_poc.yaml"
    config = load_config(str(cfg_path))

    dummy = DummyRunner()

    # Build engines – identical to CLI sequence
    elig_engine = EligibilityEngine(config.eligibility, dummy)
    wf_engine = WaterfallEngine(config.waterfall, dummy)

    # Prepare all waterfall jobs (no execution yet)
    wf_engine.num_steps(elig_engine)

    # Run – this will call dummy.to_df which just records SQL
    wf_engine.run(eligibility_engine=elig_engine)

    assert dummy.statements, "No SQL captured from WaterfallEngine run()"

    for stmt in dummy.statements:
        # sqlglot raises ParseError on invalid syntax
        try:
            sqlglot.parse_one(stmt, read="teradata")
        except sqlglot.errors.ParseError as err:  # pragma: no cover
            pytest.fail(f"Teradata syntax error detected in SQL:\n{stmt}\n\n{err}")