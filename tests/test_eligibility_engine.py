import os
import pytest

import tlptaco.engines.eligibility as elig_mod
from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.config.schema import (
    EligibilityConfig, ConditionsConfig, TemplateConditions, ConditionCheck, TableConfig
)

class DummyRunner:
    def __init__(self):
        self.statements = []
    def run(self, sql):
        # Collect executed SQL statements
        self.statements.append(sql)

class DummyLogger:
    def info(self, msg): pass
    def exception(self, msg): pass

class FakeSQLGen:
    def __init__(self, templates_dir):
        self.templates_dir = templates_dir
    def render(self, template_name, context):
        # Return two dummy statements separated by semicolons
        return "DUMMY_STMT1; DUMMY_STMT2;"

@pytest.fixture(autouse=True)
def patch_sqlgenerator(monkeypatch):
    # Monkeypatch SQLGenerator used in EligibilityEngine
    monkeypatch.setattr(elig_mod, 'SQLGenerator', FakeSQLGen)
    yield

def make_config():
    # Build a minimal EligibilityConfig
    main_checks = [ConditionCheck(name="chk", sql="1=1")]
    tmpl_main = TemplateConditions(BA=main_checks, others={})
    conds = ConditionsConfig(main=tmpl_main, channels={})
    tables = [
        TableConfig(
            name="tbl", alias="tbl", sql=None,
            join_type=None, join_conditions=None,
            where_conditions=None, unique_index=None,
            collect_stats=None
        )
    ]
    return EligibilityConfig(
        eligibility_table="test_tbl",
        conditions=conds,
        tables=tables,
        unique_identifiers=["tbl.id"]
    )

def test_eligibility_engine_runs_multiple_statements(tmp_path):
    cfg = make_config()
    runner = DummyRunner()
    logger = DummyLogger()
    engine = EligibilityEngine(cfg, runner, logger)
    # Run without progress
    engine.run()
    # Expect DROP TABLE then two dummy statements
    assert runner.statements[0].startswith("DROP TABLE test_tbl")
    # The next statements are DUMMY_STMT1 and DUMMY_STMT2
    assert "DUMMY_STMT1" in runner.statements[1]
    assert "DUMMY_STMT2" in runner.statements[2]
    # Exactly three statements run
    assert len(runner.statements) == 3
