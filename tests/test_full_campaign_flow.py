import os
import pandas as pd
import pytest

from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.engines.output import OutputEngine
from tlptaco.config.schema import (
    AppConfig, EligibilityConfig, WaterfallConfig, OutputConfig,
    ConditionsConfig, TemplateConditions, ConditionCheck,
    OutputChannelConfig, OutputOptions, TableConfig, DatabaseConfig, LoggingConfig
)

class DummyRunner:
    def __init__(self):
        self.queries = []
    def run(self, sql):
        # No-op for DDL/DML
        self.queries.append(sql)
    def to_df(self, sql):
        # Return a dummy waterfall result based on sql context
        # Simulate two checks with two metrics; use 'cntr' column to match engine expectations
        df = pd.DataFrame([
            {'check_name': 'chk1', 'stat_name': 'unique_drops', 'cntr': 10},
            {'check_name': 'chk2', 'stat_name': 'remaining', 'cntr': 5},
        ])
        # Include 'section' column expected by pivot logic
        df['section'] = 'Base'
        return df
    def cleanup(self):
        pass

class DummyLogger:
    def info(self, msg): pass
    def warning(self, msg): pass
    def debug(self, msg): pass
    def exception(self, msg): pass

@pytest.fixture(autouse=True)
def patch_sqlgenerator(monkeypatch):
    # Use identity rendering (no Jinja errors)
    from tlptaco.sql.generator import SQLGenerator as RealGen
    class FakeGen(RealGen):
        def render(self, template_name, context):
            # Return a dummy SELECT for to_df
            return "SELECT * FROM dummy;"
    import tlptaco.engines.eligibility as elig_mod
    import tlptaco.engines.waterfall as wf_mod
    import tlptaco.engines.output as out_mod
    monkeypatch.setattr(elig_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(wf_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(out_mod, 'SQLGenerator', FakeGen)
    yield

def make_app_config(tmp_path):
    # Minimal AppConfig for full run
    # Define a single channel 'default' for output
    channel_checks = TemplateConditions(BA=[ConditionCheck(name='chk1', sql='1=1')], others={})
    elig_cfg = EligibilityConfig(
        eligibility_table='elig_tbl',
        conditions=ConditionsConfig(
            main=TemplateConditions(BA=[ConditionCheck(name='chk1', sql='1=1')], others={}),
            channels={'default': channel_checks}
        ),
        tables=[TableConfig(name='t', alias='t', sql=None, join_type=None,
                           join_conditions=None, where_conditions=None,
                           unique_index=None, collect_stats=None)],
        unique_identifiers=['t.id']
    )
    wf_cfg = WaterfallConfig(output_directory=str(tmp_path), count_columns=['t.id'])
    out_opts = OutputOptions(format='csv', additional_arguments={}, custom_function=None)
    out_ch = OutputChannelConfig(
        columns=['t.id', 'chk1'],
        file_location=str(tmp_path),
        file_base_name='out',
        output_options=out_opts,
        unique_on=[]
    )
    out_cfg = OutputConfig(channels={'default': out_ch})
    db_cfg = DatabaseConfig(host='h', user='u', password='p', logmech=None)
    log_cfg = LoggingConfig(level='INFO', file=None, debug_file=None)
    return AppConfig(logging=log_cfg, database=db_cfg,
                     eligibility=elig_cfg, waterfall=wf_cfg, output=out_cfg)

def test_full_campaign_flow(tmp_path, monkeypatch):
    # Assemble config, runner, and logger
    app_cfg = make_app_config(tmp_path)
    runner = DummyRunner()
    logger = DummyLogger()

    # Run Eligibility
    elig_engine = EligibilityEngine(app_cfg.eligibility, runner, logger)
    elig_engine.run()
    # Expect a DROP and a dummy SELECT
    assert any('DROP TABLE elig_tbl' in q for q in runner.queries)
    # Reset queries for next stage
    runner.queries.clear()

    # Run Waterfall
    wf_engine = WaterfallEngine(app_cfg.waterfall, runner, logger)
    # Monkeypatch write_waterfall_excel to capture output without writing files
    from tlptaco.engines.waterfall_excel import write_waterfall_excel as real_writer
    captured = {}
    def fake_writer(conditions_df, compiled, output_path, group_name,
                     offer_code, campaign_planner, lead, current_date):
        captured['path'] = output_path
        captured['compiled'] = compiled
    monkeypatch.setattr('tlptaco.engines.waterfall_excel.write_waterfall_excel', fake_writer)
    # Run waterfall report
    wf_engine.run(elig_engine)
    # Verify an Excel path was generated under output_directory
    assert str(tmp_path) in captured.get('path', '')
    # compiled is list of (section_name, DataFrame) tuples
    compiled = captured.get('compiled', [])
    assert compiled, "No compiled output captured"
    # Inspect the first section's DataFrame
    section_name, df_out = compiled[0]
    # It should include check_name and the new metrics columns
    assert 'chk1' in df_out['check_name'].values
    assert 'unique_drops' in df_out.columns
    assert 'remaining' in df_out.columns
