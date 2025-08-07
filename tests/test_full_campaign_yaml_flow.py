import os
import yaml
import pytest
import pandas as pd

from pathlib import Path
import pandas as pd
from tlptaco.sql.generator import SQLGenerator as RealGen

# Auto-patch SQLGenerator to return dummy SQL for waterfall templates
class FakeGen(RealGen):
    def render(self, template_name, context):
        return "SELECT * FROM dummy;"
import tlptaco.engines.eligibility as elig_mod
import tlptaco.engines.waterfall as wf_mod
import tlptaco.engines.output as out_mod
@pytest.fixture(autouse=True)
def patch_sqlgenerator(monkeypatch):
    monkeypatch.setattr(elig_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(wf_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(out_mod, 'SQLGenerator', FakeGen)
    yield
from tlptaco.config.loader import load_config
from tlptaco.config.schema import AppConfig
from tlptaco.engines.eligibility import EligibilityEngine
from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.engines.output import OutputEngine


class DummyRunner:
    def __init__(self):
        self.queries = []
    def run(self, sql):
        # track executed SQL
        self.queries.append(sql)
    def to_df(self, sql):
        # Return a simple DataFrame for waterfall and output that matches engine expectations
        df = pd.DataFrame([
            {'check_name': 'chkA', 'stat_name': 'unique_drops', 'cntr': 1, 'section': 'Base'},
            {'check_name': 'chkB', 'stat_name': 'remaining', 'cntr': 2, 'section': 'Base'},
        ])
        return df
    def cleanup(self):
        pass

class DummyLogger:
    def info(self, msg): pass
    def warning(self, msg): pass
    def debug(self, msg): pass
    def exception(self, msg): pass


def test_full_campaign_from_yaml(tmp_path, monkeypatch):
    # Build a complex config with multiple channels and templates
    cfg = {
        'logging': {'level': 'INFO', 'file': None, 'debug_file': None},
        'database': {'host': 'h', 'user': 'u', 'password': 'p', 'logmech': None},
        'eligibility': {
            'eligibility_table': 'elig_tbl',
            'unique_identifiers': ['t.id', 't.grp'],
            'tables': [
                {'name': 't_main', 'alias': 't', 'join_type': '', 'join_conditions': '',
                 'where_conditions': "t.active=1", 'unique_index': None, 'collect_stats': None}
            ],
            'conditions': {
                'main': {'BA': [
                    {'name': 'chk_main1', 'sql': 'col1=1'},
                    {'name': 'chk_main2', 'sql': 'col2=2'}
                ], 'others': {}},
                'channels': {
                    'email': {
                        'BA': [{'name': 'chk_email_BA', 'sql': "colE='x'"}],
                        'others': {
                            'segX': [{'name': 'segX_chk1', 'sql': 'colX>5'}],
                            'segY': [{'name': 'segY_chk1', 'sql': 'colY>10'}]
                        }
                    },
                    'sms': {
                        'BA': [],
                        'others': {'seg_sms': [{'name': 'seg_sms_chk', 'sql': 'colS=3'}]}
                    },
                    'push': {
                        'BA': [
                            {'name': 'chk_push1', 'sql': 'colP=1'},
                            {'name': 'chk_push2', 'sql': 'colP=2'}
                        ],
                        'others': {}
                    }
                }
            }
        },
        'waterfall': {
            'output_directory': str(tmp_path / 'wf'),
            'count_columns': ['t.id']
        },
        'output': {
            'channels': {
                'email': {
                    'columns': ['t.id', 'chk_main1', 'chk_main2', 'chk_email_BA', 'segX_chk1', 'segY_chk1'],
                    'file_location': str(tmp_path / 'out' / 'email'),
                    'file_base_name': 'email_out',
                    'output_options': {'format': 'csv', 'additional_arguments': {}, 'custom_function': None},
                    'unique_on': ['t.id']
                },
                'sms': {
                    'columns': ['t.id', 'chk_main1', 'seg_sms_chk'],
                    'file_location': str(tmp_path / 'out' / 'sms'),
                    'file_base_name': 'sms_out',
                    'output_options': {'format': 'parquet', 'additional_arguments': {}, 'custom_function': None},
                    'unique_on': []
                },
                'push': {
                    'columns': ['t.id', 'chk_main1', 'chk_push1', 'chk_push2'],
                    'file_location': str(tmp_path / 'out' / 'push'),
                    'file_base_name': 'push_out',
                    'output_options': {'format': 'excel', 'additional_arguments': {}, 'custom_function': None},
                    'unique_on': ['t.grp']
                }
            }
        }
    }
    # Write YAML config
    cfg_path = tmp_path / 'config.yaml'
    cfg_path.write_text(yaml.safe_dump(cfg))
    # Load into AppConfig
    app_cfg = load_config(str(cfg_path))
    assert isinstance(app_cfg, AppConfig)

    # Prepare runner and logger
    runner = DummyRunner()
    logger = DummyLogger()

    # Run eligibility
    elig_engine = EligibilityEngine(app_cfg.eligibility, runner, logger)
    elig_engine.run()
    # Should have drop and creation statements
    assert any('DROP TABLE elig_tbl' in q or 'CREATE TABLE' in q for q in runner.queries)
    # Clear for next
    runner.queries.clear()

    # Run waterfall
    # Capture Excel outputs via write_waterfall_excel
    # Patch the Excel writer to capture waterfall outputs
    import tlptaco.engines.waterfall_excel as wf_excel_mod
    wf_captured = {}
    def fake_wf_writer(conditions_df, compiled_current, output_path, *, previous=None,
                       offer_code='', campaign_planner='', lead='', current_date='', starting_pops=None):
        wf_captured.setdefault('paths', []).append(output_path)
        wf_captured['compiled'] = compiled_current
    monkeypatch.setattr(wf_excel_mod, 'write_waterfall_excel', fake_wf_writer)
    wf_engine = WaterfallEngine(app_cfg.waterfall, runner, logger)
    wf_engine.run(elig_engine)
    # Expect at least one waterfall report under wf directory
    paths = wf_captured.get('paths', [])
    assert any(str(tmp_path / 'wf') in p for p in paths)

    # Run output stage
    out_records = []
    from tlptaco.iostream import writer as io_writer
    def fake_write(df, path, fmt, **kwargs):
        out_records.append({'path': path, 'fmt': fmt, 'df': df.copy()})
    monkeypatch.setattr(io_writer, 'write_dataframe', fake_write)
    out_engine = OutputEngine(app_cfg.output, runner, logger)
    out_engine.run(elig_engine)
    # Validate outputs for each channel
    from datetime import datetime
    today = datetime.now().strftime('%Y%m%d')
    expected = {
        f'email_out_{today}.csv',
        f'sms_out_{today}.parquet',
        f'push_out_{today}.xlsx',
    }
    got = {Path(r['path']).name for r in out_records}
    assert got == expected
