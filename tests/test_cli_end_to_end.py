"""
End-to-end smoke test for the tlptaco CLI with a dummy DBRunner.
"""
import sys
import os
import yaml
import pandas as pd
import pytest
from pathlib import Path

import tlptaco.cli as cli_mod


@pytest.fixture(autouse=True)
def patch_spinner_and_progress(monkeypatch):
    # Disable loading spinner and progress manager
    class DummySpinner:
        def start(self): pass
        def stop(self): pass
    monkeypatch.setattr('tlptaco.utils.loading_bar.LoadingSpinner', DummySpinner)
    monkeypatch.setattr('tlptaco.utils.loading_bar.ProgressManager', DummySpinner)
    yield

class DummyRunner:
    """Stub DBRunner that collects SQL and returns dummy DataFrames."""
    def __init__(self, *args, **kwargs):
        self.queries = []
    def run(self, sql):
        # Capture DDL and DML
        self.queries.append(sql)
    def to_df(self, sql):
        # Return a simple long-format table for waterfall and output
        records = []
        # Two checks for base waterfall
        for chk in ['main_BA_1', 'main_BA_2']:
            for stat in ['unique_drops', 'incremental_drops', 'cumulative_drops', 'regain', 'remaining']:
                records.append({'section': 'Base', 'check_name': chk, 'stat_name': stat, 'cntr': 1})
        df = pd.DataFrame(records)
        return df
    def cleanup(self): pass

@pytest.fixture(autouse=True)
def patch_sqlgenerator(monkeypatch):
    # Stub out Jinja SQL rendering
    from tlptaco.sql.generator import SQLGenerator as RealGen
    class FakeGen(RealGen):
        def render(self, template_name, context):
            return "SELECT * FROM dummy;"
    import tlptaco.engines.eligibility as elig_mod
    import tlptaco.engines.waterfall as wf_mod
    import tlptaco.engines.output as out_mod
    monkeypatch.setattr(elig_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(wf_mod, 'SQLGenerator', FakeGen)
    monkeypatch.setattr(out_mod, 'SQLGenerator', FakeGen)
    yield

@pytest.fixture(autouse=True)
def patch_dbrunner(monkeypatch):
    # Replace DBRunner in CLI with DummyRunner
    monkeypatch.setattr(cli_mod, 'DBRunner', DummyRunner)
    yield

@pytest.fixture
def capture_writes(monkeypatch):
    # Capture waterfall Excel writes and output writes
    import tlptaco.engines.waterfall_excel as wf_excel_mod
    import tlptaco.iostream.writer as io_writer

    wf_captured = {'paths': [], 'compiled': None}
    def fake_wf_writer(conditions_df, compiled, output_path, *args, **kwargs):
        wf_captured['paths'].append(output_path)
        wf_captured['compiled'] = compiled
    monkeypatch.setattr(wf_excel_mod, 'write_waterfall_excel', fake_wf_writer)

    out_captured = {'records': []}
    def fake_write(df, path, fmt, **kwargs):
        out_captured['records'].append({'path': path, 'fmt': fmt})
    monkeypatch.setattr(io_writer, 'write_dataframe', fake_write)

    return wf_captured, out_captured

def test_cli_end_to_end(tmp_path, capture_writes):
    wf_captured, out_captured = capture_writes
    # Build a minimal config
    cfg = {
        'logging': {'level': 'INFO', 'file': None, 'debug_file': None},
        'database': {'host': 'h', 'user': 'u', 'password': 'p', 'logmech': None},
        'eligibility': {
            'eligibility_table': 'elig_tbl',
            'unique_identifiers': ['t.id'],
            'tables': [
                {'name': 't', 'alias': 't', 'sql': None,
                 'join_type': None, 'join_conditions': None,
                 'where_conditions': None, 'unique_index': None, 'collect_stats': None}
            ],
            'conditions': {
                'main': {'BA': [{'sql': '1=1'}], 'others': {}},
                'channels': {'default': {'BA': [{'sql': '1=1'}], 'others': {}}}
            }
        },
        'waterfall': {'output_directory': 'wf', 'count_columns': ['t.id']},
        'output': {'channels': {
            'default': {
                'columns': ['t.id'],
                'file_location': 'out',
                'file_base_name': 'out',
                'output_options': {'format': 'csv', 'additional_arguments': {}, 'custom_function': None},
                'unique_on': []
            }
        }}
    }
    # Write YAML config
    cfg_path = tmp_path / 'config.yaml'
    cfg_path.write_text(yaml.safe_dump(cfg))
    # Run CLI
    sys.argv = ['prog', '--config', str(cfg_path), '--output-dir', str(tmp_path), '--mode', 'full']
    cli_mod.main()

    # Verify DBRunner ran DDL for eligibility
    # DummyRunner tracked via cfg? We cannot inspect runner here, but ensure writes happened
    # Check waterfall writes
    assert wf_captured['paths'], "Waterfall writer was not called"
    # All paths should start with tmp_path/wf
    for p in wf_captured['paths']:
        assert str(tmp_path / 'wf') in p

    # Check output writes
    recs = out_captured['records']
    assert recs, "Output writer was not called"
    # Only one channel 'default' -> one record
    assert len(recs) == 1
    rec = recs[0]
    # CSV extension, under tmp_path/out
    assert rec['fmt'] == 'csv'
    assert str(tmp_path / 'out') in rec['path']
    assert rec['path'].endswith('.csv')