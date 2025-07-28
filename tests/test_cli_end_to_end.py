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
    # Patch configure_logging to no-op to avoid touching filesystem
    def dummy_configure_logging(cfg, verbose=False):
        import logging
        logger = logging.getLogger('dummy')
        logger.addHandler(logging.NullHandler())
        return logger
    monkeypatch.setattr(cli_mod, 'configure_logging', dummy_configure_logging)
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
    def fake_wf_writer(conditions_df, compiled_current, output_path, *, previous=None, **kwargs):
        wf_captured['paths'].append(output_path)
        wf_captured['compiled'] = compiled_current
    monkeypatch.setattr(wf_excel_mod, 'write_waterfall_excel', fake_wf_writer)

    out_captured = {'records': []}
    def fake_write(df, path, fmt, **kwargs):
        out_captured['records'].append({'path': path, 'fmt': fmt})
    monkeypatch.setattr(io_writer, 'write_dataframe', fake_write)

    return wf_captured, out_captured

def test_cli_end_to_end(tmp_path, capture_writes):
    wf_captured, out_captured = capture_writes
    # ------------------------------------------------------------------
    # Load the full POC YAML and tweak logging paths to avoid filesystem
    # issues during the test run.
    # ------------------------------------------------------------------
    poc_path = Path(__file__).resolve().parent.parent / "example_campaign_poc.yaml"
    cfg_obj = yaml.safe_load(poc_path.read_text())

    # Simplify logging so configure_logging doesn't attempt to write files
    cfg_obj['logging']['file'] = None
    cfg_obj['logging']['debug_file'] = None
    cfg_obj['logging'].pop('sql_file', None)

    # Write tweaked YAML into the temp directory
    cfg_path = tmp_path / 'config.yaml'
    cfg_path.write_text(yaml.safe_dump(cfg_obj))
    # Run CLI
    sys.argv = ['prog', '--config', str(cfg_path), '--output-dir', str(tmp_path), '--mode', 'full']
    cli_mod.main()

    # Verify Waterfall writer was called and under expected directory
    assert wf_captured['paths'], "Waterfall writer was not called"
    expected_wf_prefix = tmp_path / 'reports' / 'poc' / 'waterfall'
    for p in wf_captured['paths']:
        assert str(expected_wf_prefix) in p

    # Check output writes for the three channels
    recs = out_captured['records']
    assert len(recs) == 3, "Expected 3 output channels (email, sms, push)"

    expected_suffixes = {
        'email_list.csv',
        'sms_list.parquet',
        'push_list.xlsx',
    }
    got_suffixes = {Path(r['path']).name for r in recs}
    assert got_suffixes == expected_suffixes