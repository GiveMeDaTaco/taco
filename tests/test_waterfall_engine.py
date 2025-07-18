import os
import pandas as pd
import pytest

from tlptaco.engines.waterfall import WaterfallEngine
from tlptaco.config.schema import (
    WaterfallConfig,
    EligibilityConfig,
    ConditionsConfig,
    TemplateConditions,
    TableConfig,
    ConditionCheck,
)


class DummyEligEngine:
    """Dummy eligibility engine stub carrying only cfg."""
    def __init__(self, cfg):
        self.cfg = cfg


def make_elig_config():
    # Main BA with no segments
    main = TemplateConditions(BA=[ConditionCheck(sql='1=1')], others={})
    # Channel with one BA check and one non-BA segment
    channel = TemplateConditions(
        BA=[ConditionCheck(sql='1=1')],
        others={'seg1': [ConditionCheck(sql='1=1')]}  # a single segment
    )
    cond = ConditionsConfig(main=main, channels={'email': channel})
    # Single table alias 't'
    table = TableConfig(
        name='t_main', alias='t', sql=None,
        join_type=None, join_conditions=None,
        where_conditions=None, unique_index=None,
        collect_stats=None
    )
    return EligibilityConfig(
        eligibility_table='elig_tbl',
        conditions=cond,
        tables=[table],
        unique_identifiers=['t.id']
    )


def test_prepare_waterfall_steps_basic():
    elig_cfg = make_elig_config()
    wf_cfg = WaterfallConfig(output_directory='out_dir', count_columns=['t.id'])
    engine = WaterfallEngine(wf_cfg, runner=None, logger=None)
    dummy = DummyEligEngine(elig_cfg)
    # Prepare steps
    engine._prepare_waterfall_steps(dummy)
    groups = engine._waterfall_groups
    # One group for one count_columns entry
    assert isinstance(groups, list) and len(groups) == 1
    grp = groups[0]
    assert grp['name'] == 'id'
    # Expect 3 jobs: Base (standard), email-BA (standard), email segments
    types = [job['type'] for job in grp['jobs']]
    assert types == ['standard', 'standard', 'segments']
    # Output path should include output_directory and group name
    assert 'out_dir' in grp['output_path'] and 'id' in grp['output_path']


def test_pivot_waterfall_df():
    # Build a raw DataFrame with two metrics for a single check
    df_raw = pd.DataFrame([
        {'section': 'Base', 'check_name': 'chkA', 'stat_name': 'unique_drops', 'cntr': 5},
        {'section': 'Base', 'check_name': 'chkA', 'stat_name': 'remaining', 'cntr': 3},
    ])
    wf_cfg = WaterfallConfig(output_directory='out_dir', count_columns=['t.id'])
    engine = WaterfallEngine(wf_cfg, runner=None, logger=None)
    # Pivot
    pivoted = engine._pivot_waterfall_df(df_raw, 'Base')
    # Check columns
    expected_cols = {'check_name', 'unique_drops', 'remaining', 'section'}
    assert set(pivoted.columns) == expected_cols
    # Check values
    row = pivoted[pivoted['check_name'] == 'chkA'].iloc[0]
    assert row['unique_drops'] == 5
    assert row['remaining'] == 3
    assert row['section'] == 'Base'


def test_pivot_empty_dataframe():
    # Empty input should yield empty DataFrame
    df_empty = pd.DataFrame(columns=['section', 'check_name', 'stat_name', 'cntr'])
    engine = WaterfallEngine(
        WaterfallConfig(output_directory='x', count_columns=['a']),
        runner=None, logger=None
    )
    out = engine._pivot_waterfall_df(df_empty, 'Any')
    assert isinstance(out, pd.DataFrame) and out.empty