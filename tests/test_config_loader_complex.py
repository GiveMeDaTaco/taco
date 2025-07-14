import yaml
import pytest
from pathlib import Path

from tlptaco.config.loader import load_config
from tlptaco.config.schema import AppConfig

@pytest.mark.parametrize('format', ['yaml', 'json'])
def test_complex_config_loading(tmp_path, format):
    # Create a complex campaign config with multiple channels and segments
    cfg = {
        'logging': {'level': 'DEBUG', 'file': None, 'debug_file': None},
        'database': {'host': 'h', 'user': 'u', 'password': 'p', 'logmech': None},
        'eligibility': {
            'eligibility_table': 'elig_tbl',
            'unique_identifiers': ['t.id', 't.grp'],
            'tables': [
                {'name': 't_main', 'alias': 't', 'join_type': '', 'join_conditions': '', 'where_conditions': 't.active=1', 'unique_index': None, 'collect_stats': None}
            ],
            'conditions': {
                'main': {'BA': [{'name': 'chk1', 'sql': 'col1=1'}], 'others': {}},
                'channels': {
                    'email': {
                        'BA': [{'name': 'chk2', 'sql': "col2='x'"}],
                        'others': {
                            'segA': [{'name': 'segA_c1', 'sql': 'colA>5'}],
                            'segB': [{'name': 'segB_c1', 'sql': 'colB>10'}]
                        }
                    },
                    'sms': {
                        'BA': [{'name': 'chk3', 'sql': "col3='y'"}],
                        'others': {}
                    }
                }
            }
        },
        'waterfall': {
            'output_directory': str(tmp_path / 'wf'),
            'count_columns': ['t.id', ['t.id', 't.grp']]
        },
        'output': {
            'channels': {
                'email': {
                    'columns': ['t.id', 'chk1', 'chk2', 'segA_c1'],
                    'file_location': str(tmp_path / 'out' / 'email'),
                    'file_base_name': 'email_out',
                    'output_options': {'format': 'csv', 'additional_arguments': {}, 'custom_function': None},
                    'unique_on': ['t.id']
                },
                'sms': {
                    'columns': ['t.id', 'chk1', 'chk3'],
                    'file_location': str(tmp_path / 'out' / 'sms'),
                    'file_base_name': 'sms_out',
                    'output_options': {'format': 'csv', 'additional_arguments': {}, 'custom_function': None},
                    'unique_on': []
                }
            }
        }
    }
    # Write to file
    path = tmp_path / f'config.{format}'
    if format == 'yaml':
        path.write_text(yaml.safe_dump(cfg))
    else:
        import json
        path.write_text(json.dumps(cfg))
    # Load config
    app_cfg = load_config(str(path))
    assert isinstance(app_cfg, AppConfig)
    # Check eligibility
    elig = app_cfg.eligibility
    assert elig.eligibility_table == 'elig_tbl'
    assert elig.unique_identifiers == ['t.id', 't.grp']
    # Check channels
    chans = elig.conditions.channels
    assert set(chans.keys()) == {'email', 'sms'}
    # email has two segments
    email_others = chans['email'].others
    assert set(email_others.keys()) == {'segA', 'segB'}
    # waterfall
    wf = app_cfg.waterfall
    assert wf.output_directory.endswith('wf')
    assert wf.count_columns[0] == 't.id'
    assert isinstance(wf.count_columns[1], list)
    # output
    out = app_cfg.output.channels
    assert 'email' in out and 'sms' in out
    email_cfg = out['email']
    assert email_cfg.columns == ['t.id', 'chk1', 'chk2', 'segA_c1']
    # unique_on propagated
    assert email_cfg.unique_on == ['t.id']
