"""Tests for tab character handling in YAML loader."""

import textwrap

import pytest


import tlptaco.config.loader as loader_mod


def test_loader_replaces_tabs(monkeypatch, tmp_path):
    """A YAML with tabs but otherwise valid should load successfully."""

    # minimal stub config – we monkeypatch AppConfig.parse_obj to bypass full validation
    yaml_text = textwrap.dedent(
        """
        logging:\t   # tab after ':'
          level: INFO
        database:
          host: h
          user: u
          password: p
        eligibility:
          eligibility_table: t
          conditions:
            main:
              BA:
                - sql: "1=1"
          tables: []
          unique_identifiers: []
        waterfall:
          output_directory: out
          count_columns: []
        output:
          channels: {}
        """
    )

    cfg_path = tmp_path / "tabs.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")

    # Patch AppConfig.parse_obj to a no-op returning dict (focus on loader)
    monkeypatch.setattr(loader_mod.AppConfig, "parse_obj", staticmethod(lambda d: d))

    cfg = loader_mod.load_config(str(cfg_path))
    assert isinstance(cfg, dict)


def test_loader_tabs_validation_error(monkeypatch, tmp_path):
    """Validation error should include advisory when tabs present."""

    yaml_text = "logging:\t\n  level: INFO\n"  # missing many required keys + tab
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(yaml_text)

    # Build a fake ValidationError to raise from parse_obj
    from pydantic import BaseModel, ValidationError

    class _Dummy(BaseModel):
        a: int

    def _raise(_):
        # Trigger a ValidationError by validating bad data
        try:
            _Dummy.model_validate({})  # missing required field 'a'
        except ValidationError as e:
            raise e

    monkeypatch.setattr(loader_mod.AppConfig, "parse_obj", staticmethod(_raise))

    with pytest.raises(ValueError) as exc:
        loader_mod.load_config(str(cfg_path))

    msg = str(exc.value)
    assert 'Replace all tab characters with spaces' in msg
