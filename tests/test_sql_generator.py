import os
import tempfile
import pytest

from tlptaco.sql.generator import SQLGenerator


def test_no_autoescape(tmp_path):
    # Create a simple SQL template containing special HTML-like characters
    tmpl_dir = tmp_path
    tmpl_file = tmpl_dir / "test.sql.j2"
    tmpl_file.write_text("SELECT '<tag>' AS col;")
    gen = SQLGenerator(str(tmpl_dir))
    rendered = gen.render('test.sql.j2', {})
    # Ensure that '<tag>' is not escaped
    assert "<tag>" in rendered


def test_list_templates_filters(tmp_path):
    # Create several files, only .sql.j2 should be listed
    for name in ["a.sql.j2", "b.sql.j2", "c.txt"]:
        (tmp_path / name).write_text("")
    gen = SQLGenerator(str(tmp_path))
    all_templates = gen.list_templates()
    assert 'a.sql.j2' in all_templates and 'b.sql.j2' in all_templates
    assert 'c.txt' not in all_templates

    # Test filter_func argument
    filtered = gen.list_templates(filter_func=lambda n: n.startswith('b'))
    assert filtered == ['b.sql.j2']
