import os
import tlptaco.sql.generator as mod
from tlptaco.sql.generator import SQLGenerator

def test_basic_rendering(tmp_path):
    # Create a simple SQL template and ensure render returns raw content
    tmpl_dir = tmp_path
    tmpl_file = tmpl_dir / "simple.sql.j2"
    tmpl_file.write_text("SELECT 1;")
    gen = SQLGenerator(str(tmpl_dir))
    rendered = gen.render('simple.sql.j2', {})
    # Rendered SQL should match the template exactly
    assert rendered.strip() == "SELECT 1;"

def test_list_templates_filtering(tmp_path):
    # Create mixed files in templates
    names = ["a.sql.j2", "b.sql.j2", "ignore.txt"]
    for name in names:
        (tmp_path / name).write_text("")
    gen = SQLGenerator(str(tmp_path))
    all_tpls = gen.list_templates()
    # Only .sql.j2 files are listed by default
    assert set(all_tpls) == {"a.sql.j2", "b.sql.j2"}
    # filter_func works
    filtered = gen.list_templates(filter_func=lambda n: n.startswith('b'))
    assert filtered == ['b.sql.j2']
