"""Test optional column_types casting in output SQL template."""


import os


from tlptaco.sql.generator import SQLGenerator


def test_output_column_casting(tmp_path):
    """Given column_types mapping, template should emit CAST expressions."""

    # Point generator at real templates dir
    templates_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'tlptaco', 'sql', 'templates'))

    gen = SQLGenerator(templates_dir)

    ctx = {
        'columns': ['c.customer_id', 'c.email'],
        'eligibility_table': 'elig_tbl',
        'cases': [],
        'unique_on': [],
        'column_types': {
            'c.customer_id': 'VARCHAR(9)'
        }
    }

    sql = gen.render('output.sql.j2', ctx)

    # Expect CAST expression for customer_id
    assert 'CAST(c.customer_id AS VARCHAR(9)) AS customer_id' in sql
    # And plain email column remains
    assert 'c.email' in sql
