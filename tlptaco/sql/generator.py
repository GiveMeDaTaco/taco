"""
Render SQL from Jinja2 templates with provided context.
"""
import os
try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
except ImportError:
    Environment = FileSystemLoader = select_autoescape = None

class SQLGenerator:
    """Render Jinja2 SQL templates.

    The generator keeps a dedicated Jinja *Environment* pointing at the
    ``sql/templates`` directory so that engines only need to pass a context
    dict and a template file name.

    Example
    -------
    >>> gen = SQLGenerator('/path/to/templates')
    >>> sql = gen.render('eligibility.sql.j2', {'table': 't'})
    >>> print(sql[:60])
    """
    def __init__(self, templates_dir: str):
        if Environment is None:
            raise ImportError("jinja2 is required to render SQL templates; please install jinja2")
        # Prepare Jinja environment
        self.env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(["sql", "jinja"])
        )
        # No version or commit tracking in SQL generation (removed per user request)

    def render(self, template_name: str, context: dict) -> str:
        """
        Render the named SQL template with the provided context and return raw SQL.
        """
        tmpl = self.env.get_template(template_name)
        return tmpl.render(**context)

    def list_templates(self, filter_func=None) -> list[str]:  # noqa: F821
        """
        List available SQL templates in the environment.
        By default, only files ending with '.sql.j2' are returned.
        Optionally, apply a filter_func(name) to further filter template names.
        """
        templates = [name for name in self.env.list_templates()
                     if name.endswith('.sql.j2')]
        if filter_func:
            templates = [name for name in templates if filter_func(name)]
        return templates
