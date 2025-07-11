"""
Render SQL from Jinja2 templates with provided context.
"""
import os
try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
except ImportError:
    Environment = FileSystemLoader = select_autoescape = None

class SQLGenerator:
    def __init__(self, templates_dir: str):
        if Environment is None:
            raise ImportError("jinja2 is required to render SQL templates; please install jinja2")
        self.env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(["sql", "jinja"])
        )

    def render(self, template_name: str, context: dict) -> str:
        tmpl = self.env.get_template(template_name)
        return tmpl.render(**context)