from jinja2 import Environment, FileSystemLoader

from settings import settings


class TemplateManager:
    def __init__(self):
        self.templates_dir = settings.BASE_DIR / "templates"
        self.env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            autoescape=True
        )

    def get_template(self, template_name: str):
        return self.env.get_template(template_name)


template_manager = TemplateManager()