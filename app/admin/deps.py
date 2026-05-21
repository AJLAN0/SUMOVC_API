from pathlib import Path
from typing import Any

from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from app.admin.errors import explain_error, humanize_error, humanize_error_block
from app.admin.flash import pop_flashes

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

templates.env.filters["humanize_error"] = humanize_error
templates.env.filters["explain_error"] = explain_error
templates.env.globals["humanize_error_block"] = humanize_error_block


def render_admin(
    request: Request,
    template_name: str,
    context: dict[str, Any],
    status_code: int = 200,
):
    ctx = {**context, "flashes": pop_flashes(request)}
    return templates.TemplateResponse(request, template_name, ctx, status_code=status_code)
