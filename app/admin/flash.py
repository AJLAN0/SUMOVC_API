"""Session flash messages for admin HTML pages."""

from __future__ import annotations

from typing import Any

from starlette.requests import Request


def _get_flashes(request: Request) -> list[dict[str, Any]]:
    return list(request.session.get("flashes") or [])


def add_flash(request: Request, level: str, message: str, *, hint: str | None = None, title: str | None = None) -> None:
    flashes = _get_flashes(request)
    flashes.append({"level": level, "message": message, "hint": hint, "title": title})
    request.session["flashes"] = flashes


def flash_success(request: Request, message: str, hint: str | None = None) -> None:
    add_flash(request, "success", message, hint=hint)


def flash_error(request: Request, message: str, hint: str | None = None, title: str | None = None) -> None:
    add_flash(request, "error", message, hint=hint, title=title)


def flash_warning(request: Request, message: str, hint: str | None = None) -> None:
    add_flash(request, "warning", message, hint=hint)


def pop_flashes(request: Request) -> list[dict[str, Any]]:
    flashes = _get_flashes(request)
    request.session["flashes"] = []
    return flashes
