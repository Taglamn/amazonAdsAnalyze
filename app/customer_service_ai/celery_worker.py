from __future__ import annotations

from .queue import celery_app

# Ensure task decorators are registered when the worker starts.
from . import tasks as _tasks  # noqa: F401

__all__ = ["celery_app"]
