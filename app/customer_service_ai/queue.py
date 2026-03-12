from __future__ import annotations

from celery import Celery

from .config import get_customer_service_settings

settings = get_customer_service_settings()

celery_app = Celery(
    "customer_service_ai",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    task_track_started=True,
)
