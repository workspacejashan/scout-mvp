from __future__ import annotations

from celery import Celery

from app.core.config import settings


celery_app = Celery(
    "scout_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.task_track_started = True
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_acks_late = True

celery_app.autodiscover_tasks(["app.worker"])

# Celery CLI autodiscovery convenience
celery = celery_app
app = celery_app

