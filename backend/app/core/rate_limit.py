from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings


def _key_func(request):
    """Rate-limit key: prefer x-user-id header, fall back to IP."""
    user_id = (request.headers.get("x-user-id") or "").strip()
    if user_id:
        return f"user:{user_id}"
    return get_remote_address(request)


# Use Redis as the storage backend so limits persist across restarts
# and work with multiple instances.
limiter = Limiter(
    key_func=_key_func,
    storage_uri=settings.REDIS_URL,
    strategy="fixed-window",
)
