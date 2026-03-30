from __future__ import annotations

from contextlib import asynccontextmanager

import json

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from dotenv import load_dotenv
import os
import redis as _redis

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.router import api_router
from app.db.base import Base
from app.db.session import engine, SessionLocal
from app.db import models  # noqa: F401
from app.core.config import settings
from app.core.rate_limit import limiter


load_dotenv()


is_prod = (getattr(settings, "ENV", "") or "").strip().lower() == "production"


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "ALLOW-FROM https://gleel2.com"
        response.headers["Content-Security-Policy"] = "frame-ancestors 'self' https://gleel2.com https://*.gleel2.com http://localhost:* http://127.0.0.1:*"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        if is_prod:
            response.headers["Strict-Transport-Security"] = (
                "max-age=63072000; includeSubDomains; preload"
            )
        return response


# ---------------------------------------------------------------------------
# Lifespan (startup + shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — create tables (idempotent, only creates missing ones)
    Base.metadata.create_all(bind=engine)
    yield
    # Shutdown: dispose connection pool so workers drain cleanly.
    engine.dispose()


app = FastAPI(
    title="Scout MVP API",
    docs_url=None if is_prod else "/docs",
    redoc_url=None if is_prod else "/redoc",
    openapi_url=None if is_prod else "/openapi.json",
    lifespan=lifespan,
)

# Rate limiting (Redis-backed via slowapi)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Security headers before CORS so they apply to all responses.
app.add_middleware(SecurityHeadersMiddleware)

cors_env = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
if cors_env == "*":
    allow_origins = ["*"]
elif cors_env:
    allow_origins = [o.strip() for o in cors_env.split(",") if o.strip()]
else:
    allow_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")


# ---------------------------------------------------------------------------
# Health check endpoints (no auth required)
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    """Liveness probe — always returns 200 if the process is running."""
    return {"status": "ok"}


@app.get("/ready")
def readiness():
    """Readiness probe — checks database and Redis connectivity."""
    errors = []

    # Database check
    try:
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
    except Exception as e:
        errors.append(f"database: {type(e).__name__}")

    # Redis check
    try:
        r = _redis.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        r.ping()
        r.close()
    except Exception as e:
        errors.append(f"redis: {type(e).__name__}")

    if errors:
        return Response(
            content=json.dumps({"status": "degraded", "errors": errors}),
            status_code=503,
            media_type="application/json",
        )
    return {"status": "ok"}

