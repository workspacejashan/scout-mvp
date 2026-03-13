from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import get_db


def _is_production() -> bool:
    return str(getattr(settings, "ENV", "") or "").strip().lower() == "production"


def require_admin(request: Request) -> None:
    """
    Minimal auth gate: validates ADMIN_API_TOKEN to prove the request
    came through the authorized frontend proxy.

    - In production: fail-closed unless ADMIN_API_TOKEN is set AND provided by the caller.
    - In development: allow missing token for convenience, but if token is set, enforce it.
    """
    # Exempt paths that receive external callbacks directly.
    if request.url.path.endswith("/sms/twilio/webhook"):
        return
    if request.url.path.endswith("/billing/stripe/webhook"):
        return

    expected = (getattr(settings, "ADMIN_API_TOKEN", "") or "").strip()
    prod = _is_production()

    if prod and not expected:
        # Fail-closed: do not accidentally run unauthenticated in production.
        raise HTTPException(status_code=500, detail="server_misconfigured:missing_ADMIN_API_TOKEN")

    if not expected:
        # Dev default: open unless you set a token.
        return

    auth = (request.headers.get("authorization") or "").strip()
    got = ""
    if auth.lower().startswith("bearer "):
        got = auth[7:].strip()
    else:
        # Optional alternate header for tooling.
        got = (request.headers.get("x-admin-token") or "").strip()

    if not got or got != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


def get_current_user_id(request: Request) -> str:
    """
    Extract user_id from x-user-id header (set by frontend middleware).
    Falls back to APP_DEFAULT_OWNER_ID for transition period (v1 sessions).
    """
    user_id = (request.headers.get("x-user-id") or "").strip()
    if not user_id:
        return settings.APP_DEFAULT_OWNER_ID
    return user_id


def get_current_user(
    request: Request, db: Session = Depends(get_db)
):
    """
    Load the full User object from DB. Auto-creates if not found
    (supports access-code auth where user is created on first API call).
    Import here to avoid circular imports at module level.
    """
    from app.db.models import User, AccountTier

    user_id = get_current_user_id(request)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        # Auto-create user for access-code auth flow
        user = User(id=user_id, email=f"{user_id}@scout.local", tier=AccountTier.unlocked)
        db.add(user)
        db.commit()
        db.refresh(user)
    return user
