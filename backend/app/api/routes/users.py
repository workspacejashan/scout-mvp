from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id, require_admin
from app.core.config import settings
from app.core.rate_limit import limiter
from app.db.models import AccountTier, User
from app.db.session import get_db

# ---------------------------------------------------------------------------
# Free email provider blocklist
# ---------------------------------------------------------------------------

FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.co.in",
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    "aol.com",
    "icloud.com", "me.com", "mac.com",
    "protonmail.com", "proton.me", "pm.me",
    "tutanota.com", "tuta.io",
    "zoho.com", "zohomail.com",
    "yandex.com", "yandex.ru",
    "mail.com", "email.com",
    "gmx.com", "gmx.net",
    "fastmail.com",
    "hey.com",
    "mailinator.com",
    "guerrillamail.com",
    "tempmail.com",
}


def _is_work_email(email: str) -> bool:
    domain = email.rsplit("@", 1)[-1].lower()
    return domain not in FREE_EMAIL_DOMAINS


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ResolveUserIn(BaseModel):
    email: str


class UnlockCodeIn(BaseModel):
    code: str


# ---------------------------------------------------------------------------
# Router — /api/users
#
# The /resolve endpoint is called during the OTP verify flow before the user
# has a session, so it is protected only by the ADMIN_API_TOKEN (which the
# frontend server injects). The other endpoints also need a user session.
# ---------------------------------------------------------------------------

router = APIRouter(dependencies=[Depends(require_admin)])


@router.post("/resolve")
@limiter.limit("10/minute")
def resolve_user(request: Request, payload: ResolveUserIn, db: Session = Depends(get_db)):
    """Get or create a user by email. Called by frontend during OTP verification."""
    email = payload.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="invalid_email")

    if not _is_work_email(email):
        raise HTTPException(status_code=400, detail="work_email_required")

    user = db.query(User).filter(User.email == email).first()
    if user:
        return {
            "id": user.id,
            "email": user.email,
            "tier": user.tier.value,
            "is_new": False,
        }

    user = User(email=email, tier=AccountTier.free)
    try:
        db.add(user)
        db.commit()
        db.refresh(user)
    except IntegrityError:
        db.rollback()
        # Race condition: another request created the user first.
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=500, detail="failed_to_create_user")

    return {
        "id": user.id,
        "email": user.email,
        "tier": user.tier.value,
        "is_new": True,
    }


@router.get("/me")
def get_me(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Return the current user's profile and tier info."""
    user = db.query(User).filter(User.id == owner_id).first()
    if not user:
        # Auto-create user for access-code auth flow
        user = User(id=owner_id, email=f"{owner_id}@scout.local", tier=AccountTier.unlocked)
        db.add(user)
        db.commit()
        db.refresh(user)
    return {
        "id": user.id,
        "email": user.email,
        "tier": user.tier.value,
        "stripe_subscription_status": user.stripe_subscription_status,
    }


@router.post("/apply-unlock-code")
@limiter.limit("5/minute")
def apply_unlock_code(
    request: Request,
    payload: UnlockCodeIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Validate the universal unlock code and upgrade the user's tier."""
    expected = (settings.UNIVERSAL_UNLOCK_CODE or "").strip()
    if not expected:
        raise HTTPException(status_code=400, detail="no_unlock_code_configured")

    if payload.code.strip() != expected:
        raise HTTPException(status_code=400, detail="invalid_code")

    user = db.query(User).filter(User.id == owner_id).first()
    if not user:
        user = User(id=owner_id, email=f"{owner_id}@scout.local", tier=AccountTier.unlocked)
        db.add(user)
        db.commit()
        db.refresh(user)

    user.tier = AccountTier.unlocked
    db.add(user)
    db.commit()
    return {"tier": user.tier.value}
