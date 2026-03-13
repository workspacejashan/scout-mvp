from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import AccountTier, Job, JobStatus, User


def check_can_create_job(user: User, db: Session) -> None:
    """Raise 403 if a free-tier user has hit their job limit."""
    if user.tier != AccountTier.free:
        return
    count = (
        db.query(Job)
        .filter(Job.owner_id == user.id, Job.status == JobStatus.active)
        .count()
    )
    if count >= settings.FREE_TIER_MAX_JOBS:
        raise HTTPException(
            status_code=403,
            detail=f"Free tier limit: max {settings.FREE_TIER_MAX_JOBS} active jobs. Upgrade to Pro for unlimited.",
        )


def check_paid_feature(user: User) -> None:
    """Raise 403 if a free-tier user tries to access a paid-only feature."""
    if user.tier != AccountTier.free:
        return
    raise HTTPException(
        status_code=403,
        detail="This feature requires a Pro subscription or an unlock code.",
    )
