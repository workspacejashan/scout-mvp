from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import ProfileEnrichment, SmsMessageStatus, SmsOptOut
from app.services.phones import normalize_us_phone_e164


PHONE_PROVIDER_PRIORITY = [
    "scrapedo_tps",
    "scrapedo_advancedbackgroundchecks",
    "scrapedo_usphonebook",
]


@dataclass(frozen=True)
class SelectedPhone:
    phone_e164: str
    provider: str
    raw: str


def select_best_phone_for_profile(
    enrichments: list[ProfileEnrichment],
) -> Optional[SelectedPhone]:
    """
    Choose the first valid US E.164 phone from enrichments by provider priority.
    """
    by_provider: dict[str, list[ProfileEnrichment]] = {}
    for e in enrichments:
        p = (e.provider or "").strip().lower()
        by_provider.setdefault(p, []).append(e)

    for provider in PHONE_PROVIDER_PRIORITY:
        recs = by_provider.get(provider) or []
        # Prefer most recently finished
        recs = sorted(recs, key=lambda r: (r.finished_at or datetime.min), reverse=True)
        for r in recs:
            for raw in (r.phone_numbers or []):
                p = normalize_us_phone_e164(raw or "")
                if p:
                    return SelectedPhone(phone_e164=p, provider=provider, raw=str(raw))
    return None


def is_opted_out(db: Session, *, owner_id: str, phone_e164: str) -> bool:
    return (
        db.query(SmsOptOut.id)
        .filter(
            SmsOptOut.owner_id == owner_id,
            SmsOptOut.phone_e164 == phone_e164,
            SmsOptOut.revoked_at.is_(None),
        )
        .first()
        is not None
    )


def is_in_cooldown(db: Session, *, owner_id: str, phone_e164: str, now: datetime) -> bool:
    """
    Cooldown is based on last *sent* outbound message.
    """
    cutoff = now - timedelta(days=int(settings.SMS_COOLDOWN_DAYS or 14))
    from app.db.models import SmsOutboundMessage  # avoid circular import at module load

    last = (
        db.query(func.max(SmsOutboundMessage.sent_at))
        .filter(
            SmsOutboundMessage.owner_id == owner_id,
            SmsOutboundMessage.to_phone_e164 == phone_e164,
            SmsOutboundMessage.status == SmsMessageStatus.sent,
            SmsOutboundMessage.sent_at.isnot(None),
            SmsOutboundMessage.sent_at >= cutoff,
        )
        .scalar()
    )
    return last is not None


def render_sms_template(template_text: str, *, first_name: str, job_name: str, job_location: str, recruiter_company: str) -> str:
    t = template_text or ""
    # Simple placeholder replacement
    t = t.replace("{first_name}", first_name or "")
    t = t.replace("{job_name}", job_name or "")
    t = t.replace("{job_location}", job_location or "")
    t = t.replace("{recruiter_company}", recruiter_company or "")
    return t.strip()

