from __future__ import annotations

import asyncio
import html as _html
import json as _json
import random
import re
import urllib.parse
from collections import defaultdict
from datetime import datetime
from typing import Optional

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import (
    DroppedProfile,
    EnrichmentStatus,
    Job,
    JobSettings,
    JobProfile,
    JobProfilePin,
    LocationVariant,
    Profile,
    ProfileEnrichment,
    SmsBatch,
    SmsBatchStatus,
    SmsMessageStatus,
    SmsOptOut,
    SmsOutboundMessage,
    StrategyRun,
    StrategyRunStatus,
    TitleVariant,
)
from app.db.session import SessionLocal
from app.services.boolean_canonical import boolean_matches_text, parse_boolean
from app.services.cse_llm_extract import extract_profiles_from_cse_items
from app.services.normalize import (
    clean_person_name,
    extract_location_city_state,
    extract_name_from_title,
    is_linkedin_in_url,
    normalize_linkedin_url,
    split_first_last,
)
from app.services.timezone import state_to_timezone
from app.services.twilio import TwilioError, is_configured as twilio_is_configured, send_sms
from app.worker.celery_app import celery_app


def _utcnow() -> datetime:
    return datetime.utcnow()


def _profile_has_any_phone(db: Session, *, owner_id: str, profile_id: str) -> bool:
    """
    True if we have at least one stored phone number for this profile (any provider).
    """
    rows = (
        db.query(ProfileEnrichment.phone_numbers)
        .filter(ProfileEnrichment.owner_id == owner_id, ProfileEnrichment.profile_id == profile_id)
        .all()
    )
    for (nums,) in rows:
        try:
            if nums and isinstance(nums, list) and len(nums) > 0:
                return True
        except Exception:
            continue
    return False


_CHAIN_ENRICH_PROVIDERS = [
    "scrapedo_tps",
    "scrapedo_advancedbackgroundchecks",
    "scrapedo_usphonebook",
]


def _profile_has_chain_job(db: Session, *, owner_id: str, profile_id: str) -> bool:
    row = (
        db.query(JobProfile.job_id)
        .join(JobSettings, (JobSettings.job_id == JobProfile.job_id) & (JobSettings.owner_id == JobProfile.owner_id))
        .filter(
            JobProfile.owner_id == owner_id,
            JobProfile.profile_id == profile_id,
            JobSettings.auto_enrich_enabled == True,  # noqa: E712
            JobSettings.auto_enrich_provider == "chain",
        )
        .first()
    )
    return bool(row)


def queue_chain_for_profiles(db: Session, *, owner_id: str, profile_ids: list[str]) -> dict:
    """
    Queue the next chain provider for each profile.
    Chain order: TPS -> ABC -> USPhonebook.
    """
    deduped = list(dict.fromkeys([p for p in (profile_ids or []) if p]))
    if not deduped:
        return {"queued": 0, "skipped_has_phones": 0, "skipped_in_flight": 0}

    queued = 0
    skipped_has_phones = 0
    skipped_in_flight = 0

    for pid in deduped:
        if _profile_has_any_phone(db, owner_id=owner_id, profile_id=pid):
            skipped_has_phones += 1
            continue

        recs = (
            db.query(ProfileEnrichment)
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.profile_id == pid,
                ProfileEnrichment.provider.in_(_CHAIN_ENRICH_PROVIDERS),
            )
            .all()
        )
        by_provider = {r.provider: r for r in recs}

        for provider in _CHAIN_ENRICH_PROVIDERS:
            rec = by_provider.get(provider)
            if not rec:
                db.add(
                    ProfileEnrichment(
                        owner_id=owner_id,
                        profile_id=pid,
                        provider=provider,
                        status=EnrichmentStatus.queued,
                    )
                )
                queued += 1
                break

            if rec.status in (EnrichmentStatus.queued, EnrichmentStatus.running):
                skipped_in_flight += 1
                break

            # completed/failed with no phones -> continue to next provider

    if queued:
        db.commit()

    return {
        "queued": queued,
        "skipped_has_phones": skipped_has_phones,
        "skipped_in_flight": skipped_in_flight,
    }


@celery_app.task(name="backfill_profile_timezones")
def backfill_profile_timezones(
    owner_id: Optional[str] = None,
    *,
    batch_size: int = 1000,
    dry_run: bool = False,
) -> dict:
    """
    Backfill profiles.timezone from profiles.state (US-only).

    Stores IANA timezone strings like "America/Chicago".
    Multi-timezone states use the most-populated timezone.
    """
    owner = (owner_id or "").strip()
    batch_size = max(int(batch_size or 0), 1)

    updated = 0
    skipped_no_state = 0
    skipped_unknown_state = 0
    scanned = 0

    with SessionLocal() as db:
        while True:
            rows = (
                db.query(Profile)
                .filter(Profile.owner_id == owner)
                .filter((Profile.timezone.is_(None)) | (Profile.timezone == ""))
                .order_by(Profile.created_at.asc())
                .limit(batch_size)
                .all()
            )
            if not rows:
                break

            for p in rows:
                scanned += 1
                st = (p.state or "").strip()
                if not st:
                    skipped_no_state += 1
                    continue

                tz = state_to_timezone(st)
                if not tz:
                    skipped_unknown_state += 1
                    continue

                p.timezone = tz
                db.add(p)
                updated += 1

            if dry_run:
                db.rollback()
                break
            db.commit()

    return {
        "owner_id": owner,
        "dry_run": bool(dry_run),
        "batch_size": batch_size,
        "scanned": scanned,
        "updated": updated,
        "skipped_no_state": skipped_no_state,
        "skipped_unknown_state": skipped_unknown_state,
    }


@celery_app.task(name="prune_bad_profiles")
def prune_bad_profiles(
    owner_id: Optional[str] = None,
    *,
    limit: int = 5000,
    batch_size: int = 500,
    dry_run: bool = True,
    include_pinned: bool = False,
    max_scan: int = 200000,
) -> dict:
    """
    Delete profiles that are clearly bad / unusable for enrichment + outreach.

    "Bad" is defined using the same internal heuristics used to decide whether we should
    repair/overwrite stored fields:
    - _bad_profile_name(full_name_raw)
    - _bad_profile_city(city, full_name_raw)
    - _bad_profile_state(state)

    Safety:
    - By default, pinned uploads are NOT deleted (include_pinned=False).
    - FK-safe delete order: JobProfilePin -> JobProfile -> ProfileEnrichment -> Profile
    - dry_run=True returns counts + samples without deleting.
    """
    owner = (owner_id or "").strip()
    limit = max(int(limit or 0), 0)
    batch_size = min(max(int(batch_size or 0), 1), 5000)
    max_scan = max(int(max_scan or 0), 0)

    scanned = 0
    candidates = 0
    deleted = 0
    skipped_pinned = 0
    reasons: dict[str, int] = defaultdict(int)
    sample: list[dict] = []

    last_created_at: Optional[datetime] = None
    last_id: Optional[str] = None

    with SessionLocal() as db:
        while True:
            if max_scan and scanned >= max_scan:
                break
            if limit and not dry_run and deleted >= limit:
                break

            q = db.query(Profile).filter(Profile.owner_id == owner).order_by(Profile.created_at.asc(), Profile.id.asc())
            if last_created_at is not None and last_id is not None:
                q = q.filter(
                    (Profile.created_at > last_created_at)
                    | ((Profile.created_at == last_created_at) & (Profile.id > last_id))
                )

            rows = q.limit(batch_size).all()
            if not rows:
                break

            last_created_at = rows[-1].created_at
            last_id = rows[-1].id

            candidate_ids: list[str] = []

            for p in rows:
                scanned += 1
                if max_scan and scanned > max_scan:
                    break

                # Skip pinned uploads by default (explicit user intent).
                if not include_pinned:
                    is_pinned = (
                        db.query(JobProfilePin.id)
                        .filter(
                            JobProfilePin.owner_id == owner,
                            JobProfilePin.profile_id == p.id,
                        )
                        .first()
                        is not None
                    )
                    if is_pinned:
                        skipped_pinned += 1
                        continue

                bad_name = _bad_profile_name(getattr(p, "full_name_raw", "") or "")
                bad_city = _bad_profile_city(getattr(p, "city", "") or "", getattr(p, "full_name_raw", "") or "")
                bad_state = _bad_profile_state(getattr(p, "state", "") or "")

                if not (bad_name or bad_city or bad_state):
                    continue

                candidates += 1
                if bad_name:
                    reasons["bad_name"] += 1
                if bad_city:
                    reasons["bad_city"] += 1
                if bad_state:
                    reasons["bad_state"] += 1

                if len(sample) < 25:
                    sample.append(
                        {
                            "id": p.id,
                            "name": (p.full_name_raw or "")[:120],
                            "city": (p.city or "")[:120],
                            "state": (p.state or "")[:120],
                            "bad_name": bad_name,
                            "bad_city": bad_city,
                            "bad_state": bad_state,
                        }
                    )

                if not dry_run:
                    if not limit or deleted + len(candidate_ids) < limit:
                        candidate_ids.append(p.id)

            if dry_run:
                continue

            if not candidate_ids:
                continue

            # FK-safe delete ordering
            db.query(JobProfilePin).filter(
                JobProfilePin.owner_id == owner, JobProfilePin.profile_id.in_(candidate_ids)
            ).delete(synchronize_session=False)
            db.query(JobProfile).filter(
                JobProfile.owner_id == owner, JobProfile.profile_id.in_(candidate_ids)
            ).delete(synchronize_session=False)
            db.query(ProfileEnrichment).filter(
                ProfileEnrichment.owner_id == owner, ProfileEnrichment.profile_id.in_(candidate_ids)
            ).delete(synchronize_session=False)
            db.query(Profile).filter(Profile.owner_id == owner, Profile.id.in_(candidate_ids)).delete(
                synchronize_session=False
            )
            db.commit()

            deleted += len(candidate_ids)

    return {
        "owner_id": owner,
        "dry_run": bool(dry_run),
        "include_pinned": bool(include_pinned),
        "limit": limit,
        "batch_size": batch_size,
        "max_scan": max_scan,
        "scanned": scanned,
        "candidates": candidates,
        "deleted": deleted,
        "skipped_pinned": skipped_pinned,
        "reasons": dict(reasons),
        "sample": sample,
    }


@celery_app.task(name="send_sms_batch")
def send_sms_batch(batch_id: str, *, max_to_send: int = 200) -> dict:
    """
    Send approved SMS messages for a batch.

    Constraints enforced:
    - Only send messages in SmsMessageStatus.approved
    - Skip if recipient not currently within business hours (7am–7pm profile local time)
    - Respect opt-outs and cooldown (rechecked at send time)
    - Respect daily limits (simple UTC-day counting; v1)
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    now = datetime.utcnow()

    sent = 0
    skipped_hours = 0
    skipped_opt_out = 0
    skipped_cooldown = 0
    skipped_limits = 0
    failed = 0

    max_to_send = max(int(max_to_send or 0), 1)

    with SessionLocal() as db:
        batch = db.get(SmsBatch, batch_id)
        if not batch:
            return {"ok": False, "error": "batch_not_found"}
        owner_id = batch.owner_id

        # Safety kill-switch: do not send real SMS unless explicitly enabled.
        if not bool(getattr(settings, "SMS_SENDING_ENABLED", False)):
            # Leave messages as approved so enabling later can send without recreating batches.
            return {"ok": False, "error": "sms_sending_disabled", "batch_id": batch_id}

        if not twilio_is_configured():
            # Leave messages as approved so a later run (after env is configured) can send.
            return {"ok": False, "error": "twilio_not_configured", "batch_id": batch_id}

        # Load owner settings (optional; defaults are in DB via get_or_create in API)
        from app.db.models import OwnerSettings

        owner_settings = db.query(OwnerSettings).filter(OwnerSettings.owner_id == owner_id).first()
        global_limit = int(getattr(owner_settings, "sms_global_daily_limit", None) or settings.SMS_GLOBAL_DAILY_LIMIT or 200)
        start_hour = int(getattr(owner_settings, "sms_business_start_hour", None) or 7)
        end_hour = int(getattr(owner_settings, "sms_business_end_hour", None) or 19)

        # Job limit
        from app.db.models import JobSettings

        job_settings = (
            db.query(JobSettings)
            .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == batch.job_id)
            .first()
        )
        job_limit = int(getattr(job_settings, "sms_daily_limit", None) or settings.SMS_JOB_DAILY_LIMIT or 50)

        # Daily counts (UTC day, v1)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)

        global_sent_today = (
            db.query(func.count(SmsOutboundMessage.id))
            .filter(
                SmsOutboundMessage.owner_id == owner_id,
                SmsOutboundMessage.status == SmsMessageStatus.sent,
                SmsOutboundMessage.sent_at >= day_start,
                SmsOutboundMessage.sent_at < day_end,
            )
            .scalar()
            or 0
        )
        job_sent_today = (
            db.query(func.count(SmsOutboundMessage.id))
            .filter(
                SmsOutboundMessage.owner_id == owner_id,
                SmsOutboundMessage.job_id == batch.job_id,
                SmsOutboundMessage.status == SmsMessageStatus.sent,
                SmsOutboundMessage.sent_at >= day_start,
                SmsOutboundMessage.sent_at < day_end,
            )
            .scalar()
            or 0
        )

        remaining_global = max(global_limit - int(global_sent_today), 0)
        remaining_job = max(job_limit - int(job_sent_today), 0)

        # Pull approved messages (oldest first)
        msgs = (
            db.query(SmsOutboundMessage)
            .filter(
                SmsOutboundMessage.owner_id == owner_id,
                SmsOutboundMessage.batch_id == batch_id,
                SmsOutboundMessage.status == SmsMessageStatus.approved,
            )
            .order_by(SmsOutboundMessage.approved_at.asc().nullslast(), SmsOutboundMessage.created_at.asc())
            .limit(1000)
            .all()
        )

        attempted = 0
        for m in msgs:
            if attempted >= max_to_send:
                break

            if remaining_global <= 0 or remaining_job <= 0:
                skipped_limits += 1
                break

            # Re-check opt-out
            opt = (
                db.query(SmsOptOut.id)
                .filter(
                    SmsOptOut.owner_id == owner_id,
                    SmsOptOut.phone_e164 == m.to_phone_e164,
                    SmsOptOut.revoked_at.is_(None),
                )
                .first()
            )
            if opt:
                skipped_opt_out += 1
                continue

            # Re-check cooldown
            cutoff = now - timedelta(days=int(settings.SMS_COOLDOWN_DAYS or 14))
            recent = (
                db.query(func.count(SmsOutboundMessage.id))
                .filter(
                    SmsOutboundMessage.owner_id == owner_id,
                    SmsOutboundMessage.to_phone_e164 == m.to_phone_e164,
                    SmsOutboundMessage.status == SmsMessageStatus.sent,
                    SmsOutboundMessage.sent_at.isnot(None),
                    SmsOutboundMessage.sent_at >= cutoff,
                )
                .scalar()
                or 0
            )
            if recent > 0:
                skipped_cooldown += 1
                continue

            # Business hours by profile timezone (A)
            tzname = None
            if m.profile_id:
                prof = db.get(Profile, m.profile_id)
                tzname = (getattr(prof, "timezone", None) or "").strip() if prof else ""
            if not tzname:
                skipped_hours += 1
                continue

            try:
                local = now.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(tzname))
                hour = int(local.hour)
            except Exception:
                skipped_hours += 1
                continue

            if hour < start_hour or hour >= end_hour:
                skipped_hours += 1
                continue

            # Send
            try:
                attempted += 1
                m.status = SmsMessageStatus.sending
                db.add(m)
                db.commit()

                sid = send_sms(to_phone_e164=m.to_phone_e164, from_phone_e164=m.from_phone_e164, body=m.body)
                m.status = SmsMessageStatus.sent
                m.twilio_sid = sid
                m.sent_at = datetime.utcnow()
                m.error = None
                db.add(m)
                db.commit()
                sent += 1
                remaining_global -= 1
                remaining_job -= 1
            except TwilioError as e:
                failed += 1
                m.status = SmsMessageStatus.failed
                m.error = str(e)
                m.sent_at = datetime.utcnow()
                db.add(m)
                db.commit()
            except Exception as e:  # noqa: BLE001
                failed += 1
                m.status = SmsMessageStatus.failed
                m.error = f"send_failed:{e}"
                m.sent_at = datetime.utcnow()
                db.add(m)
                db.commit()

        # Mark batch completed when nothing remains approved
        remaining = (
            db.query(func.count(SmsOutboundMessage.id))
            .filter(
                SmsOutboundMessage.owner_id == owner_id,
                SmsOutboundMessage.batch_id == batch_id,
                SmsOutboundMessage.status.in_([SmsMessageStatus.approved, SmsMessageStatus.sending]),
            )
            .scalar()
            or 0
        )

        # If we still have approved messages but it's outside business hours,
        # re-schedule this task to run at the next earliest local start_hour.
        if remaining > 0 and skipped_hours > 0 and sent == 0 and failed == 0:
            from datetime import timezone as _timezone

            next_utc: datetime | None = None
            for m in msgs:
                tzname = ""
                if m.profile_id:
                    prof = db.get(Profile, m.profile_id)
                    tzname = (getattr(prof, "timezone", None) or "").strip() if prof else ""
                if not tzname:
                    continue
                try:
                    local_now = now.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(tzname))
                except Exception:
                    continue

                if local_now.hour < start_hour:
                    local_next = local_now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                else:
                    # after end_hour => tomorrow start_hour
                    local_next = (local_now + timedelta(days=1)).replace(
                        hour=start_hour, minute=0, second=0, microsecond=0
                    )

                utc_next = local_next.astimezone(_timezone.utc).replace(tzinfo=None)
                if next_utc is None or utc_next < next_utc:
                    next_utc = utc_next

            if next_utc is not None:
                delay_s = max(int((next_utc - now).total_seconds()), 60)
                try:
                    send_sms_batch.apply_async(args=[batch_id], kwargs={"max_to_send": max_to_send}, countdown=delay_s)
                except Exception:
                    pass

        if remaining == 0 and batch.status == SmsBatchStatus.approved:
            batch.status = SmsBatchStatus.completed
            batch.completed_at = datetime.utcnow()
            db.add(batch)
            db.commit()

    return {
        "ok": True,
        "batch_id": batch_id,
        "sent": sent,
        "failed": failed,
        "skipped_hours": skipped_hours,
        "skipped_opt_out": skipped_opt_out,
        "skipped_cooldown": skipped_cooldown,
        "skipped_limits": skipped_limits,
    }


def _strip_site_prefix(boolean_text: str) -> str:
    """Remove site:linkedin.com/in if user pasted it."""
    return re.sub(r"site:linkedin\.com/in/?", "", boolean_text, flags=re.IGNORECASE).strip()


# ------------------------------------------------------------------------------
# Enrichment helpers (Scrape.do + AdvancedBackgroundChecks)
# ------------------------------------------------------------------------------

_SCRAPEDO_ABC_PROVIDER_VERSION = 6
_SCRAPEDO_TPS_PROVIDER_VERSION = 3
_TPS_BASE_URL = "https://www.truepeoplesearch.com"
_SCRAPEDO_USPHONEBOOK_PROVIDER_VERSION = 1
_USPHONEBOOK_BASE_URL = "https://www.usphonebook.com"


_US_STATE_TO_ABBREV: dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

_US_ABBREV_TO_STATE: dict[str, str] = {v: k for k, v in _US_STATE_TO_ABBREV.items()}


def _slugify(s: str) -> str:
    """Lowercase, replace non-alnum with '-', collapse repeats."""
    s2 = (s or "").strip().lower()
    s2 = re.sub(r"[^a-z0-9]+", "-", s2)
    return s2.strip("-")


def _state_to_abbrev(state: str) -> Optional[str]:
    s = (state or "").strip()
    if not s:
        return None
    # Already an abbreviation?
    s2 = re.sub(r"[^A-Za-z]", "", s).upper()
    if len(s2) == 2:
        # Only accept real US state abbreviations (reject "RN", "IMC", etc.)
        return s2 if s2 in _US_ABBREV_TO_STATE else None
    # Full name
    # Normalize: keep letters/spaces, title-case
    name = re.sub(r"[^A-Za-z ]", " ", s).strip()
    name = re.sub(r"\s+", " ", name)
    if not name:
        return None
    return _US_STATE_TO_ABBREV.get(name.title())


def _state_to_usphonebook_state_slug(state: str) -> Optional[str]:
    """
    USPhonebook listing URLs use full state name (e.g. /texas/...), not abbreviation.
    """
    ab = _state_to_abbrev(state or "")
    if not ab:
        return None
    full = _US_ABBREV_TO_STATE.get(ab.upper())
    if not full:
        return None
    return _slugify(full)


def _candidate_abc_names_urls(prof: Profile) -> tuple[list[str], Optional[str], dict]:
    """
    Build AdvancedBackgroundChecks "names" URL candidates using profile name + city + state.
    Pattern (observed): /names/<first-last>_<city>-<state>
    Example: https://www.advancedbackgroundchecks.com/names/kamilah-jamison_joppa-md
    """
    tokens, name_err = _name_tokens_for_abc(getattr(prof, "full_name_raw", "") or "")
    if name_err:
        return [], name_err, {}
    # Location: prefer stored city/state, but fall back to re-parsing snippet if city looks like tenure.
    city_raw = (prof.city or "").strip()
    state_raw = (prof.state or "").strip()

    def _bad_city(s: str) -> bool:
        s2 = (s or "").strip()
        if not s2:
            return True
        lower = s2.lower()

        # tenure / duration text
        if "year" in lower or "month" in lower:
            return True

        # obvious non-city noise from snippets
        if "|" in s2 or "%" in s2 or "…" in s2 or "..." in s2:
            return True

        words = [w for w in re.split(r"\\s+", s2) if w]
        if len(words) > 4:
            return True
        if len(s2) > 40:
            return True

        # Multiple sentences in the "city" field is a strong signal it's not a city.
        if ". " in s2 and len(words) > 2:
            return True

        # Digits in city almost always means we're holding tenure/address text.
        if any(ch.isdigit() for ch in s2):
            return True

        return False

    def _bad_state(s: str) -> bool:
        # state should not be a long sentence; if it has lots of punctuation/digits, reject.
        if not s:
            return True
        if any(ch.isdigit() for ch in s):
            return True
        if len(s) > 40:
            return True
        return False

    used_fallback = False
    if _bad_city(city_raw) or _bad_state(state_raw):
        try:
            snippet = (prof.cse_item_json or {}).get("snippet") or ""
        except Exception:
            snippet = ""
        loc = extract_location_city_state(snippet, strategy_state=None)
        if loc:
            city_raw, state_raw, _country = loc
            used_fallback = True

    if not city_raw or not state_raw:
        return [], "missing_location", {}

    city_slug = _slugify(city_raw)
    if not city_slug:
        return [], "bad_city", {}

    st = _state_to_abbrev(state_raw)
    if not st:
        return [], "unsupported_state", {}

    # Build name slugs:
    # - full given names (e.g., "Mary Jane") + last
    # - first token + last
    first_full = " ".join(tokens[:-1]).strip()
    last = tokens[-1].strip()
    first_token = tokens[0].strip()

    name_slugs: list[str] = []
    full_slug = _slugify(f"{first_full} {last}")
    if full_slug:
        name_slugs.append(full_slug)
    if first_token and first_token != first_full:
        short_slug = _slugify(f"{first_token} {last}")
        if short_slug:
            name_slugs.append(short_slug)

    # de-dupe, keep order
    seen: set[str] = set()
    name_slugs2: list[str] = []
    for ns in name_slugs:
        if ns and ns not in seen:
            seen.add(ns)
            name_slugs2.append(ns)

    urls = [
        f"https://www.advancedbackgroundchecks.com/names/{ns}_{city_slug}-{st.lower()}"
        for ns in name_slugs2
    ]
    return (
        urls,
        None,
        {
            "target_first": first_token,
            "target_last": last,
            "target_city_slug": city_slug,
            "target_state_abbrev": st,
            "location_fallback_used": used_fallback,
            "city_raw": city_raw,
            "state_raw": state_raw,
        },
    )


def _candidate_usphonebook_listing_urls(prof: Profile) -> tuple[list[str], Optional[str], dict]:
    """
    Build USPhonebook listing URL candidates using profile name + city + state.
    Pattern (observed): /<first-last>/<state_name>/<city>
    Example: https://www.usphonebook.com/sujan-panday/texas/frisco
    """
    tokens, name_err = _name_tokens_for_abc(getattr(prof, "full_name_raw", "") or "")
    if name_err:
        return [], name_err, {}

    city_raw = (prof.city or "").strip()
    state_raw = (prof.state or "").strip()
    if not city_raw or not state_raw:
        return [], "missing_location", {}

    city_slug = _slugify(city_raw)
    if not city_slug:
        return [], "bad_city", {}

    state_slug = _state_to_usphonebook_state_slug(state_raw)
    st = _state_to_abbrev(state_raw)
    if not state_slug or not st:
        return [], "unsupported_state", {}

    first_full = " ".join(tokens[:-1]).strip()
    last = tokens[-1].strip()
    first_token = tokens[0].strip()

    name_slugs: list[str] = []
    full_slug = _slugify(f"{first_full} {last}")
    if full_slug:
        name_slugs.append(full_slug)
    if first_token and first_token != first_full:
        short_slug = _slugify(f"{first_token} {last}")
        if short_slug:
            name_slugs.append(short_slug)

    # de-dupe, keep order
    seen: set[str] = set()
    name_slugs2: list[str] = []
    for ns in name_slugs:
        if ns and ns not in seen:
            seen.add(ns)
            name_slugs2.append(ns)

    urls = [f"{_USPHONEBOOK_BASE_URL}/{ns}/{state_slug}/{city_slug}" for ns in name_slugs2]
    return (
        urls,
        None,
        {
            "target_first_token": first_token,
            "target_last": last,
            "target_city_slug": city_slug,
            "target_state_abbrev": st,
            "city_raw": city_raw,
            "state_raw": state_raw,
            "state_slug": state_slug,
        },
    )


_PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-.]*)?(?:\(?\d{3}\)?[\s\-.]*)\d{3}[\s\-.]*\d{4}(?!\d)"
)


def _extract_us_e164_phones(text: str) -> list[str]:
    """
    Extract US phone numbers and normalize to E.164 (+1XXXXXXXXXX).
    Intentionally conservative: only 10-digit NANP numbers.
    """
    if not text:
        return []

    matches: list[str] = []

    # Prefer tel: links when present
    for m in re.findall(r"tel:([^\"'<>\\s]+)", text, flags=re.IGNORECASE):
        matches.append(m)

    # Fallback: generic phone patterns
    matches.extend(_PHONE_RE.findall(text))

    out: set[str] = set()
    for raw in matches:
        digits = re.sub(r"\D", "", raw or "")
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            continue
        out.add("+1" + digits)

    return sorted(out)


def _digits_to_us_e164(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return "+1" + digits


_MONTH_TO_NUM = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _tps_last_reported_index(text: str) -> Optional[int]:
    """
    Parse "Last reported <Mon> <YYYY>" and return a comparable integer (YYYY*12 + MM).
    """
    if not text:
        return None
    m = re.search(r"Last reported\s+([A-Za-z]{3,9})\s+(\d{4})", text, flags=re.IGNORECASE)
    if not m:
        return None
    mon = (m.group(1) or "").strip().lower()
    yr = (m.group(2) or "").strip()
    mm = _MONTH_TO_NUM.get(mon)
    try:
        yy = int(yr)
    except Exception:
        return None
    if not mm:
        return None
    return (yy * 12) + mm


def _extract_tps_best_wireless_phone(detail_html: str) -> tuple[Optional[str], Optional[str]]:
    """
    Return (phone_e164, last_reported_text) for the most recently reported Wireless number on TPS.
    If none, returns (None, None).
    """
    if not detail_html:
        return None, None

    # Restrict to the phone section when possible (more robust than scanning whole page).
    section = detail_html
    idx = detail_html.find('id="toc-phones"')
    if idx == -1:
        idx = detail_html.find("id='toc-phones'")
    if idx != -1:
        tail = detail_html[idx:]
        # next toc marker ends this section
        next_idx = tail.find('id="toc-', 20)
        section = tail[:next_idx] if next_idx != -1 else tail

    # Each entry has /find/phone/<digits> and a type label (Wireless/Landline) and often "Last reported".
    entries: list[dict] = []
    for m in re.finditer(
        r'<a[^>]+href="/find/phone/(\d{7,15})"[^>]*>.*?</a>\s*-\s*<span[^>]*>\s*([^<]+?)\s*</span>',
        section,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        digits = (m.group(1) or "").strip()
        typ = (m.group(2) or "").strip().lower()
        if "wireless" not in typ:
            continue

        phone = _digits_to_us_e164(digits)
        if not phone:
            continue

        # Look ahead for "Last reported" in the same card.
        look = section[m.end() : m.end() + 1200]
        last_idx = _tps_last_reported_index(look)
        last_txt = None
        m_lr = re.search(
            r"Last reported\s+([A-Za-z]{3,9})\s+(\d{4})", look, flags=re.IGNORECASE
        )
        if m_lr:
            last_txt = f"{m_lr.group(1).strip()} {m_lr.group(2).strip()}"

        entries.append(
            {
                "phone": phone,
                "last_reported_index": last_idx or 0,
                "last_reported_text": last_txt,
            }
        )

    if not entries:
        return None, None

    # Highest last_reported wins; stable fallback to first occurrence in HTML.
    best = max(entries, key=lambda e: (e.get("last_reported_index") or 0))
    return best.get("phone"), best.get("last_reported_text")


def _extract_abc_first_wireless_phone(detail_html: str) -> Optional[str]:
    """
    Return the first Wireless phone number from an ABC detail page.
    Prefers embedded gResults JSON if present; falls back to HTML parsing.
    """
    if not detail_html:
        return None

    # 1) Try embedded gResults (more stable than HTML)
    m = re.search(r"gResults\s*:\s*'([^']+)'", detail_html)
    if m:
        try:
            raw = _html.unescape(m.group(1))
            inner = _json.loads(raw)
            obj = _json.loads(inner) if isinstance(inner, str) else inner
            phones = obj.get("phoneNumbers")
            if isinstance(phones, list):
                for p in phones:
                    if not isinstance(p, dict):
                        continue
                    typ = str(p.get("phoneType") or "").strip().lower()
                    if not typ.startswith("wire"):
                        continue
                    phone = _digits_to_us_e164(str(p.get("phoneNumber") or ""))
                    if phone:
                        return phone
        except Exception:
            pass

    # 2) Fallback: HTML " - Wireless" lines in the Phone Numbers box
    m2 = re.search(
        r'>\s*\(?([0-9]{3})\)?[\s\-.]*([0-9]{3})[\s\-.]*([0-9]{4})\s*</a>\s*-\s*Wireless\b',
        detail_html,
        flags=re.IGNORECASE,
    )
    if m2:
        return _digits_to_us_e164(m2.group(1) + m2.group(2) + m2.group(3))
    return None


def _extract_usphonebook_best_wireless_phone(detail_html: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Return (phone_e164, last_report_unix, carrier) for the most recently reported Wireless number on USPhonebook.
    Uses embedded gResults JSON (observed stable).
    """
    if not detail_html:
        return None, None, None

    m = re.search(r"gResults\s*:\s*'([^']+)'", detail_html)
    if not m:
        return None, None, None

    try:
        raw = _html.unescape(m.group(1))
        inner = _json.loads(raw)
        obj = _json.loads(inner) if isinstance(inner, str) else inner
    except Exception:
        return None, None, None

    if not isinstance(obj, dict):
        return None, None, None

    phones = obj.get("phoneNumbers")
    if not isinstance(phones, list):
        return None, None, None

    best = None
    for p in phones:
        if not isinstance(p, dict):
            continue
        typ = str(p.get("phoneType") or "").strip().lower()
        if "wire" not in typ:
            continue
        phone = _digits_to_us_e164(str(p.get("phoneNumber") or ""))
        if not phone:
            continue

        last_raw = p.get("lastReport")
        last_ts = 0
        try:
            last_ts = int(last_raw) if last_raw is not None else 0
        except Exception:
            last_ts = 0

        item = {"phone": phone, "last_report": last_ts, "carrier": (p.get("carrier") or None)}
        if best is None or (item["last_report"] or 0) > (best["last_report"] or 0):
            best = item

    if not best:
        return None, None, None

    return best["phone"], best.get("last_report"), best.get("carrier")


def _scrapedo_fetch_html(
    *,
    token: str,
    target_url: str,
    timeout_s: float = 60.0,
    super_mode: bool = True,
    render: bool = False,
) -> tuple[int, str, str]:
    """
    Fetch HTML via Scrape.do (sync version).
    Uses super=true (stronger anti-bot bypass) because AdvancedBackgroundChecks is WAF-protected.
    """
    params = {"token": token, "url": target_url}
    if super_mode:
        params["super"] = "true"
    if render:
        params["render"] = "true"
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get("https://api.scrape.do/", params=params)
        return r.status_code, r.text, (r.headers.get("content-type") or "")




def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


def _name_tokens_for_abc(full_name_raw: str) -> tuple[list[str], Optional[str]]:
    """
    Derive name tokens for ABC from a profile name.
    Rules:
    - If commas exist, keep only the part before the first comma (drop credentials like ", RN").
    - Drop known trailing credentials.
    - Return (tokens, error).
    """
    s = (full_name_raw or "").strip()
    if not s:
        return [], "missing_name"

    # Drop everything after first comma (credentials usually live there).
    if "," in s:
        s = s.split(",", 1)[0].strip()

    # Collapse whitespace and remove obvious age patterns if present.
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bAge\s+\d+\b", "", s, flags=re.IGNORECASE).strip()

    tokens = [t for t in s.split(" ") if t]
    if len(tokens) < 2:
        return [], "missing_first_last"

    # Strip trailing credentials (lowercased, punctuation-insensitive)
    # This is needed because CSE titles often include suffix creds (e.g., "BSN RN CCRN").
    extra_creds = {
        "rn",
        "bsn",
        "msn",
        "np",
        "aprn",
        "crna",
        "ccrn",
        "lpn",
        "lvn",
        "dnp",
        "cna",
        "pa",
        "md",
        "phd",
        "dds",
        "dmd",
        "do",
    }
    while tokens:
        t = re.sub(r"[^A-Za-z]", "", tokens[-1]).lower()
        if t in extra_creds:
            tokens = tokens[:-1]
            continue
        break

    if len(tokens) < 2:
        return [], "missing_first_last_after_credential_strip"

    return tokens, None


def _name_first_last_for_abc(full_name_raw: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    tokens, err = _name_tokens_for_abc(full_name_raw)
    if err:
        return None, None, err
    return tokens[0], tokens[-1], None


def _parse_abc_name_results(html: str) -> list[dict]:
    """
    Parse an AdvancedBackgroundChecks /names page into result cards.
    We only consider internal "View Details" links for the primary person card.
    """
    if not html:
        return []

    # Split into card chunks.
    starts = [m.start() for m in re.finditer(r'<div class="card loopindex_"', html)]
    if not starts:
        return []
    chunks: list[str] = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(html)
        chunks.append(html[st:end])

    out: list[dict] = []
    for idx, chunk in enumerate(chunks):
        # Name
        name_m = re.search(
            r'<h4 class="card-title"[^>]*>(.*?)</h4>', chunk, flags=re.IGNORECASE | re.DOTALL
        )
        if not name_m:
            continue
        name_text = _strip_tags(name_m.group(1))
        name_text = re.sub(r"\s+", " ", name_text).strip()
        name_text = re.sub(r"\bAge\s+\d+\b", "", name_text, flags=re.IGNORECASE).strip()
        if not name_text:
            continue

        # Location (first card-text paragraph)
        loc_m = re.search(
            r'<p class="card-text">\s*([^<]+?)\s*</p>', chunk, flags=re.IGNORECASE | re.DOTALL
        )
        city = None
        state = None
        if loc_m:
            loc_text = re.sub(r"\s+", " ", (loc_m.group(1) or "")).strip()
            # Expect "City, ST"
            m2 = re.search(r"^(.+?),\s*([A-Z]{2})\b", loc_text)
            if m2:
                city = m2.group(1).strip()
                state = m2.group(2).strip().upper()

        # Detail link ("View Details")
        detail_m = re.search(
            r'class="btn btn-primary link-to-details"[^>]*href="([^"]+)"',
            chunk,
            flags=re.IGNORECASE,
        )
        detail_href = detail_m.group(1).strip() if detail_m else None
        detail_url = None
        if detail_href and detail_href.startswith("/"):
            detail_url = "https://www.advancedbackgroundchecks.com" + detail_href

        out.append(
            {
                "index": idx,
                "name": name_text,
                "city": city,
                "state": state,
                "detail_url": detail_url,
            }
        )

    return out


def _parse_usphonebook_listing_results(html: str) -> list[dict]:
    """
    Parse a USPhonebook listing page (/first-last/state/city) into result cards.
    Extract: name, city/state, and a record detail URL.
    """
    if not html:
        return []

    starts = [m.start() for m in re.finditer(r'<div class="success-wrapper-block"', html)]
    if not starts:
        return []

    chunks: list[str] = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(html)
        chunks.append(html[st:end])

    out: list[dict] = []
    for idx, chunk in enumerate(chunks):
        # Name
        name_text = None
        m = re.search(r'itemprop="name"[^>]*>(.*?)</', chunk, flags=re.IGNORECASE | re.DOTALL)
        if m:
            name_text = re.sub(r"\s+", " ", _strip_tags(m.group(1) or "")).strip()
        if not name_text:
            continue

        # Location
        city = None
        state = None
        m = re.search(
            r"Lives in:\s*<span[^>]*>(.*?)</span>",
            chunk,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m:
            loc_text = re.sub(r"\s+", " ", _strip_tags(m.group(1) or "")).strip()
            m2 = re.search(r"^(.+?),\s*([A-Z]{2})\b", loc_text)
            if m2:
                city = m2.group(1).strip()
                state = m2.group(2).strip().upper()

        # Detail URL
        detail_url = None
        m = re.search(r'itemid="(https?://[^"]+)"', chunk, flags=re.IGNORECASE)
        if m:
            detail_url = (m.group(1) or "").strip()
        if not detail_url:
            m = re.search(
                r'href="([^"]+)"[^>]*>\s*<span[^>]*>\s*VIEW FULL ADDRESS\s*&\s*PHONE\s*</span>',
                chunk,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if m:
                href = (m.group(1) or "").strip()
                if href.startswith("/"):
                    detail_url = _USPHONEBOOK_BASE_URL + href
                elif href.startswith("http://") or href.startswith("https://"):
                    detail_url = href

        if not detail_url:
            continue

        out.append(
            {
                "index": idx,
                "name": name_text,
                "city": city,
                "state": state,
                "detail_url": detail_url,
            }
        )

    return out


def _parse_tps_results(html: str) -> list[dict]:
    """
    Parse a TruePeopleSearch /results page into candidate cards.

    We intentionally keep this regex-based (no BeautifulSoup dependency).
    The site structure varies; we prefer extracting:
    - name
    - city/state (if present)
    - a details URL (data-detail-link or /details href)
    """
    if not html:
        return []

    # Split into card-ish chunks when possible (card-summary), else fall back to detail-link anchors.
    starts = [
        m.start()
        for m in re.finditer(
            r'<div[^>]+class="[^"]*card-summary[^"]*"', html, flags=re.IGNORECASE
        )
    ]
    if not starts:
        starts = [m.start() for m in re.finditer(r'data-detail-link="', html, flags=re.IGNORECASE)]
    if not starts:
        return []

    chunks: list[str] = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(html)
        chunks.append(html[st:end])

    out: list[dict] = []
    for idx, chunk in enumerate(chunks):
        # Detail link
        detail_href = None
        m = re.search(r'data-detail-link="([^"]+)"', chunk, flags=re.IGNORECASE)
        if m:
            detail_href = (m.group(1) or "").strip()
        if not detail_href:
            m = re.search(r'href="([^"]*/details[^"]*)"', chunk, flags=re.IGNORECASE)
            if m:
                detail_href = (m.group(1) or "").strip()

        detail_url = None
        if detail_href:
            if detail_href.startswith("http://") or detail_href.startswith("https://"):
                detail_url = detail_href
            elif detail_href.startswith("/"):
                detail_url = _TPS_BASE_URL + detail_href
            else:
                detail_url = _TPS_BASE_URL + "/" + detail_href.lstrip("/")

        # Name (try a few common patterns)
        name_text = None
        for pat in (
            r'<div[^>]*class="[^"]*\bh4\b[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*content-header[^"]*"[^>]*>(.*?)</div>',
            r"<h4[^>]*>(.*?)</h4>",
        ):
            m = re.search(pat, chunk, flags=re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            name_text = _strip_tags(m.group(1))
            name_text = re.sub(r"\s+", " ", name_text or "").strip()
            break
        if name_text:
            # Drop common age prefix inside name blocks ("Age 40s")
            name_text = re.sub(r"\bAge\s+\d+s?\b", "", name_text, flags=re.IGNORECASE).strip()
            name_text = re.sub(r"\s+", " ", name_text).strip()

        # Location (best-effort)
        city = None
        state = None

        # Prefer "content-value" blocks; then fallback to any "City, ST" match.
        for m in re.finditer(
            r'class="content-value"[^>]*>(.*?)</', chunk, flags=re.IGNORECASE | re.DOTALL
        ):
            t = re.sub(r"\s+", " ", _strip_tags(m.group(1))).strip()
            m2 = re.search(r"^(.+?),\s*([A-Z]{2})\b", t)
            if m2:
                city = m2.group(1).strip()
                state = m2.group(2).strip().upper()
                break

        if not city:
            m2 = re.search(r"\b([A-Za-z][A-Za-z .'\-]{1,60}),\s*([A-Z]{2})\b", chunk)
            if m2:
                city = m2.group(1).strip()
                state = m2.group(2).strip().upper()

        out.append(
            {
                "index": idx,
                "name": name_text,
                "city": city,
                "state": state,
                "detail_url": detail_url,
            }
        )

    # Only return candidates we can attempt to open.
    return [r for r in out if r.get("detail_url")]


async def _fetch_cse_page(client: httpx.AsyncClient, query: str, start: int) -> dict:
    """
    Fetch one Google SERP page via Scrape.do.

    NOTE: This replaces Google CSE entirely (CSE had a hard 100-result cap and paid pagination).
    We keep the return shape compatible with downstream code: {"items": [...], "queries": {...}}.
    """
    if not settings.SCRAPEDO_API_KEY:
        raise RuntimeError("SCRAPEDO_API_KEY is not set")

    # Google SERP pagination uses `start` (0-based), stepping by 10.
    start0 = max(int(start or 0), 0)
    target_url = "https://www.google.com/search?" + urllib.parse.urlencode(
        {"q": query, "start": str(start0), "hl": "en", "gl": "US"}
    )
    base_params: dict[str, str] = {"token": settings.SCRAPEDO_API_KEY, "url": target_url}
    if getattr(settings, "SCRAPEDO_SERP_SUPER", False):
        base_params["super"] = "true"

    def _apply_render_params(p: dict[str, str]) -> dict[str, str]:
        p2 = dict(p)
        p2["render"] = "true"
        # Only render-mode params (Scrape.do can reject these when render=false).
        p2["blockResources"] = (
            "true" if getattr(settings, "SCRAPEDO_SERP_BLOCK_RESOURCES", False) else "false"
        )
        sel = str(getattr(settings, "SCRAPEDO_SERP_WAIT_SELECTOR", "") or "").strip()
        if sel:
            p2["waitSelector"] = sel
        try:
            wait_ms = int(getattr(settings, "SCRAPEDO_SERP_CUSTOM_WAIT_MS", 0) or 0)
        except Exception:
            wait_ms = 0
        if wait_ms > 0:
            p2["customWait"] = str(wait_ms)
        return p2

    params = _apply_render_params(base_params) if getattr(settings, "SCRAPEDO_SERP_RENDER_ALWAYS", False) else base_params

    retries = max(settings.CSE_PAGE_RETRIES, 0)
    for attempt in range(retries + 1):
        try:
            r = await client.get("https://api.scrape.do/", params=params)
            status_code = int(getattr(r, "status_code", 0) or 0)

            # Scrape.do auth/rate errors should be explicit.
            if status_code in (401, 403):
                raise RuntimeError(f"scrapedo_auth_failed_http_{status_code}: check SCRAPEDO_API_KEY")
            if status_code == 429:
                raise RuntimeError("scrapedo_rate_limited_http_429")
            if status_code >= 400:
                body = (r.text or "").strip()
                raise RuntimeError(f"scrapedo_http_{status_code}: {body[:300]}")

            html = r.text or ""
            items, meta = _parse_google_serp_html(html)

            # If blocked/consent, retry with headless render once (if enabled).
            if meta.get("blocked") and getattr(settings, "SCRAPEDO_SERP_RENDER_ON_BLOCK", False):
                params2 = _apply_render_params(base_params)
                r2 = await client.get("https://api.scrape.do/", params=params2)
                html2 = r2.text or ""
                items2, meta2 = _parse_google_serp_html(html2)
                if items2:
                    items, meta = items2, meta2
                    params = params2
            page: dict = {
                "items": items,
                "queries": {"request": [{"startIndex": start0}]},
                "_serp_meta": meta,
            }
            return page
        except Exception:
            if attempt >= retries:
                raise
            await asyncio.sleep((0.8 * (2**attempt)) + random.random() * 0.2)
    raise RuntimeError("unreachable")


def _parse_google_serp_html(html: str) -> tuple[list[dict], dict]:
    """
    Best-effort parse Google SERP HTML (as fetched via Scrape.do) into CSE-like items:
      [{"link": "...", "title": "...", "snippet": "..."}]
    """
    text = html or ""
    low = text.lower()

    # NOTE: Google sometimes serves consent overlays but still includes results in HTML.
    # We detect "block signals" but only flag blocked=true if parsing yields 0 items.
    has_block_signals = any(
        s in low
        for s in (
            "unusual traffic",
            "our systems have detected unusual traffic",
            "/sorry/",
            "captcha",
        )
    )
    # Consent overlays often still have results behind them - don't treat as blocked.
    has_consent_signals = any(
        s in low
        for s in (
            "before you continue to google",
            "consent.google.com",
            "to continue, please",
        )
    )

    meta: dict = {
        "blocked": False,  # Will set to True only if 0 items AND block signals
        "did_not_match": "did not match any documents" in low,
    }

    def _strip_tags(s: str) -> str:
        return re.sub(r"<[^>]+>", "", s or "")

    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").replace("\n", " ")).strip()

    def _normalize_href(href: str) -> str:
        href2 = _html.unescape(href or "")
        href2 = href2.replace("\\u003d", "=").replace("\\u0026", "&")
        if href2.startswith("/url?"):
            href2 = "https://www.google.com" + href2
        if href2.startswith("https://www.google.com/url?"):
            try:
                q = urllib.parse.parse_qs(urllib.parse.urlparse(href2).query).get("q", [""])[0]
                if q:
                    href2 = q
            except Exception:
                pass
        return href2.strip()

    items: list[dict] = []
    seen_links: set[str] = set()

    # Common container in current Google HTML: <div class="MjjYud"> ... </div>
    parts = re.split(r'<div class="MjjYud"', text)
    for part in parts[1:]:
        chunk = part[:9000]

        # Prefer the actual organic result link (anchor that contains an <h3>).
        # The first href inside the block is often NOT the result (e.g. tracking, sitelinks).
        m_res = re.search(
            r'<a[^>]+href="([^"]+)"[^>]*>\s*(?:<div[^>]*>\s*)?<h3[^>]*>(.*?)</h3>',
            chunk,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m_res:
            link = _normalize_href(m_res.group(1))
            title = _clean(_strip_tags(m_res.group(2)))
        else:
            # Fallback: best-effort first href + first h3.
            m_href = re.search(r'href="([^"]+)"', chunk, flags=re.IGNORECASE)
            if not m_href:
                continue
            link = _normalize_href(m_href.group(1))
            if not link:
                continue
            m_title = re.search(r"<h3[^>]*>(.*?)</h3>", chunk, flags=re.IGNORECASE | re.DOTALL)
            title = _clean(_strip_tags(m_title.group(1))) if m_title else ""

        if not link:
            continue

        # Snippet often sits in <div class="VwiC3b ..."> ... </div>
        m_snip = re.search(
            r'<div class="VwiC3b[^"]*"[^>]*>(.*?)</div>', chunk, flags=re.IGNORECASE | re.DOTALL
        )
        if not m_snip:
            m_snip = re.search(
                r'<div class="VwiC3b[^>]*>(.*?)</div>', chunk, flags=re.IGNORECASE | re.DOTALL
            )
        snippet = _clean(_strip_tags(m_snip.group(1))) if m_snip else ""

        # Only keep LinkedIn profile links; downstream expects /in/ anyway.
        if not is_linkedin_in_url(link):
            continue

        # De-dupe within a page; Google sometimes repeats results.
        if link in seen_links:
            continue
        seen_links.add(link)
        items.append({"link": link, "title": title, "snippet": snippet})
        if len(items) >= 10:
            break

    # Fallback: if the structured parse yields <10, scan for any linkedin.com/in/ links in the HTML.
    # This protects us against Google markup variance on deeper pages.
    if len(items) < 10:
        try:
            # 1) Direct occurrences
            direct = set(
                m.group(0).strip()
                for m in re.finditer(
                    r"https?://(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_%]+/?",
                    text,
                    flags=re.IGNORECASE,
                )
            )

            # 2) Google redirect URLs: /url?q=<TARGET>
            redirected: set[str] = set()
            for m in re.finditer(r"/url\\?q=([^&\"']+)", text, flags=re.IGNORECASE):
                try:
                    target = urllib.parse.unquote(m.group(1))
                except Exception:
                    continue
                if is_linkedin_in_url(target):
                    redirected.add(target.strip())

            for link in sorted(direct | redirected):
                if len(items) >= 10:
                    break
                # Normalize and apply /in/ filter
                try:
                    link2 = normalize_linkedin_url(link)
                except Exception:
                    link2 = link
                if not is_linkedin_in_url(link2):
                    continue
                if link2 in seen_links:
                    continue
                seen_links.add(link2)
                items.append({"link": link2, "title": "", "snippet": ""})
        except Exception:
            pass

    # Only flag blocked if we got 0 items AND we saw hard block signals (not just consent).
    if not items and has_block_signals:
        meta["blocked"] = True

    return items, meta


def _serp_planned_starts(max_results: int, per_page: int) -> list[int]:
    """
    Plan Google `start` indices (0-based).
    Unlike CSE, we do NOT hard-cap at 100; cap is controlled by env (CSE_MAX_RESULTS).
    """
    per_page = min(max(int(per_page or 10), 1), 10)
    max_results = max(int(max_results or 0), 0)
    if max_results <= 0:
        return []
    pages_needed = (max_results + per_page - 1) // per_page
    starts = [i * per_page for i in range(pages_needed)]
    return starts


async def _run_cse(query: str) -> tuple[list[dict], list[str], int]:
    """Run Google SERP via Scrape.do with best-effort pagination and safe early-stop."""
    if not settings.SCRAPEDO_API_KEY:
        return [], ["SCRAPEDO_API_KEY not set"], 0

    # Google SERP is effectively 10 results per page.
    per_page = 10
    max_results = max(int(settings.CSE_MAX_RESULTS or 0), 0)
    planned_starts = _serp_planned_starts(max_results=max_results, per_page=per_page)
    if not planned_starts:
        return [], [], 0

    # Concurrency increases captcha risk; keep it conservative.
    sem = asyncio.Semaphore(max(min(settings.CSE_PAGE_CONCURRENCY, 2), 1))
    errors: list[str] = []
    pages: list[dict] = []

    async with httpx.AsyncClient(timeout=45) as client:
        async def _one(start0: int) -> dict:
            async with sem:
                return await _fetch_cse_page(client, query=query, start=start0)

        # Fetch sequentially in order so we can early-stop on no-results/captcha.
        # (We keep the semaphore wrapper to allow small concurrency if configured.)
        consecutive_empty = 0
        for start0 in planned_starts:
            try:
                page = await _one(start0)
                pages.append(page)
                meta = (page.get("_serp_meta") or {}) if isinstance(page, dict) else {}
                if meta.get("blocked"):
                    errors.append("google_serp_blocked_captcha")
                    break
                if meta.get("did_not_match") and not (page.get("items") or []):
                    break
                if not (page.get("items") or []):
                    # No results parsed. This can be a transient markup variance on deeper pages.
                    # Allow one empty page, then stop to avoid burning credits indefinitely.
                    consecutive_empty += 1
                    errors.append(f"google_serp_empty_page_start_{start0}")
                    if consecutive_empty >= 2:
                        break
                    continue
                consecutive_empty = 0
            except Exception as e:
                errors.append(str(e))
                break

    # Stable order: process pages from low->high startIndex when possible.
    def _page_start(p: dict) -> int:
        try:
            req = ((p.get("queries") or {}).get("request") or [{}])[0]
            return int(req.get("startIndex") or 0)
        except Exception:
            return 0

    pages.sort(key=_page_start)
    return pages, errors, len(planned_starts)


def _bad_profile_name(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return True
    lower = s.lower()
    if "linkedin" in lower or "http" in lower or "www." in lower:
        return True
    if any(ch.isdigit() for ch in s):
        return True
    # Names with trailing credentials often show up as comma-separated tokens:
    # "Hannah Bass, RN, MSN" → treat as bad so LLM can clean it.
    if "," in s and re.search(
        r",\s*(?:r\.?\s*n\.?|rn|b\.?\s*s\.?\s*n\.?|bsn|m\.?\s*s\.?\s*n\.?|msn|dnp|md|phd|mba|lpn|cna|np|aprn|pa|ccrn|cmsrn|ocn|cen|cnor|bc)",
        s,
        flags=re.IGNORECASE,
    ):
        return True
    # These separators usually indicate we captured non-name text.
    if " - " in s or " – " in s or " | " in s:
        return True
    if len([w for w in s.split() if w]) < 2:
        return True
    if len(s) > 80:
        return True
    return False


def _bad_profile_city(city: str, full_name: str = "") -> bool:
    s = (city or "").strip()
    if not s:
        return True
    lower = s.lower()
    if any(ch.isdigit() for ch in s):
        return True
    if "year" in lower or "month" in lower or "present" in lower:
        return True
    if "|" in s or "%" in s or "…" in s or "..." in s:
        return True
    # Multiple sentences in "city" is a strong signal it's not a city.
    if ". " in s:
        return True
    words = [w for w in re.split(r"\s+", s) if w]
    if len(words) > 5:
        return True
    if len(s) > 60:
        return True

    # Blacklist common non-city text that LLM sometimes returns as city
    non_city_phrases = [
        "registered", "nurse", "nursing", "medical", "surgical", "telemetry",
        "manager", "director", "coordinator", "supervisor", "specialist",
        "hospital", "clinic", "healthcare", "health care", "center",
        "bsn", "msn", "rn", "lpn", "cna", "md", "phd", "preceptor",
        "university", "college", "education", "experience", "linkedin",
        "labor", "delivery", "intensive", "critical", "emergency",
        "greater", "metro", "area", "region",
        "icu", "nicu", "picu", "ccu", "pacu", "or ", "er ", "ed ",
    ]
    for phrase in non_city_phrases:
        if phrase in lower:
            return True

    # If city looks like a person's name (matches the profile name), it's bad
    if full_name:
        # Extract alpha tokens, drop common credentials, then compare against first/last.
        cred = {
            "rn",
            "bsn",
            "msn",
            "md",
            "phd",
            "mba",
            "lpn",
            "cna",
            "dpt",
            "crna",
            "np",
            "aprn",
            "pa",
            "ccrn",
            "cmsrn",
            "ocn",
            "cen",
            "bc",
            "cnor",
            "lnc",
            "dnp",
        }
        tokens = [t for t in re.findall(r"[a-z]+", full_name.lower()) if t and t not in cred]
        if len(tokens) >= 2:
            first = tokens[0]
            last = tokens[-1]
            fl = f"{first} {last}"
            if lower == fl or lower == first or lower == last:
                return True

    return False


def _bad_profile_state(state: str) -> bool:
    s = (state or "").strip()
    if not s:
        return True
    if any(ch.isdigit() for ch in s):
        return True
    if "|" in s or "%" in s or "…" in s or "..." in s:
        return True
    if ". " in s:
        return True
    if len(s) > 60:
        return True

    # Must be a valid US state (full name or 2-letter abbreviation)
    if _state_to_abbrev(s) is None:
        return True

    return False


def _expand_us_state_abbrev(state: str) -> str:
    """
    Normalize US states to full names when we only have abbreviations (PA -> Pennsylvania).
    This improves matching against location booleans which typically use full names.
    """
    s = (state or "").strip()
    ab = re.sub(r"[^A-Za-z]", "", s).upper()
    if len(ab) == 2:
        full = _US_ABBREV_TO_STATE.get(ab)
        if full:
            return full
    return s


def _get_or_create_profile(
    db: Session,
    *,
    owner_id: str,
    linkedin_url_raw: str,
    linkedin_url_canonical: str,
    full_name: str,
    city: str,
    state: str,
    country: Optional[str],
    title: Optional[str] = None,
    cse_item_json: dict,
    allow_overwrite_if_no_phone: bool = False,
) -> Profile:
    """Get existing profile or create new one."""
    existing = db.scalar(
        select(Profile).where(
            (Profile.owner_id == owner_id) & (Profile.linkedin_url_canonical == linkedin_url_canonical)
        )
    )
    if existing:
        # If this is a rerun and the profile still has no phone, overwrite extraction fields.
        if allow_overwrite_if_no_phone and not _profile_has_any_phone(
            db, owner_id=owner_id, profile_id=existing.id
        ):
            existing.full_name_raw = full_name
            existing.first_name, existing.last_name = split_first_last(full_name)
            existing.city = city
            existing.state = state
            existing.country = country
            if title:
                existing.title = title
            existing.cse_item_json = cse_item_json
            db.add(existing)
            return existing

        changed = False

        # Only "repair" when stored fields look bad, to avoid stomping user uploads.
        if full_name and _bad_profile_name(existing.full_name_raw) and not _bad_profile_name(full_name):
            if existing.full_name_raw != full_name:
                existing.full_name_raw = full_name
                existing.first_name, existing.last_name = split_first_last(full_name)
                changed = True

        if city and _bad_profile_city(existing.city or "", existing.full_name_raw) and not _bad_profile_city(
            city, full_name
        ):
            if (existing.city or "") != city:
                existing.city = city
                changed = True

        if state and _bad_profile_state(existing.state or "") and not _bad_profile_state(state):
            if (existing.state or "") != state:
                existing.state = state
                changed = True

        if country and (not existing.country or _bad_profile_state(existing.country or "")):
            if (existing.country or "") != country:
                existing.country = country
                changed = True

        # Title: keep the AI-extracted title if present. Prefer not to overwrite non-empty with empty.
        if title and (existing.title or "") != title:
            existing.title = title
            changed = True

        # Prefer preserving upload-provided CSE data; otherwise keep the latest snippet/title.
        try:
            src = str((existing.cse_item_json or {}).get("source") or "").strip().lower()
        except Exception:
            src = ""
        if src != "upload":
            if existing.cse_item_json != cse_item_json:
                existing.cse_item_json = cse_item_json
                changed = True

        if changed:
            db.add(existing)
            db.flush()
        return existing

    first_name, last_name = split_first_last(full_name)
    prof = Profile(
        owner_id=owner_id,
        linkedin_url_raw=linkedin_url_raw,
        linkedin_url_canonical=linkedin_url_canonical,
        full_name_raw=full_name,
        first_name=first_name,
        last_name=last_name,
        city=city,
        state=state,
        country=country,
        title=title,
        cse_item_json=cse_item_json,
    )
    db.add(prof)
    db.flush()
    return prof


def _ensure_job_profile_link(db: Session, *, owner_id: str, job_id: str, profile_id: str) -> bool:
    """Link profile to job if not already linked. Returns True if new link created."""
    existing = db.scalar(
        select(JobProfile).where(
            (JobProfile.owner_id == owner_id)
            & (JobProfile.job_id == job_id)
            & (JobProfile.profile_id == profile_id)
        )
    )
    if existing:
        return False
    db.add(JobProfile(owner_id=owner_id, job_id=job_id, profile_id=profile_id))
    return True


def _drop(
    db: Session,
    *,
    owner_id: str,
    job_id: str,
    strategy_run_id: str,
    reason: str,
    linkedin_url_raw: Optional[str],
    cse_item_json: dict,
) -> None:
    """Record a dropped profile."""
    db.add(
        DroppedProfile(
            owner_id=owner_id,
            job_id=job_id,
            strategy_run_id=strategy_run_id,
            reason=reason,
            linkedin_url_raw=linkedin_url_raw,
            cse_item_json=cse_item_json,
        )
    )


def _queue_next_run(db: Session, owner_id: str, job_id: str) -> None:
    """Queue the next pending strategy run for this job (if any)."""
    try:
        js = (
            db.query(JobSettings.paused)
            .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id)
            .first()
        )
        if js and bool(js[0]):
            return
    except Exception:
        # If settings are unavailable, default to not-paused.
        pass

    next_run = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job_id,
            StrategyRun.status == StrategyRunStatus.queued,
        )
        .order_by(StrategyRun.created_at)
        .first()
    )
    if next_run:
        try:
            run_strategy_run.delay(next_run.id)
        except Exception:
            pass  # Will be picked up by retry logic


def _profile_text_for_match(cse_item_json: dict) -> str:
    title = (cse_item_json or {}).get("title") or ""
    snippet = (cse_item_json or {}).get("snippet") or ""
    return f"{title} {snippet}".strip()


def _profile_title_for_match(profile: Profile) -> str:
    """
    Title-only matching haystack.
    Prefer AI-extracted Profile.title. Fall back to CSE title+snippet if missing.
    """
    t = (profile.title or "").strip()
    if t:
        return t
    return _profile_text_for_match(profile.cse_item_json or {})


def _location_text_for_match(profile: Profile) -> str:
    # Include both structured city/state and raw snippet so location booleans can match either.
    cse = profile.cse_item_json or {}
    snippet = (cse.get("snippet") or "").strip()
    st_abbrev = _state_to_abbrev(profile.state) or ""
    parts = [profile.city or "", profile.state or "", st_abbrev, snippet]
    return " ".join([p for p in parts if p]).strip()


def _job_requirement_bools(db: Session, *, owner_id: str, job_id: str) -> tuple[list[str], list[str]]:
    titles = (
        db.query(TitleVariant)
        .filter(
            TitleVariant.owner_id == owner_id,
            TitleVariant.job_id == job_id,
            TitleVariant.selected == True,
        )
        .all()
    )
    locations = (
        db.query(LocationVariant)
        .filter(
            LocationVariant.owner_id == owner_id,
            LocationVariant.job_id == job_id,
            LocationVariant.selected == True,
        )
        .all()
    )
    def _is_parseable(b: str) -> bool:
        try:
            parse_boolean(b)
            return True
        except Exception:
            return False

    title_bools: list[str] = []
    for t in titles:
        b = (t.boolean_text or "").strip()
        if not b:
            continue
        if _is_parseable(b):
            title_bools.append(b)

    location_bools: list[str] = []
    for l in locations:
        b = (l.boolean_text or "").strip()
        if not b:
            continue
        if _is_parseable(b):
            location_bools.append(b)

    return title_bools, location_bools


def _chunks(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _get_job_auto_enrich_provider(db: Session, *, owner_id: str, job_id: str) -> Optional[str]:
    row = (
        db.query(JobSettings)
        .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id)
        .first()
    )
    if not row or not bool(getattr(row, "auto_enrich_enabled", False)):
        return None
    provider = (getattr(row, "auto_enrich_provider", "") or "").strip().lower()
    if not provider or provider == "disabled":
        return None
    return provider


def maybe_queue_job_auto_enrich(
    db: Session,
    *,
    owner_id: str,
    job_id: str,
    profile_ids: list[str],
) -> dict:
    """
    If job auto-enrichment is enabled, queue enrichment for the given profile_ids.

    Rules (per product decisions):
    - No new UI toggle: job becomes auto-enrich enabled after user clicks "Find phones"
      (i.e., /api/enrichment/enrich-job sets JobSettings.auto_enrich_enabled/provider).
    - Skip if the profile already has phones from ANY provider.
    - Otherwise, queue enrichment for the job's selected provider.
    """
    provider = _get_job_auto_enrich_provider(db, owner_id=owner_id, job_id=job_id)
    if not provider:
        return {"queued": 0, "skipped_has_phones": 0, "skipped_in_flight": 0, "provider": "disabled"}

    if provider == "chain":
        stats = queue_chain_for_profiles(db, owner_id=owner_id, profile_ids=profile_ids)
        if stats.get("queued"):
            try:
                run_enrichment_dispatcher.delay()
            except Exception:
                pass
        return {**stats, "provider": "chain"}

    # De-dupe while preserving order (Python 3.7+ dict preserves insertion order)
    deduped = list(dict.fromkeys([p for p in (profile_ids or []) if p]))
    if not deduped:
        return {"queued": 0, "skipped_has_phones": 0, "skipped_in_flight": 0, "provider": provider}

    queued = 0
    skipped_has_phones = 0
    skipped_in_flight = 0

    # Avoid huge IN() clauses
    for chunk in _chunks(deduped, 500):
        # Profiles that already have phones from any provider
        phones_rows = (
            db.query(ProfileEnrichment.profile_id)
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.profile_id.in_(chunk),
                ProfileEnrichment.status == EnrichmentStatus.completed,
                func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
            )
            .distinct()
            .all()
        )
        has_phones = {pid for (pid,) in phones_rows}

        # Existing records for this provider
        existing = (
            db.query(ProfileEnrichment)
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.profile_id.in_(chunk),
                ProfileEnrichment.provider == provider,
            )
            .all()
        )
        by_pid = {rec.profile_id: rec for rec in existing}

        for pid in chunk:
            if pid in has_phones:
                skipped_has_phones += 1
                continue

            rec = by_pid.get(pid)
            if rec:
                if rec.status in (EnrichmentStatus.queued, EnrichmentStatus.running):
                    skipped_in_flight += 1
                    continue

                # If this provider already found phones, treat as done.
                if rec.status == EnrichmentStatus.completed and (rec.phone_numbers or []):
                    skipped_has_phones += 1
                    continue

                # Re-queue (failed or completed-without-phones)
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.finished_at = None
                rec.last_error = None
                rec.raw_json = None
                rec.phone_numbers = None
                db.add(rec)
                queued += 1
            else:
                db.add(
                    ProfileEnrichment(
                        owner_id=owner_id,
                        profile_id=pid,
                        provider=provider,
                        status=EnrichmentStatus.queued,
                    )
                )
                queued += 1

    if queued:
        db.commit()
        try:
            run_enrichment_dispatcher.delay()
        except Exception:
            pass

    return {
        "queued": queued,
        "skipped_has_phones": skipped_has_phones,
        "skipped_in_flight": skipped_in_flight,
        "provider": provider,
    }


def _profile_matches_job_requirement(
    profile: Profile,
    *,
    title_bools: list[str],
    location_bools: list[str],
) -> bool:
    # If there are no selected booleans, treat as match (defensive).
    text = _profile_title_for_match(profile)

    def _any_bool_matches(bools: list[str], haystack: str) -> bool:
        if not bools:
            return True
        parsed_any = False
        for b in bools:
            try:
                parsed_any = True
                if boolean_matches_text(b, haystack):
                    return True
            except Exception:
                continue
        # If none of the booleans were parseable, do not filter.
        if not parsed_any:
            return True
        return False

    title_ok = _any_bool_matches(title_bools, text)
    # User requirement: location should not exclude results.
    return title_ok


def _rebuild_job_profile_links(
    db: Session,
    *,
    owner_id: str,
    job_id: str,
    title_bools: list[str],
    location_bools: list[str],
) -> dict:
    """
    Ensure job_profiles only include profiles that match the job requirement.
    Also adds any existing profiles in the global pool that match the requirement.
    """
    # Safety: if the job has no selected booleans at all (e.g., copilot timed out and created
    # no variants, or user deselected everything), DO NOT treat that as "match everything".
    # In that state, we skip the global-pool sync to avoid accidentally linking the entire DB.
    if not title_bools and not location_bools:
        pinned_ids = {
            r[0]
            for r in db.query(JobProfilePin.profile_id)
            .filter(JobProfilePin.owner_id == owner_id, JobProfilePin.job_id == job_id)
            .all()
        }

        # Safety: avoid "match everything" when Copilot failed to create variants.
        #
        # Also: we have seen historical jobs get polluted by linking the ENTIRE global pool in this state.
        # If a job with no selected booleans is linked to (essentially) all profiles, purge those links.
        existing_count = (
            db.query(func.count(JobProfile.id))
            .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id)
            .scalar()
            or 0
        )
        if existing_count:
            total_profiles = (
                db.query(func.count(Profile.id)).filter(Profile.owner_id == owner_id).scalar() or 0
            )
            if total_profiles > 0 and existing_count >= total_profiles:
                # Purge only non-pinned links. (Pinned uploads should never be deleted here.)
                q = db.query(JobProfile).filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id)
                if pinned_ids:
                    q = q.filter(~JobProfile.profile_id.in_(list(pinned_ids)))
                removed = q.delete(synchronize_session=False) or 0
                if removed:
                    db.commit()
                return {"added": 0, "removed": int(removed), "matched_total": 0}

        return {"added": 0, "removed": 0, "matched_total": 0}

    # Compute matching profile ids (global pool)
    profiles = db.query(Profile).filter(Profile.owner_id == owner_id).all()
    match_ids: set[str] = set()
    for p in profiles:
        try:
            if _profile_matches_job_requirement(p, title_bools=title_bools, location_bools=location_bools):
                match_ids.add(p.id)
        except Exception:
            # If boolean parsing fails for a given profile, treat as non-match
            continue

    existing_links = (
        db.query(JobProfile)
        .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id)
        .all()
    )
    existing_ids = {jp.profile_id for jp in existing_links}
    pinned_ids = {
        r[0]
        for r in db.query(JobProfilePin.profile_id)
        .filter(JobProfilePin.owner_id == owner_id, JobProfilePin.job_id == job_id)
        .all()
    }

    removed = 0
    for jp in existing_links:
        if jp.profile_id in pinned_ids:
            continue
        if jp.profile_id not in match_ids:
            db.delete(jp)
            removed += 1

    to_add = list(match_ids - existing_ids)
    added = 0
    for pid in to_add:
        db.add(JobProfile(owner_id=owner_id, job_id=job_id, profile_id=pid))
        added += 1

    if removed or added:
        db.commit()

    # If the job is configured for auto-enrichment, queue enrichment for newly linked profiles.
    # (This covers "rebuild pulled profiles from global pool".)
    if added:
        try:
            maybe_queue_job_auto_enrich(db, owner_id=owner_id, job_id=job_id, profile_ids=to_add)
        except Exception:
            pass

    return {"added": added, "removed": removed, "matched_total": len(match_ids)}


@celery_app.task(name="run_strategy_run")
def run_strategy_run(strategy_run_id: str) -> None:
    """Execute a strategy run: call CSE, parse results, store profiles."""

    db = SessionLocal()
    try:
        run = db.get(StrategyRun, strategy_run_id)
        if not run:
            return
        owner_id = run.owner_id
        if run.status not in (StrategyRunStatus.queued,):
            return

        # Respect per-job pause (do not start new work while paused).
        try:
            js = (
                db.query(JobSettings.paused)
                .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == run.job_id)
                .first()
            )
            if js and bool(js[0]):
                return
        except Exception:
            pass

        # Reruns are encoded in the combo_signature to avoid schema changes.
        is_rerun = "::rerun::" in (run.combo_signature or "")

        # Mark as running
        run.status = StrategyRunStatus.running
        run.started_at = _utcnow()
        db.add(run)
        db.commit()
        db.refresh(run)

        # Get the location variant to use its state for validation
        location_variant = db.get(LocationVariant, run.location_variant_id)
        strategy_state: Optional[str] = None
        if location_variant and location_variant.entities:
            # Last entity is typically the state (either "FL" or "Florida")
            for e in reversed(location_variant.entities):
                e2 = (e or "").strip()
                if not e2:
                    continue
                # Accept 2-letter abbreviations or full state names
                if len(e2) == 2 or len(e2) > 2:
                    strategy_state = e2
                    break

        # Build query
        boolean_no_site = _strip_site_prefix(run.boolean_text)
        query = f"site:linkedin.com/in {boolean_no_site}".strip()

        # Run CSE
        pages, errors, pages_total = asyncio.run(_run_cse(query))
        run.pages_total = pages_total
        db.add(run)
        db.commit()

        added = 0
        dropped = 0
        new_linked_profile_ids: list[str] = []

        # Job-level requirement booleans (selected variants) — used to decide whether
        # a profile actually belongs to the job.
        title_bools, location_bools = _job_requirement_bools(db, owner_id=owner_id, job_id=run.job_id)

        for page in pages:
            items = page.get("items") or []

            # GPT PRIMARY:
            # Send all LinkedIn /in/ results to the LLM and ONLY store profiles if
            # we get a clean name + city + state back. No regex extraction.
            linkedin_items: list[dict] = []
            for item in items:
                link = (item.get("link") or "").strip()
                if not link or not is_linkedin_in_url(link):
                    continue
                linkedin_items.append(item)

            llm_map: dict[str, dict[str, str]] = {}
            if linkedin_items:
                try:
                    llm_map = extract_profiles_from_cse_items(linkedin_items, strategy_state=strategy_state)
                except Exception as e:
                    errors.append(f"llm_extract_failed: {e}")
                    llm_map = {}

            for item in linkedin_items:
                link = (item.get("link") or "").strip()
                if not link:
                    continue

                llm = llm_map.get(link) or {}

                # Name (LLM is source of truth)
                name_raw = (llm.get("name") or "").strip()
                # LLM sometimes leaks credentials into the name (e.g. ", RN" or "R.N.").
                # Strip common trailing creds before applying name cleaning.
                if name_raw:
                    # If comma exists and the RHS looks like credentials, keep only the LHS.
                    if "," in name_raw:
                        left, right = name_raw.split(",", 1)
                        right_letters = re.sub(r"[^a-z]", "", right.lower())
                        cred_tokens = {
                            "rn",
                            "bsn",
                            "msn",
                            "dnp",
                            "md",
                            "phd",
                            "mba",
                            "lpn",
                            "cna",
                            "np",
                            "aprn",
                            "pa",
                            "ccrn",
                            "cmsrn",
                            "ocn",
                            "cen",
                            "cnor",
                            "bc",
                        }
                        if any(tok in right_letters for tok in cred_tokens):
                            name_raw = left.strip()

                    # Strip trailing creds even without commas: "Jane Doe RN" / "Jane Doe R.N."
                    for _ in range(3):
                        new = re.sub(
                            r"(?:\s|,)*(?:r\.?\s*n\.?|rn|b\.?\s*s\.?\s*n\.?|bsn|m\.?\s*s\.?\s*n\.?|msn|dnp|md|phd|mba|lpn|cna|np|aprn|pa|ccrn|cmsrn|ocn|cen|cnor|bc)[\s\.,-]*$",
                            "",
                            name_raw,
                            flags=re.IGNORECASE,
                        ).strip(" ,.-")
                        if new == name_raw:
                            break
                        name_raw = new
                full_name = clean_person_name(name_raw) or ""
                if not full_name or _bad_profile_name(full_name):
                    _drop(
                        db,
                        owner_id=owner_id,
                        job_id=run.job_id,
                        strategy_run_id=run.id,
                        reason="missing_name",
                        linkedin_url_raw=link,
                        cse_item_json=item,
                    )
                    dropped += 1
                    continue

                # Title (AI extracted)
                title_extracted = (llm.get("title") or "").strip()

                # Location (LLM is preferred, but we will backfill from snippet/strategy_state if missing)
                city = (llm.get("city") or "").strip()
                state = _expand_us_state_abbrev((llm.get("state") or "").strip())

                # Occasionally we see "Colorado Denver" (state leaked into city).
                # Fix only the "STATE CITY" prefix case, while preserving real cities like "Kansas City"
                # and "Virginia Beach" (which would otherwise become "Beach").
                if city and state and " " not in state.strip():
                    st_lower = state.strip().lower()
                    cl = city.lower()
                    if cl.startswith(st_lower + " ") and not cl.endswith(" city"):
                        city2 = city[len(state) :].strip(" ,.-")
                        if city2:
                            # If the remainder is a common city suffix ("Beach", "Falls", etc),
                            # do NOT strip the state prefix (these are legitimate city names).
                            suffix_guard = {
                                "beach",
                                "falls",
                                "springs",
                                "heights",
                                "park",
                                "valley",
                                "river",
                                "lake",
                                "harbor",
                                "hills",
                                "island",
                                "islands",
                                "bend",
                                "grove",
                                "creek",
                                "point",
                                "port",
                                "bay",
                                "view",
                                "cove",
                            }
                            first_word = (city2.split(" ", 1)[0] or "").strip().lower()
                            if first_word not in suffix_guard:
                                city = city2

                # Country (LLM if available) — initialize early so snippet fallback can fill it.
                country_raw = (llm.get("country") or "").strip()

                # If city/state are missing (very common when snippet only says "Location: Miami"),
                # attempt deterministic snippet extraction; if still missing, use strategy_state.
                try:
                    snippet = str((item or {}).get("snippet") or "").strip()
                except Exception:
                    snippet = ""

                if snippet and ((not city) or (not state)):
                    try:
                        loc = extract_location_city_state(snippet, strategy_state=strategy_state)
                    except Exception:
                        loc = None
                    if loc:
                        city2, state2, country2 = loc
                        if not city and city2:
                            city = city2
                        if not state and state2:
                            state = _expand_us_state_abbrev(state2)

                        # Best-effort country fill
                        if not country_raw and country2:
                            country_raw = str(country2)

                if (not state) and strategy_state:
                    state = _expand_us_state_abbrev(strategy_state)

                country: Optional[str] = None
                if country_raw:
                    cl = re.sub(r"\s+", " ", country_raw).strip().lower()
                    if cl in {"united states", "united states of america", "usa", "us"}:
                        country = "United States"

                if (not city) or (not state) or _bad_profile_city(city, full_name) or _bad_profile_state(state):
                    _drop(
                        db,
                        owner_id=owner_id,
                        job_id=run.job_id,
                        strategy_run_id=run.id,
                        reason="missing_or_unsupported_location",
                        linkedin_url_raw=link,
                        cse_item_json=item,
                    )
                    dropped += 1
                    continue

                canonical = normalize_linkedin_url(link)
                prof = _get_or_create_profile(
                    db,
                    owner_id=owner_id,
                    linkedin_url_raw=link,
                    linkedin_url_canonical=canonical,
                    full_name=full_name,
                    city=city,
                    state=state,
                    country=country,
                    title=title_extracted or None,
                    cse_item_json=item,
                    allow_overwrite_if_no_phone=is_rerun,
                )

                if _profile_matches_job_requirement(prof, title_bools=title_bools, location_bools=location_bools):
                    if _ensure_job_profile_link(db, owner_id=owner_id, job_id=run.job_id, profile_id=prof.id):
                        added += 1
                        new_linked_profile_ids.append(prof.id)

            run.pages_completed += 1
            db.add(run)
            db.commit()

        # Finalize
        run.added_count = added
        run.dropped_count = dropped
        run.error_count = len(errors)
        run.last_error = errors[-1] if errors else None
        run.status = StrategyRunStatus.partial if errors else StrategyRunStatus.completed
        run.finished_at = _utcnow()
        db.add(run)
        db.commit()

        # Rebuild job_profile links so the job only contains profiles that match the job requirement
        # (selected title/location variants). This also pulls in already-existing matching profiles
        # from the global pool.
        try:
            _rebuild_job_profile_links(
                db,
                owner_id=owner_id,
                job_id=run.job_id,
                title_bools=title_bools,
                location_bools=location_bools,
            )
        except Exception:
            pass

        # Auto-enrich newly linked profiles from this scouting run.
        if new_linked_profile_ids:
            try:
                maybe_queue_job_auto_enrich(
                    db,
                    owner_id=owner_id,
                    job_id=run.job_id,
                    profile_ids=new_linked_profile_ids,
                )
            except Exception:
                pass

        # Queue next run for this job
        _queue_next_run(db, owner_id, run.job_id)

    except Exception as e:
        try:
            run = db.get(StrategyRun, strategy_run_id)
            if run:
                run.status = StrategyRunStatus.failed
                run.last_error = str(e)
                run.finished_at = _utcnow()
                db.add(run)
                db.commit()
                # IMPORTANT: failure should not block the rest of the queued runs for this job.
                # Continue the chain just like the success path does.
                try:
                    _queue_next_run(db, owner_id, run.job_id)
                except Exception:
                    pass
        finally:
            pass
    finally:
        db.close()


# ------------------------------------------------------------------------------
# Enrichment follow-ups (chain + retry)
# ------------------------------------------------------------------------------


@celery_app.task(name="retry_failed_enrichment")
def retry_failed_enrichment(enrichment_id: str) -> dict:
    db = SessionLocal()
    try:
        rec = db.get(ProfileEnrichment, enrichment_id)
        if not rec:
            return {"status": "skipped_not_found"}
        owner_id = rec.owner_id
        if rec.status != EnrichmentStatus.failed:
            return {"status": f"skipped_{rec.status.value}"}

        if _profile_has_any_phone(db, owner_id=owner_id, profile_id=rec.profile_id):
            return {"status": "skipped_has_phones"}

        rec.status = EnrichmentStatus.queued
        rec.started_at = None
        rec.finished_at = None
        rec.last_error = None
        db.add(rec)
        db.commit()
    finally:
        db.close()

    try:
        run_enrichment_dispatcher.delay()
    except Exception:
        pass
    return {"status": "queued"}


def _post_enrichment_followups(enrichment_id: str) -> None:
    db = SessionLocal()
    try:
        rec = db.get(ProfileEnrichment, enrichment_id)
        if not rec:
            return
        owner_id = rec.owner_id

        # Schedule a single retry for failed records after 1 minute.
        if rec.status == EnrichmentStatus.failed:
            raw = dict(rec.raw_json or {})
            retry_count = int(raw.get("retry_count") or 0)
            if retry_count < 1:
                raw["retry_count"] = retry_count + 1
                raw["retry_scheduled_at"] = _utcnow().isoformat()
                rec.raw_json = raw
                db.add(rec)
                db.commit()
                try:
                    retry_failed_enrichment.apply_async(args=[rec.id], countdown=60)
                except Exception:
                    pass

        # Chain fallback (TPS -> ABC -> USPhonebook) if enabled for any job.
        if rec.status in (EnrichmentStatus.completed, EnrichmentStatus.failed):
            phones = rec.phone_numbers or []
            if not phones and _profile_has_chain_job(db, owner_id=owner_id, profile_id=rec.profile_id):
                queue_chain_for_profiles(db, owner_id=owner_id, profile_ids=[rec.profile_id])
    finally:
        db.close()


@celery_app.task(name="rebuild_job_profiles")
def rebuild_job_profiles(job_id: str) -> dict:
    """
    Rebuild/prune job_profiles for a job based on current selected title/location variants.
    Useful when variants are toggled or job requirements change.
    """
    db = SessionLocal()
    try:
        job = db.get(Job, job_id)
        if not job:
            return {"error": "job_not_found"}
        owner_id = job.owner_id
        title_bools, location_bools = _job_requirement_bools(db, owner_id=owner_id, job_id=job_id)
        return _rebuild_job_profile_links(
            db,
            owner_id=owner_id,
            job_id=job_id,
            title_bools=title_bools,
            location_bools=location_bools,
        )
    finally:
        db.close()


@celery_app.task(name="repair_job_profile_extraction")
def repair_job_profile_extraction(job_id: str) -> dict:
    """
    Best-effort repair for existing profiles linked to a job where name/location
    was stored incorrectly from earlier CSE parsing.

    Strategy:
    - Only touches profiles whose stored fields look bad.
    - Try regex-based re-parse from stored cse_item_json first (free).
    - If still bad and OpenRouter is configured, use a tiny batched LLM call.
    """
    db = SessionLocal()
    try:
        job = db.get(Job, job_id)
        if not job:
            return {"job_id": job_id, "error": "job_not_found"}
        owner_id = job.owner_id

        profiles = (
            db.query(Profile)
            .join(JobProfile, (JobProfile.profile_id == Profile.id) & (JobProfile.owner_id == owner_id))
            .filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id)
            .order_by(JobProfile.created_at.desc())
            .all()
        )

        suspects = [
            p
            for p in profiles
            if _bad_profile_name(p.full_name_raw)
            or _bad_profile_city(p.city or "", p.full_name_raw)
            or _bad_profile_state(p.state or "")
        ]

        stats = {
            "job_id": job_id,
            "total": len(profiles),
            "suspects": len(suspects),
            "updated_free": 0,
            "llm_attempted": 0,
            "llm_updated": 0,
        }

        llm_items: list[dict] = []
        link_to_profile: dict[str, Profile] = {}

        for p in suspects:
            cse = p.cse_item_json or {}
            title = str(cse.get("title") or "").strip()
            snippet = str(cse.get("snippet") or "").strip()

            changed = False

            # Name repair (regex)
            if title and _bad_profile_name(p.full_name_raw):
                new_name = extract_name_from_title(title)
                if new_name and not _bad_profile_name(new_name):
                    p.full_name_raw = new_name
                    p.first_name, p.last_name = split_first_last(new_name)
                    changed = True

            # Location repair (regex)
            if snippet and (_bad_profile_city(p.city or "", p.full_name_raw) or _bad_profile_state(p.state or "")):
                loc = extract_location_city_state(snippet, strategy_state=None)
                if loc:
                    city, state, country = loc
                    state = _expand_us_state_abbrev(state)

                    # Only accept regex locations that look valid (this avoids false hits like
                    # "Hannah Bass, RN, MSN" being mis-parsed as City/State/Country).
                    if not _bad_profile_city(city, p.full_name_raw) and not _bad_profile_state(state):
                        if city and _bad_profile_city(p.city or "", p.full_name_raw) and not _bad_profile_city(
                            city, p.full_name_raw
                        ):
                            p.city = city
                            changed = True
                        if state and _bad_profile_state(p.state or "") and not _bad_profile_state(state):
                            p.state = state
                            changed = True

                        country_norm = None
                        c = (country or "").strip()
                        if c:
                            cl = re.sub(r"\s+", " ", c).strip().lower()
                            if cl in {"united states", "united states of america", "usa", "us"}:
                                country_norm = "United States"
                        if country_norm and (p.country or "") != country_norm:
                            p.country = country_norm
                            changed = True

            if changed:
                db.add(p)
                stats["updated_free"] += 1
                continue

            # Still bad → LLM fallback (only if we have something to parse)
            link = (p.linkedin_url_raw or p.linkedin_url_canonical or "").strip()
            if link and (title or snippet):
                llm_items.append({"link": link, "title": title, "snippet": snippet})
                link_to_profile[link] = p

        if llm_items:
            stats["llm_attempted"] = len(llm_items)
            try:
                llm_map = extract_profiles_from_cse_items(llm_items, strategy_state=None)
            except Exception:
                llm_map = {}

            for link, data in llm_map.items():
                p = link_to_profile.get(link)
                if not p:
                    continue

                changed = False

                name_raw = str((data or {}).get("name") or "").strip()
                if name_raw and _bad_profile_name(p.full_name_raw):
                    name_clean = clean_person_name(name_raw)
                    if name_clean and not _bad_profile_name(name_clean):
                        p.full_name_raw = name_clean
                        p.first_name, p.last_name = split_first_last(name_clean)
                        changed = True

                city_raw = str((data or {}).get("city") or "").strip()
                state_raw = str((data or {}).get("state") or "").strip()
                if city_raw and state_raw:
                    state_raw = _expand_us_state_abbrev(state_raw)
                    if _bad_profile_city(p.city or "", p.full_name_raw) and not _bad_profile_city(city_raw, p.full_name_raw):
                        p.city = city_raw
                        changed = True
                    if _bad_profile_state(p.state or "") and not _bad_profile_state(state_raw):
                        p.state = state_raw
                        changed = True

                country_raw = str((data or {}).get("country") or "").strip()
                if country_raw:
                    cl = re.sub(r"\s+", " ", country_raw).strip().lower()
                    if cl in {"united states", "united states of america", "usa", "us"}:
                        if (p.country or "") != "United States":
                            p.country = "United States"
                            changed = True

                if changed:
                    db.add(p)
                    stats["llm_updated"] += 1

        db.commit()
        return stats
    finally:
        db.close()


@celery_app.task(name="enrich_profile_enrichment")
def enrich_profile_enrichment(enrichment_id: str) -> None:
    """
    Enrich a profile (e.g., phone lookup).

    This is intentionally strict:
    - If ENRICH_PROVIDER is not configured, it fails with a clear error.
    - Provider integrations can be added later without changing the queue/API.
    """
    db = SessionLocal()
    try:
        rec = db.get(ProfileEnrichment, enrichment_id)
        if not rec:
            return
        owner_id = rec.owner_id
        if rec.status != EnrichmentStatus.queued:
            return

        rec.status = EnrichmentStatus.running
        rec.started_at = _utcnow()
        db.add(rec)
        db.commit()
        db.refresh(rec)

        prof = db.get(Profile, rec.profile_id)
        if not prof:
            rec.status = EnrichmentStatus.failed
            rec.last_error = "profile_not_found"
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            return

        provider = (rec.provider or "disabled").strip().lower()

        # Provider integrations (EnformionGo later). No guessing: fail clearly until configured.
        if provider == "disabled":
            raise RuntimeError(
                "Enrichment disabled. Set ENRICH_PROVIDER and provider credentials in your environment."
            )
        if provider == "enformiongo":
            if not settings.ENFORMIONGO_API_KEY:
                raise RuntimeError("ENFORMIONGO_API_KEY is not set")
            raise RuntimeError("EnformionGo integration is not implemented yet in this codebase")

        if provider == "scrapedo_advancedbackgroundchecks":
            if not settings.SCRAPEDO_API_KEY:
                raise RuntimeError("SCRAPEDO_API_KEY is not set")

            candidate_urls, build_error, keys = _candidate_abc_names_urls(prof)
            attempts: list[dict] = []
            phones: list[str] = []
            selected: Optional[dict] = None
            detail_attempt: Optional[dict] = None
            picked = None

            target_first = (keys.get("target_first") or "").strip().lower()
            target_last = (keys.get("target_last") or "").strip().lower()
            target_city_slug = (keys.get("target_city_slug") or "").strip().lower()
            target_state = (keys.get("target_state_abbrev") or "").strip().upper()

            # If we had to re-parse location from snippet, repair the stored profile location so
            # we don't keep generating garbage URLs later.
            try:
                if keys.get("location_fallback_used") and keys.get("city_raw") and keys.get("state_raw"):
                    prof.city = str(keys["city_raw"])
                    prof.state = str(keys["state_raw"])
                    db.add(prof)
            except Exception:
                pass

            if not candidate_urls:
                # Can't attempt lookup; treat as completed-with-zero (do not invent data).
                rec.phone_numbers = []
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_ABC_PROVIDER_VERSION,
                    "reason": build_error or "no_candidate_urls",
                    "match_keys": keys,
                    "profile": {
                        "full_name_raw": prof.full_name_raw,
                        "first_name": prof.first_name,
                        "last_name": prof.last_name,
                        "city": prof.city,
                        "state": prof.state,
                    },
                }
            else:
                # Step 1: fetch /names page, parse cards, choose first card matching (name + city + state)
                for names_url in candidate_urls:
                    status_code, html, content_type = _scrapedo_fetch_html(
                        token=settings.SCRAPEDO_API_KEY, target_url=names_url, timeout_s=90
                    )
                    attempt = {
                        "names_url": names_url,
                        "status_code": status_code,
                        "content_type": content_type,
                        "html_len": len(html or ""),
                    }
                    attempts.append(attempt)

                    # 404 = likely no matching page for this slug
                    if status_code == 404:
                        continue

                    # Any other non-2xx is treated as a provider failure (credentials / rate-limit / etc.)
                    if status_code < 200 or status_code >= 300:
                        if status_code in (401, 403):
                            raise RuntimeError(
                                f"scrapedo_auth_failed_http_{status_code}: check SCRAPEDO_API_KEY"
                            )
                        if status_code == 429:
                            raise RuntimeError("scrapedo_rate_limited_http_429")
                        raise RuntimeError(f"scrapedo_http_{status_code}")

                    results = _parse_abc_name_results(html or "")
                    attempt["results_count"] = len(results)

                    match_strict = None
                    match_name_only = None

                    for r in results:
                        if not r.get("detail_url"):
                            continue
                        cf, cl, err = _name_first_last_for_abc(r.get("name") or "")
                        if err or not cf or not cl:
                            continue
                        if cf.strip().lower() != target_first or cl.strip().lower() != target_last:
                            continue

                        if match_name_only is None:
                            match_name_only = r

                        city = (r.get("city") or "").strip()
                        st = (r.get("state") or "").strip().upper()
                        if city and st and _slugify(city) == target_city_slug and st == target_state:
                            match_strict = r
                            break

                    match = match_strict or match_name_only
                    if not match:
                        continue

                    selected = {
                        "name": match.get("name"),
                        "city": match.get("city"),
                        "state": match.get("state"),
                        "detail_url": match.get("detail_url"),
                        "index": match.get("index"),
                        "match_reason": ("name_city_state" if match_strict else "name_only"),
                    }

                    # Step 2: open detail page and extract phones from there
                    detail_url = match.get("detail_url")
                    if not detail_url:
                        break
                    d_status, d_html, d_ctype = _scrapedo_fetch_html(
                        token=settings.SCRAPEDO_API_KEY, target_url=detail_url, timeout_s=90
                    )
                    detail_attempt = {
                        "detail_url": detail_url,
                        "status_code": d_status,
                        "content_type": d_ctype,
                        "html_len": len(d_html or ""),
                    }

                    if d_status == 404:
                        phones = []
                        break

                    if d_status < 200 or d_status >= 300:
                        if d_status in (401, 403):
                            raise RuntimeError(
                                f"scrapedo_auth_failed_http_{d_status}: check SCRAPEDO_API_KEY"
                            )
                        if d_status == 429:
                            raise RuntimeError("scrapedo_rate_limited_http_429")
                        raise RuntimeError(f"scrapedo_http_{d_status}")

                    best = _extract_abc_first_wireless_phone(d_html or "")
                    phones = [best] if best else []
                    break

                rec.phone_numbers = phones
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_ABC_PROVIDER_VERSION,
                    "match_keys": keys,
                    "abc": {
                        "candidate_urls": candidate_urls,
                        "attempts": attempts,
                        "selected": selected,
                        "detail": detail_attempt,
                        "picked": {"strategy": "first_wireless", "phone": (phones[0] if phones else None)},
                    },
                }

            rec.status = EnrichmentStatus.completed
            rec.last_error = None
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            return

        raise RuntimeError(f"Unknown enrichment provider: {provider}")

    except Exception as e:
        try:
            rec = db.get(ProfileEnrichment, enrichment_id)
            if rec:
                rec.status = EnrichmentStatus.failed
                rec.last_error = str(e)
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
        finally:
            pass
    finally:
        db.close()


# ------------------------------------------------------------------------------
# Round-robin enrichment dispatcher (uses ThreadPoolExecutor for parallelism)
# ------------------------------------------------------------------------------


def _enrich_one_sync(enrichment_id: str) -> dict:
    """
    Enrich a single profile synchronously.
    Returns result dict with status info.
    """
    result = {"enrichment_id": enrichment_id, "status": "unknown", "error": None, "phones": 0, "retry": False}

    db = SessionLocal()
    try:
        rec = db.get(ProfileEnrichment, enrichment_id)
        if not rec:
            result["status"] = "skipped_not_found"
            return result
        owner_id = rec.owner_id
        if rec.status != EnrichmentStatus.queued:
            result["status"] = f"skipped_{rec.status.value}"
            return result

        # Mark as running
        rec.status = EnrichmentStatus.running
        rec.started_at = _utcnow()
        db.add(rec)
        db.commit()
        db.refresh(rec)

        prof = db.get(Profile, rec.profile_id)
        if not prof:
            rec.status = EnrichmentStatus.failed
            rec.last_error = "profile_not_found"
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            result["status"] = "failed_profile_not_found"
            return result

        provider = (rec.provider or "disabled").strip().lower()

        if provider == "disabled":
            rec.status = EnrichmentStatus.failed
            rec.last_error = "enrichment_disabled"
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            result["status"] = "failed_disabled"
            return result

        if provider not in ("scrapedo_advancedbackgroundchecks", "scrapedo_tps", "scrapedo_usphonebook"):
            rec.status = EnrichmentStatus.failed
            rec.last_error = f"unsupported_provider:{provider}"
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            result["status"] = "failed_unsupported_provider"
            return result

        if not settings.SCRAPEDO_API_KEY:
            rec.status = EnrichmentStatus.failed
            rec.last_error = "SCRAPEDO_API_KEY_not_set"
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            result["status"] = "failed_no_api_key"
            return result

        # ------------------------------------------------------------------
        # TPS — Scrape.do (TruePeopleSearch)
        # ------------------------------------------------------------------
        if provider == "scrapedo_tps":
            first_raw = (prof.first_name or "").strip()
            last_raw = (prof.last_name or "").strip()
            city_raw = (prof.city or "").strip()
            state_raw = (prof.state or "").strip()
            st = _state_to_abbrev(state_raw)

            # Defensive: do not retry forever on missing/invalid profile data.
            if not first_raw or not last_raw:
                rec.phone_numbers = []
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_TPS_PROVIDER_VERSION,
                    "reason": "missing_first_last",
                    "profile": {"first_name": prof.first_name, "last_name": prof.last_name},
                }
                rec.status = EnrichmentStatus.completed
                rec.last_error = None
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "completed_missing_name"
                return result

            if not city_raw or not st:
                rec.phone_numbers = []
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_TPS_PROVIDER_VERSION,
                    "reason": ("missing_location" if not city_raw else "unsupported_state"),
                    "profile": {"city": prof.city, "state": prof.state},
                }
                rec.status = EnrichmentStatus.completed
                rec.last_error = None
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "completed_missing_location"
                return result

            target_first_token = (first_raw.split(" ")[0] if first_raw else "").strip().lower()
            target_last = last_raw.strip().lower()
            target_city_slug = _slugify(city_raw)
            target_state = st.strip().upper()

            full_name = f"{first_raw} {last_raw}".strip()
            search_url = str(
                httpx.URL(
                    _TPS_BASE_URL + "/results",
                    params={"name": full_name, "citystatezip": f"{city_raw}, {target_state}"},
                )
            )

            # Step 1: fetch results page
            try:
                status_code, html, _ctype = _scrapedo_fetch_html(
                    token=settings.SCRAPEDO_API_KEY,
                    target_url=search_url,
                    timeout_s=90,
                    super_mode=True,
                )
            except httpx.TimeoutException:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "tps_timeout_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_tps_timeout"
                result["retry"] = True
                return result
            except Exception as e:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"tps_fetch_error:{e}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_tps_fetch_error"
                result["error"] = str(e)
                return result

            search_attempt = {"search_url": search_url, "status_code": status_code, "html_len": len(html or "")}

            if status_code == 429:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "tps_rate_limited_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_tps_429"
                result["retry"] = True
                return result

            if status_code in (401, 403):
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"tps_scrapedo_auth_http_{status_code}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_tps_auth"
                return result

            if status_code == 404:
                rec.phone_numbers = []
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_TPS_PROVIDER_VERSION,
                    "match_keys": {
                        "target_first_token": target_first_token,
                        "target_last": target_last,
                        "target_city_slug": target_city_slug,
                        "target_state_abbrev": target_state,
                    },
                    "tps": {"search": search_attempt, "results_count": 0, "selected": None, "detail": None},
                }
                rec.status = EnrichmentStatus.completed
                rec.last_error = None
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "completed_tps_404"
                return result

            if status_code < 200 or status_code >= 300:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"tps_scrapedo_http_{status_code}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = f"failed_tps_http_{status_code}"
                return result

            # Parse candidates
            candidates = _parse_tps_results(html or "")
            search_attempt["results_count"] = len(candidates)

            selected: Optional[dict] = None
            match_strict = None
            match_name_state = None
            match_name_only = None
            fallback_first = (
                candidates[0]
                if candidates and all(not (c.get("name") or "").strip() for c in candidates)
                else None
            )

            for c in candidates:
                detail_url = c.get("detail_url")
                if not detail_url:
                    continue

                cand_state = (c.get("state") or "").strip()
                cand_state_abbr = _state_to_abbrev(cand_state) or cand_state.upper()

                # Name match: tolerate middle names/initials by substring search.
                cand_name_lower = (c.get("name") or "").strip().lower()
                if not cand_name_lower:
                    continue
                if target_first_token not in cand_name_lower or target_last not in cand_name_lower:
                    continue

                # Require state match if candidate state is parseable.
                if cand_state_abbr and cand_state_abbr.upper() != target_state:
                    continue

                if cand_state_abbr:
                    if match_name_state is None:
                        match_name_state = c
                else:
                    if match_name_only is None:
                        match_name_only = c

                cand_city = (c.get("city") or "").strip()
                if cand_state_abbr and cand_city and _slugify(cand_city) == target_city_slug:
                    match_strict = c
                    break

            match = None
            match_reason = None
            if match_strict:
                match = match_strict
                match_reason = "name_city_state"
            elif match_name_state:
                match = match_name_state
                match_reason = "name_state"
            elif match_name_only:
                match = match_name_only
                match_reason = "name_only"
            elif fallback_first:
                match = fallback_first
                match_reason = "first_result_fallback"
            phones: list[str] = []
            detail_attempt: Optional[dict] = None
            picked = None

            if match:
                selected = {
                    "name": match.get("name"),
                    "city": match.get("city"),
                    "state": match.get("state"),
                    "detail_url": match.get("detail_url"),
                    "index": match.get("index"),
                    "match_reason": match_reason,
                }
                detail_url = match.get("detail_url")
                if detail_url:
                    try:
                        d_status, d_html, _d_ctype = _scrapedo_fetch_html(
                            token=settings.SCRAPEDO_API_KEY,
                            target_url=str(detail_url),
                            timeout_s=90,
                            super_mode=True,
                        )
                    except httpx.TimeoutException:
                        rec.status = EnrichmentStatus.queued
                        rec.started_at = None
                        rec.last_error = "tps_detail_timeout_will_retry"
                        db.add(rec)
                        db.commit()
                        result["status"] = "retry_tps_detail_timeout"
                        result["retry"] = True
                        return result
                    except Exception as e:
                        rec.status = EnrichmentStatus.failed
                        rec.last_error = f"tps_detail_fetch_error:{e}"
                        rec.finished_at = _utcnow()
                        db.add(rec)
                        db.commit()
                        result["status"] = "failed_tps_detail_fetch"
                        result["error"] = str(e)
                        return result

                    detail_attempt = {
                        "detail_url": detail_url,
                        "status_code": d_status,
                        "html_len": len(d_html or ""),
                    }

                    if d_status == 429:
                        rec.status = EnrichmentStatus.queued
                        rec.started_at = None
                        rec.last_error = "tps_detail_rate_limited_will_retry"
                        db.add(rec)
                        db.commit()
                        result["status"] = "retry_tps_detail_429"
                        result["retry"] = True
                        return result

                    if d_status in (401, 403):
                        rec.status = EnrichmentStatus.failed
                        rec.last_error = f"tps_detail_scrapedo_auth_http_{d_status}"
                        rec.finished_at = _utcnow()
                        db.add(rec)
                        db.commit()
                        result["status"] = "failed_tps_detail_auth"
                        return result

                    if d_status == 404:
                        phones = []
                    elif d_status < 200 or d_status >= 300:
                        rec.status = EnrichmentStatus.failed
                        rec.last_error = f"tps_detail_scrapedo_http_{d_status}"
                        rec.finished_at = _utcnow()
                        db.add(rec)
                        db.commit()
                        result["status"] = f"failed_tps_detail_http_{d_status}"
                        return result
                    else:
                        best_phone, best_last = _extract_tps_best_wireless_phone(d_html or "")
                        phones = [best_phone] if best_phone else []
                        picked = {
                            "strategy": "most_recent_wireless",
                            "phone": best_phone,
                            "last_reported": best_last,
                        }
                else:
                    picked = None
            else:
                picked = None

            rec.phone_numbers = phones
            rec.raw_json = {
                "provider": provider,
                "provider_version": _SCRAPEDO_TPS_PROVIDER_VERSION,
                "match_keys": {
                    "target_first_token": target_first_token,
                    "target_last": target_last,
                    "target_city_slug": target_city_slug,
                    "target_state_abbrev": target_state,
                },
                "tps": {
                    "search": search_attempt,
                    "selected": selected,
                    "detail": detail_attempt,
                    "picked": picked,
                },
            }
            rec.status = EnrichmentStatus.completed
            rec.last_error = None
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()

            result["status"] = "completed"
            result["phones"] = len(phones)
            return result

        # ------------------------------------------------------------------
        # USPhonebook — Scrape.do
        # ------------------------------------------------------------------
        if provider == "scrapedo_usphonebook":
            candidate_urls, build_error, keys = _candidate_usphonebook_listing_urls(prof)

            attempts: list[dict] = []
            phones: list[str] = []
            selected: Optional[dict] = None
            detail_attempt: Optional[dict] = None
            picked = None

            target_first_token = (keys.get("target_first_token") or "").strip().lower()
            target_last = (keys.get("target_last") or "").strip().lower()
            target_city_slug = (keys.get("target_city_slug") or "").strip().lower()
            target_state = (keys.get("target_state_abbrev") or "").strip().upper()

            if not candidate_urls:
                rec.phone_numbers = []
                rec.raw_json = {
                    "provider": provider,
                    "provider_version": _SCRAPEDO_USPHONEBOOK_PROVIDER_VERSION,
                    "reason": build_error or "no_candidate_urls",
                    "match_keys": keys,
                }
                rec.status = EnrichmentStatus.completed
                rec.last_error = None
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "completed_no_urls"
                return result

            for listing_url in candidate_urls:
                try:
                    status_code, html, _ctype = _scrapedo_fetch_html(
                        token=settings.SCRAPEDO_API_KEY,
                        target_url=listing_url,
                        timeout_s=90,
                        super_mode=True,
                    )
                except httpx.TimeoutException:
                    rec.status = EnrichmentStatus.queued
                    rec.started_at = None
                    rec.last_error = "usphonebook_timeout_will_retry"
                    db.add(rec)
                    db.commit()
                    result["status"] = "retry_usphonebook_timeout"
                    result["retry"] = True
                    return result
                except Exception as e:
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_fetch_error:{e}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = "failed_usphonebook_fetch_error"
                    result["error"] = str(e)
                    return result

                attempt = {"listing_url": listing_url, "status_code": status_code, "html_len": len(html or "")}
                attempts.append(attempt)

                if status_code == 429:
                    rec.status = EnrichmentStatus.queued
                    rec.started_at = None
                    rec.last_error = "usphonebook_rate_limited_will_retry"
                    db.add(rec)
                    db.commit()
                    result["status"] = "retry_usphonebook_429"
                    result["retry"] = True
                    return result

                if status_code == 404:
                    continue

                if status_code in (401, 403):
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_scrapedo_auth_http_{status_code}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = "failed_usphonebook_auth"
                    return result

                if status_code < 200 or status_code >= 300:
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_scrapedo_http_{status_code}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = f"failed_usphonebook_http_{status_code}"
                    return result

                results = _parse_usphonebook_listing_results(html or "")
                attempt["results_count"] = len(results)

                match_strict = None
                match_name_only = None

                for r in results:
                    detail_url = r.get("detail_url")
                    if not detail_url:
                        continue

                    tokens2, err = _name_tokens_for_abc(r.get("name") or "")
                    if err or len(tokens2) < 2:
                        continue
                    cand_first = (tokens2[0] or "").strip().lower()
                    cand_last = (tokens2[-1] or "").strip().lower()
                    if cand_first != target_first_token or cand_last != target_last:
                        continue

                    if match_name_only is None:
                        match_name_only = r

                    cand_city = (r.get("city") or "").strip()
                    cand_state = (r.get("state") or "").strip().upper()
                    if cand_city and cand_state and _slugify(cand_city) == target_city_slug and cand_state == target_state:
                        match_strict = r
                        break

                match = match_strict or match_name_only
                if not match:
                    continue

                selected = {
                    "name": match.get("name"),
                    "city": match.get("city"),
                    "state": match.get("state"),
                    "detail_url": match.get("detail_url"),
                    "index": match.get("index"),
                    "match_reason": ("name_city_state" if match_strict else "name_only"),
                }

                detail_url = match.get("detail_url")
                if not detail_url:
                    break

                try:
                    d_status, d_html, _d_ctype = _scrapedo_fetch_html(
                        token=settings.SCRAPEDO_API_KEY,
                        target_url=str(detail_url),
                        timeout_s=90,
                        super_mode=True,
                    )
                except httpx.TimeoutException:
                    rec.status = EnrichmentStatus.queued
                    rec.started_at = None
                    rec.last_error = "usphonebook_detail_timeout_will_retry"
                    db.add(rec)
                    db.commit()
                    result["status"] = "retry_usphonebook_detail_timeout"
                    result["retry"] = True
                    return result
                except Exception as e:
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_detail_fetch_error:{e}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = "failed_usphonebook_detail_fetch"
                    result["error"] = str(e)
                    return result

                detail_attempt = {"detail_url": detail_url, "status_code": d_status, "html_len": len(d_html or "")}

                if d_status == 429:
                    rec.status = EnrichmentStatus.queued
                    rec.started_at = None
                    rec.last_error = "usphonebook_detail_rate_limited_will_retry"
                    db.add(rec)
                    db.commit()
                    result["status"] = "retry_usphonebook_detail_429"
                    result["retry"] = True
                    return result

                if d_status in (401, 403):
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_detail_scrapedo_auth_http_{d_status}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = "failed_usphonebook_detail_auth"
                    return result

                if d_status == 404:
                    phones = []
                    break
                if d_status < 200 or d_status >= 300:
                    rec.status = EnrichmentStatus.failed
                    rec.last_error = f"usphonebook_detail_scrapedo_http_{d_status}"
                    rec.finished_at = _utcnow()
                    db.add(rec)
                    db.commit()
                    result["status"] = f"failed_usphonebook_detail_http_{d_status}"
                    return result

                best_phone, best_last_ts, best_carrier = _extract_usphonebook_best_wireless_phone(d_html or "")
                phones = [best_phone] if best_phone else []
                picked = {
                    "strategy": "most_recent_wireless",
                    "phone": best_phone,
                    "last_report_unix": best_last_ts,
                    "carrier": best_carrier,
                }
                break

            rec.phone_numbers = phones
            rec.raw_json = {
                "provider": provider,
                "provider_version": _SCRAPEDO_USPHONEBOOK_PROVIDER_VERSION,
                "match_keys": keys,
                "usphonebook": {
                    "candidate_urls": candidate_urls,
                    "attempts": attempts,
                    "selected": selected,
                    "detail": detail_attempt,
                    "picked": picked,
                },
            }
            rec.status = EnrichmentStatus.completed
            rec.last_error = None
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()

            result["status"] = "completed"
            result["phones"] = len(phones)
            return result

        # Build candidate URLs
        candidate_urls, build_error, keys = _candidate_abc_names_urls(prof)
        attempts: list[dict] = []
        phones: list[str] = []
        selected: Optional[dict] = None
        detail_attempt: Optional[dict] = None

        target_first = (keys.get("target_first") or "").strip().lower()
        target_last = (keys.get("target_last") or "").strip().lower()
        target_city_slug = (keys.get("target_city_slug") or "").strip().lower()
        target_state = (keys.get("target_state_abbrev") or "").strip().upper()

        # Repair location if fallback was used
        try:
            if keys.get("location_fallback_used") and keys.get("city_raw") and keys.get("state_raw"):
                prof.city = str(keys["city_raw"])
                prof.state = str(keys["state_raw"])
                db.add(prof)
        except Exception:
            pass

        if not candidate_urls:
            rec.phone_numbers = []
            rec.raw_json = {
                "provider": provider,
                "provider_version": _SCRAPEDO_ABC_PROVIDER_VERSION,
                "reason": build_error or "no_candidate_urls",
                "match_keys": keys,
            }
            rec.status = EnrichmentStatus.completed
            rec.last_error = None
            rec.finished_at = _utcnow()
            db.add(rec)
            db.commit()
            result["status"] = "completed_no_urls"
            return result

        # Step 1: fetch /names page
        for names_url in candidate_urls:
            try:
                status_code, html, content_type = _scrapedo_fetch_html(
                    token=settings.SCRAPEDO_API_KEY,
                    target_url=names_url,
                    timeout_s=90,
                    super_mode=True,
                )
            except httpx.TimeoutException:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "timeout_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_timeout"
                result["retry"] = True
                return result
            except Exception as e:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"fetch_error:{e}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_fetch_error"
                result["error"] = str(e)
                return result

            attempt = {
                "names_url": names_url,
                "status_code": status_code,
                "html_len": len(html or ""),
            }
            attempts.append(attempt)

            # Rate limited - retry later
            if status_code == 429:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "rate_limited_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_429"
                result["retry"] = True
                return result

            if status_code == 404:
                continue

            if status_code in (401, 403):
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"scrapedo_auth_failed_http_{status_code}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_auth"
                return result

            if status_code < 200 or status_code >= 300:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"scrapedo_http_{status_code}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = f"failed_http_{status_code}"
                return result

            # Parse results
            abc_results = _parse_abc_name_results(html or "")
            attempt["results_count"] = len(abc_results)

            match_strict = None
            match_name_only = None

            for r in abc_results:
                if not r.get("detail_url"):
                    continue
                cf, cl, err = _name_first_last_for_abc(r.get("name") or "")
                if err or not cf or not cl:
                    continue
                if cf.strip().lower() != target_first or cl.strip().lower() != target_last:
                    continue

                if match_name_only is None:
                    match_name_only = r

                city = (r.get("city") or "").strip()
                st = (r.get("state") or "").strip().upper()
                if city and st and _slugify(city) == target_city_slug and st == target_state:
                    match_strict = r
                    break

            match = match_strict or match_name_only
            if not match:
                continue

            selected = {
                "name": match.get("name"),
                "city": match.get("city"),
                "state": match.get("state"),
                "detail_url": match.get("detail_url"),
                "index": match.get("index"),
                "match_reason": ("name_city_state" if match_strict else "name_only"),
            }

            # Step 2: fetch detail page
            detail_url = match.get("detail_url")
            if not detail_url:
                break

            try:
                d_status, d_html, d_ctype = _scrapedo_fetch_html(
                    token=settings.SCRAPEDO_API_KEY,
                    target_url=detail_url,
                    timeout_s=90,
                    super_mode=True,
                )
            except httpx.TimeoutException:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "detail_timeout_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_detail_timeout"
                result["retry"] = True
                return result
            except Exception as e:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"detail_fetch_error:{e}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_detail_fetch"
                result["error"] = str(e)
                return result

            detail_attempt = {
                "detail_url": detail_url,
                "status_code": d_status,
                "html_len": len(d_html or ""),
            }

            if d_status == 429:
                rec.status = EnrichmentStatus.queued
                rec.started_at = None
                rec.last_error = "detail_rate_limited_will_retry"
                db.add(rec)
                db.commit()
                result["status"] = "retry_detail_429"
                result["retry"] = True
                return result

            if d_status == 404:
                phones = []
                break

            if d_status in (401, 403):
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"detail_scrapedo_auth_http_{d_status}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = "failed_detail_auth"
                return result

            if d_status < 200 or d_status >= 300:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"detail_scrapedo_http_{d_status}"
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
                result["status"] = f"failed_detail_http_{d_status}"
                return result

            best = _extract_abc_first_wireless_phone(d_html or "")
            phones = [best] if best else []
            break

        # Save results
        rec.phone_numbers = phones
        rec.raw_json = {
            "provider": provider,
            "provider_version": _SCRAPEDO_ABC_PROVIDER_VERSION,
            "match_keys": keys,
            "abc": {
                "candidate_urls": candidate_urls,
                "attempts": attempts,
                "selected": selected,
                "detail": detail_attempt,
                "picked": {"strategy": "first_wireless", "phone": (phones[0] if phones else None)},
            },
        }
        rec.status = EnrichmentStatus.completed
        rec.last_error = None
        rec.finished_at = _utcnow()
        db.add(rec)
        db.commit()

        result["status"] = "completed"
        result["phones"] = len(phones)
        return result

    except Exception as e:
        try:
            rec = db.get(ProfileEnrichment, enrichment_id)
            if rec:
                rec.status = EnrichmentStatus.failed
                rec.last_error = str(e)
                rec.finished_at = _utcnow()
                db.add(rec)
                db.commit()
        except Exception:
            pass
        result["status"] = "failed_exception"
        result["error"] = str(e)
        return result
    finally:
        db.close()


@celery_app.task(name="run_enrichment_dispatcher")
def run_enrichment_dispatcher() -> dict:
    """
    Round-robin enrichment dispatcher using ThreadPoolExecutor.
    Processes ALL queued enrichments across all jobs with fair scheduling
    and configurable concurrency (default 25).
    Auto-retries on rate limit (429) or timeout.
    """
    import concurrent.futures

    concurrency = max(settings.ENRICH_CONCURRENCY, 1)
    backoff_s = max(settings.ENRICH_RETRY_BACKOFF_SECONDS, 0.5)

    stats = {
        "total_processed": 0,
        "completed": 0,
        "failed": 0,
        "retried": 0,
        "phones_found": 0,
    }

    retry_list: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        while True:
            # Fetch queued enrichments grouped by job
            db = SessionLocal()
            try:
                rows = (
                    db.query(ProfileEnrichment.id, JobProfile.job_id)
                    .join(
                        JobProfile,
                        (JobProfile.profile_id == ProfileEnrichment.profile_id)
                        & (JobProfile.owner_id == ProfileEnrichment.owner_id),
                    )
                    .outerjoin(
                        JobSettings,
                        (JobSettings.job_id == JobProfile.job_id) & (JobSettings.owner_id == JobProfile.owner_id),
                    )
                    .filter(
                        ProfileEnrichment.status == EnrichmentStatus.queued,
                        func.coalesce(JobSettings.paused, False) == False,  # noqa: E712
                    )
                    .order_by(ProfileEnrichment.created_at)
                    .all()
                )
            finally:
                db.close()

            # Group by job_id
            job_queues: dict[str, list[str]] = defaultdict(list)
            for enrichment_id, job_id in rows:
                job_queues[job_id].append(enrichment_id)

            # Add retry items back
            if retry_list:
                db = SessionLocal()
                try:
                    for eid in retry_list:
                        rec = db.get(ProfileEnrichment, eid)
                        if rec and rec.status == EnrichmentStatus.queued:
                            jp = (
                                db.query(JobProfile.job_id)
                                .filter(
                                    JobProfile.profile_id == rec.profile_id,
                                    JobProfile.owner_id == rec.owner_id,
                                )
                                .first()
                            )
                            if jp:
                                # Skip paused jobs
                                paused_row = (
                                    db.query(JobSettings.paused)
                                    .filter(JobSettings.owner_id == rec.owner_id, JobSettings.job_id == jp[0])
                                    .first()
                                )
                                is_paused = bool(paused_row[0]) if paused_row else False
                                if not is_paused:
                                    job_queues[jp[0]].append(eid)
                finally:
                    db.close()
                retry_list.clear()

            if not job_queues:
                break  # All done

            # Round-robin: build work batch
            job_ids = list(job_queues.keys())
            work_batch: list[str] = []
            max_batch = concurrency * 2

            while job_ids and len(work_batch) < max_batch:
                for jid in list(job_ids):
                    if job_queues[jid]:
                        work_batch.append(job_queues[jid].pop(0))
                    if not job_queues[jid]:
                        job_ids.remove(jid)
                    if len(work_batch) >= max_batch:
                        break

            if not work_batch:
                break

            # Process batch with thread pool
            futures = {executor.submit(_enrich_one_sync, eid): eid for eid in work_batch}
            for future in concurrent.futures.as_completed(futures):
                try:
                    r = future.result()
                    stats["total_processed"] += 1
                    status = r.get("status", "")
                    if status.startswith("completed"):
                        stats["completed"] += 1
                        stats["phones_found"] += r.get("phones", 0)
                    elif r.get("retry"):
                        stats["retried"] += 1
                        retry_list.append(r["enrichment_id"])
                    elif status.startswith("failed"):
                        stats["failed"] += 1
                except Exception:
                    stats["failed"] += 1

            # Backoff before next batch if we had retries
            if retry_list:
                import time
                time.sleep(backoff_s)

    return stats
