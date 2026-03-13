from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import EnrichmentStatus, Job, JobProfile, JobSettings, Profile, ProfileEnrichment
from app.db.session import get_db
from app.worker.tasks import queue_chain_for_profiles, run_enrichment_dispatcher


router = APIRouter()

_SCRAPEDO_ABC_PROVIDER_VERSION = 6
_SCRAPEDO_TPS_PROVIDER_VERSION = 3
_SCRAPEDO_USPHONEBOOK_PROVIDER_VERSION = 1

# Public UI-only identifiers -> internal provider names stored in DB.
_SOURCE_TO_PROVIDER = {
    "source1": "scrapedo_advancedbackgroundchecks",
    "source2": "scrapedo_tps",
    "source3": "scrapedo_usphonebook",
}
_CHAIN_PUBLIC_SOURCE = "chain"
_CHAIN_PROVIDERS = list(_SOURCE_TO_PROVIDER.values())


def _normalize_source_and_provider(source: Optional[str], provider: Optional[str]) -> tuple[str, str]:
    """
    Keep provider/vendor names off the wire:
    - UI should send `source=source1|source2|source3|chain`.
    - We still accept legacy `provider=` for backward compatibility.
    Returns (public_source, internal_provider).
    """
    src = (source or "").strip().lower()
    prov = (provider or "").strip().lower()

    if src in _SOURCE_TO_PROVIDER:
        return src, _SOURCE_TO_PROVIDER[src]
    if src == _CHAIN_PUBLIC_SOURCE:
        return _CHAIN_PUBLIC_SOURCE, "chain"

    # Legacy: accept internal provider string and map it to a public source (if known).
    for s, p in _SOURCE_TO_PROVIDER.items():
        if prov == p:
            return s, p
    if prov == "chain":
        return _CHAIN_PUBLIC_SOURCE, "chain"

    # Unknown/disabled falls through.
    if prov:
        return "source1", prov  # placeholder source; caller will be rejected if unsupported
    return "source1", "disabled"


class EnrichJobIn(BaseModel):
    job_id: str
    # Prefer `source` (public). `provider` is legacy and should not be used by the UI.
    source: Optional[str] = None
    provider: Optional[str] = None


class EnrichJobOut(BaseModel):
    source: str
    queued: int
    skipped_in_flight: int
    skipped_done: int


class EnrichmentSummaryOut(BaseModel):
    source: str
    total_profiles: int
    total_records: int
    queued: int
    running: int
    completed: int
    failed: int
    with_phone: int
    last_error: Optional[str] = None
    updated_at: datetime


def _sanitize_public_error(err: Optional[str]) -> Optional[str]:
    """
    Never leak infra/vendor details (e.g. "scrape.do", "SCRAPEDO_API_KEY") to the UI.
    Keep the message useful but generic.
    """
    if not err:
        return None
    s = str(err).strip()
    if not s:
        return None

    low = s.lower()
    # Common provider failures that currently include the proxy/vendor name.
    if "auth_failed" in low or "api_key" in low:
        return "Provider auth failed. Check the provider API key."
    if "rate_limited" in low or "http_429" in low:
        return "Provider rate limited. Try again shortly."

    # Generic scrub: remove any mention of scrape.do / scrapedo and sensitive env var names.
    s = re.sub(r"scrape\.do", "provider", s, flags=re.IGNORECASE)
    s = re.sub(r"scrapedo", "provider", s, flags=re.IGNORECASE)
    s = re.sub(r"SCRAPEDO_API_KEY", "PROVIDER_API_KEY", s, flags=re.IGNORECASE)
    return s


@router.post("/enrich-job", response_model=EnrichJobOut)
def enrich_job(
    payload: EnrichJobIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Queue phone enrichment for ALL profiles in a job (no limit).
    Triggers the round-robin dispatcher which processes all queued enrichments
    across all jobs with fair scheduling and 25 concurrent requests.
    """
    # Enrichment is a paid-only feature.
    from app.core.limits import check_paid_feature
    from app.db.models import User
    user = db.query(User).filter(User.id == owner_id).first()
    if user:
        check_paid_feature(user)

    public_source, provider = _normalize_source_and_provider(payload.source, payload.provider)

    if public_source == _CHAIN_PUBLIC_SOURCE or provider == "chain":
        provider = "chain"
    else:
        provider = (provider or settings.ENRICH_PROVIDER or "disabled").strip().lower() or "disabled"

    if provider == "disabled":
        raise HTTPException(
            status_code=400,
            detail="Enrichment disabled. Choose a provider or set ENRICH_PROVIDER and provider credentials in your environment.",
        )

    # Guardrail: only allow providers we explicitly support (or intend to support).
    allowed = {
        "scrapedo_advancedbackgroundchecks",
        "scrapedo_tps",
        "scrapedo_usphonebook",
        "enformiongo",
    }
    if provider not in allowed and provider != "chain":
        raise HTTPException(status_code=400, detail="Unsupported provider")
    if provider == "enformiongo":
        raise HTTPException(status_code=400, detail="Provider not implemented yet")

    job = db.get(Job, payload.job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Persist job-level auto-enrichment settings (no new UI toggle: Find phones = enable).
    # Provider selection here becomes the job's default provider going forward.
    settings_row = (
        db.query(JobSettings)
        .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job.id)
        .first()
    )
    if not settings_row:
        settings_row = JobSettings(
            owner_id=owner_id,
            job_id=job.id,
            auto_enrich_enabled=True,
            auto_enrich_provider=provider,
        )
    else:
        settings_row.auto_enrich_enabled = True
        settings_row.auto_enrich_provider = provider
    db.add(settings_row)
    db.commit()

    if provider == "chain":
        profile_ids = [
            pid
            for (pid,) in (
                db.query(JobProfile.profile_id)
                .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job.id)
                .order_by(JobProfile.created_at.desc())
                .all()
            )
        ]
        stats = queue_chain_for_profiles(db, owner_id=owner_id, profile_ids=profile_ids)
        if stats.get("queued"):
            try:
                run_enrichment_dispatcher.delay()
            except Exception:
                pass
        return EnrichJobOut(
            source=_CHAIN_PUBLIC_SOURCE,
            queued=int(stats.get("queued") or 0),
            skipped_in_flight=int(stats.get("skipped_in_flight") or 0),
            skipped_done=int(stats.get("skipped_has_phones") or 0),
        )

    skipped_in_flight = 0
    skipped_done = 0
    queued_records: list[ProfileEnrichment] = []

    # Fetch ALL profile_ids for this job (no limit)
    profile_ids = [
        pid
        for (pid,) in (
            db.query(JobProfile.profile_id)
            .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job.id)
            .order_by(JobProfile.created_at.desc())
            .all()
        )
    ]

    for profile_id in profile_ids:
        rec = (
            db.query(ProfileEnrichment)
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.profile_id == profile_id,
                ProfileEnrichment.provider == provider,
            )
            .first()
        )

        if rec:
            if rec.status in (EnrichmentStatus.queued, EnrichmentStatus.running):
                skipped_in_flight += 1
                continue
            if rec.status == EnrichmentStatus.completed:
                # If provider logic changes, allow re-queue based on provider_version stored in raw_json.
                expected_version = None
                if provider == "scrapedo_advancedbackgroundchecks":
                    expected_version = _SCRAPEDO_ABC_PROVIDER_VERSION
                elif provider == "scrapedo_tps":
                    expected_version = _SCRAPEDO_TPS_PROVIDER_VERSION
                elif provider == "scrapedo_usphonebook":
                    expected_version = _SCRAPEDO_USPHONEBOOK_PROVIDER_VERSION

                current_version = 0
                try:
                    current_version = int((rec.raw_json or {}).get("provider_version") or 0)
                except Exception:
                    current_version = 0

                if expected_version is None or current_version == expected_version:
                    skipped_done += 1
                    continue

            # Re-queue (failed or outdated provider version)
            rec.status = EnrichmentStatus.queued
            rec.started_at = None
            rec.finished_at = None
            rec.last_error = None
            rec.raw_json = None
            rec.phone_numbers = None
            db.add(rec)
            queued_records.append(rec)
        else:
            rec = ProfileEnrichment(
                owner_id=owner_id,
                profile_id=profile_id,
                provider=provider,
                status=EnrichmentStatus.queued,
            )
            db.add(rec)
            queued_records.append(rec)

    if queued_records:
        db.commit()

    # Count existing queued enrichments for this job
    existing_queued = (
        db.query(func.count(ProfileEnrichment.id))
        .join(
            JobProfile,
            (JobProfile.profile_id == ProfileEnrichment.profile_id)
            & (JobProfile.owner_id == ProfileEnrichment.owner_id),
        )
        .filter(
            ProfileEnrichment.owner_id == owner_id,
            ProfileEnrichment.provider == provider,
            ProfileEnrichment.status == EnrichmentStatus.queued,
            JobProfile.job_id == payload.job_id,
        )
        .scalar()
        or 0
    )

    # Trigger round-robin dispatcher if there are any queued enrichments
    if queued_records or existing_queued > 0:
        try:
            run_enrichment_dispatcher.delay()
        except Exception as e:
            # Mark newly added as failed if we can't start the dispatcher
            for rec in queued_records:
                rec.status = EnrichmentStatus.failed
                rec.last_error = f"dispatcher_enqueue_failed: {e}"
                rec.finished_at = datetime.utcnow()
                db.add(rec)
            db.commit()

    return EnrichJobOut(
        source=public_source,
        queued=len(queued_records),
        skipped_in_flight=skipped_in_flight,
        skipped_done=skipped_done,
    )


@router.get("/job/{job_id}/summary", response_model=EnrichmentSummaryOut)
def enrichment_summary(
    job_id: str,
    provider: Optional[str] = None,
    source: Optional[str] = None,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    public_source, provider_name = _normalize_source_and_provider(source, provider)
    provider_name = (provider_name or settings.ENRICH_PROVIDER or "disabled").strip().lower() or "disabled"

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    total_profiles = (
        db.query(func.count(JobProfile.id))
        .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id)
        .scalar()
        or 0
    )

    if public_source == _CHAIN_PUBLIC_SOURCE or provider_name == "chain":
        provider_names = _CHAIN_PROVIDERS
        base = (
            db.query(ProfileEnrichment)
            .join(
                JobProfile,
                (JobProfile.profile_id == ProfileEnrichment.profile_id)
                & (JobProfile.job_id == job_id)
                & (JobProfile.owner_id == owner_id),
            )
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.provider.in_(provider_names),
            )
        )

        total_records = base.count()
        queued = base.filter(ProfileEnrichment.status == EnrichmentStatus.queued).count()
        running = base.filter(ProfileEnrichment.status == EnrichmentStatus.running).count()
        completed = base.filter(ProfileEnrichment.status == EnrichmentStatus.completed).count()
        failed = base.filter(ProfileEnrichment.status == EnrichmentStatus.failed).count()

        with_phone = (
            db.query(func.count(func.distinct(ProfileEnrichment.profile_id)))
            .join(
                JobProfile,
                (JobProfile.profile_id == ProfileEnrichment.profile_id)
                & (JobProfile.job_id == job_id)
                & (JobProfile.owner_id == owner_id),
            )
            .filter(
                ProfileEnrichment.owner_id == owner_id,
                ProfileEnrichment.provider.in_(provider_names),
                ProfileEnrichment.status == EnrichmentStatus.completed,
                func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
            )
            .scalar()
            or 0
        )

        last_error = (
            base.filter(ProfileEnrichment.status == EnrichmentStatus.failed, ProfileEnrichment.last_error.isnot(None))
            .order_by(ProfileEnrichment.finished_at.desc().nullslast())
            .with_entities(ProfileEnrichment.last_error)
            .first()
        )

        return EnrichmentSummaryOut(
            source=_CHAIN_PUBLIC_SOURCE,
            total_profiles=total_profiles,
            total_records=total_records,
            queued=queued,
            running=running,
            completed=completed,
            failed=failed,
            with_phone=with_phone,
            last_error=_sanitize_public_error(last_error[0] if last_error else None),
            updated_at=datetime.utcnow(),
        )

    base = (
        db.query(ProfileEnrichment)
        .join(
            JobProfile,
            (JobProfile.profile_id == ProfileEnrichment.profile_id)
            & (JobProfile.job_id == job_id)
            & (JobProfile.owner_id == owner_id),
        )
        .filter(ProfileEnrichment.owner_id == owner_id, ProfileEnrichment.provider == provider_name)
    )

    total_records = base.count()
    queued = base.filter(ProfileEnrichment.status == EnrichmentStatus.queued).count()
    running = base.filter(ProfileEnrichment.status == EnrichmentStatus.running).count()
    completed = base.filter(ProfileEnrichment.status == EnrichmentStatus.completed).count()
    failed = base.filter(ProfileEnrichment.status == EnrichmentStatus.failed).count()

    with_phone = (
        base.filter(
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        ).count()
        if total_records
        else 0
    )

    last_error = (
        base.filter(ProfileEnrichment.status == EnrichmentStatus.failed, ProfileEnrichment.last_error.isnot(None))
        .order_by(ProfileEnrichment.finished_at.desc().nullslast())
        .with_entities(ProfileEnrichment.last_error)
        .first()
    )

    return EnrichmentSummaryOut(
        source=public_source,
        total_profiles=total_profiles,
        total_records=total_records,
        queued=queued,
        running=running,
        completed=completed,
        failed=failed,
        with_phone=with_phone,
        last_error=_sanitize_public_error(last_error[0] if last_error else None),
        updated_at=datetime.utcnow(),
    )


@router.get("/job/{job_id}/download")
def download_enriched_profiles(
    job_id: str,
    provider: Optional[str] = None,
    source: Optional[str] = None,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Download enriched profiles as CSV.
    Includes: name, linkedin_url, city, state, title, snippet, phone_numbers
    Only profiles with completed enrichment and at least one phone number.
    """
    _public_source, provider_name = _normalize_source_and_provider(source, provider)
    provider_name = (provider_name or settings.ENRICH_PROVIDER or "disabled").strip().lower() or "disabled"

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get profiles with completed enrichment and phones
    rows = (
        db.query(Profile, ProfileEnrichment)
        .join(
            JobProfile,
            (JobProfile.profile_id == Profile.id) & (JobProfile.owner_id == owner_id),
        )
        .join(
            ProfileEnrichment,
            (ProfileEnrichment.profile_id == Profile.id)
            & (ProfileEnrichment.owner_id == owner_id)
            & (ProfileEnrichment.provider == provider_name),
        )
        .filter(
            JobProfile.job_id == job_id,
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        )
        .all()
    )

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "linkedin_url", "city", "state", "title", "snippet", "phone_numbers"])

    for profile, enrichment in rows:
        cse = profile.cse_item_json or {}
        title = (cse.get("title") or "").replace("\n", " ").strip()
        snippet = (cse.get("snippet") or "").replace("\n", " ").strip()
        phones = enrichment.phone_numbers or []
        phones_str = ", ".join(phones)

        writer.writerow([
            profile.full_name_raw or "",
            profile.linkedin_url_raw or "",
            profile.city or "",
            profile.state or "",
            title,
            snippet,
            phones_str,
        ])

    output.seek(0)

    # Generate filename
    job_name_slug = "".join(c if c.isalnum() or c in " -_" else "" for c in (job.name or "job"))[:40].strip()
    filename = f"{job_name_slug}_enriched.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

