from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import (
    EnrichmentStatus,
    Job,
    JobProfile,
    JobSettings,
    OwnerSettings,
    Profile,
    ProfileEnrichment,
    SmsBatch,
    SmsBatchStatus,
    SmsInboundMessage,
    SmsInboundTag,
    SmsMessageStatus,
    SmsOptOut,
    SmsOutboundMessage,
)
from app.db.session import get_db
from app.services.outreach import (
    is_in_cooldown,
    is_opted_out,
    render_sms_template,
    select_best_phone_for_profile,
)
from app.services.phones import normalize_us_phone_e164, tag_inbound_heuristic
from app.worker.tasks import send_sms_batch


router = APIRouter()


DEFAULT_TEMPLATE = "Hi {first_name} — are you open to {job_name} roles in {job_location}? - {recruiter_company}"


# ------------------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------------------


class OwnerSettingsOut(BaseModel):
    recruiter_company: Optional[str] = None
    twilio_from_number: Optional[str] = None
    sms_global_daily_limit: int
    sms_business_start_hour: int
    sms_business_end_hour: int


class OwnerSettingsIn(BaseModel):
    recruiter_company: Optional[str] = None
    twilio_from_number: Optional[str] = None
    sms_global_daily_limit: Optional[int] = None
    sms_business_start_hour: Optional[int] = None
    sms_business_end_hour: Optional[int] = None


class JobSmsSettingsOut(BaseModel):
    job_id: str
    job_location_label: Optional[str] = None
    sms_template_text: Optional[str] = None
    sms_daily_limit: int


class JobSmsSettingsIn(BaseModel):
    job_location_label: Optional[str] = None
    sms_template_text: Optional[str] = None
    sms_daily_limit: Optional[int] = None


class CreateBatchIn(BaseModel):
    job_id: str
    requested_count: int = 100


class CreateBatchOut(BaseModel):
    batch_id: str
    requested_count: int
    created_count: int
    skipped_count: int


class ApproveBatchOut(BaseModel):
    batch_id: str
    approved: bool


class OutboundMessageOut(BaseModel):
    id: str
    job_id: str
    batch_id: Optional[str]
    profile_id: Optional[str]
    to_phone_e164: str
    from_phone_e164: str
    body: str
    status: SmsMessageStatus
    created_at: datetime
    sent_at: Optional[datetime]
    error: Optional[str]


class BatchOut(BaseModel):
    id: str
    job_id: str
    status: SmsBatchStatus
    requested_count: int
    created_count: int
    skipped_count: int
    created_at: datetime
    approved_at: Optional[datetime]
    completed_at: Optional[datetime]


class InboundMessageOut(BaseModel):
    id: str
    job_id: Optional[str]
    from_phone_e164: str
    to_phone_e164: str
    body: str
    tag: SmsInboundTag
    received_at: datetime


class ConversationSummaryOut(BaseModel):
    peer_phone_e164: str
    profile_name: Optional[str] = None
    last_message_body: str
    last_message_at: datetime
    last_tag: SmsInboundTag = SmsInboundTag.unknown
    job_id: Optional[str] = None
    job_name: Optional[str] = None


class ConversationMessageOut(BaseModel):
    direction: str  # "inbound" | "outbound"
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    body: str
    tag: Optional[SmsInboundTag] = None
    status: Optional[SmsMessageStatus] = None
    at: datetime


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def _get_or_create_owner_settings(db: Session, owner_id: str) -> OwnerSettings:
    row = db.query(OwnerSettings).filter(OwnerSettings.owner_id == owner_id).first()
    if row:
        return row
    row = OwnerSettings(
        owner_id=owner_id,
        recruiter_company=None,
        twilio_from_number=(settings.TWILIO_FROM_NUMBER or None),
        sms_global_daily_limit=int(settings.SMS_GLOBAL_DAILY_LIMIT or 200),
        sms_business_start_hour=7,
        sms_business_end_hour=19,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _get_or_create_job_sms_settings(db: Session, owner_id: str, job_id: str) -> JobSettings:
    row = db.query(JobSettings).filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id).first()
    if row:
        # ensure defaults exist
        if row.sms_daily_limit is None:
            row.sms_daily_limit = int(settings.SMS_JOB_DAILY_LIMIT or 50)
            db.add(row)
            db.commit()
            db.refresh(row)
        return row
    row = JobSettings(
        owner_id=owner_id,
        job_id=job_id,
        auto_enrich_enabled=False,
        auto_enrich_provider="disabled",
        job_location_label=None,
        sms_template_text=DEFAULT_TEMPLATE,
        sms_daily_limit=int(settings.SMS_JOB_DAILY_LIMIT or 50),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


# ------------------------------------------------------------------------------
# Settings endpoints
# ------------------------------------------------------------------------------


@router.get("/settings/owner", response_model=OwnerSettingsOut)
def get_owner_settings(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    row = _get_or_create_owner_settings(db, owner_id)
    return OwnerSettingsOut(
        recruiter_company=row.recruiter_company,
        twilio_from_number=row.twilio_from_number,
        sms_global_daily_limit=row.sms_global_daily_limit,
        sms_business_start_hour=row.sms_business_start_hour,
        sms_business_end_hour=row.sms_business_end_hour,
    )


@router.post("/settings/owner", response_model=OwnerSettingsOut)
def update_owner_settings(
    payload: OwnerSettingsIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    row = _get_or_create_owner_settings(db, owner_id)
    if payload.recruiter_company is not None:
        row.recruiter_company = payload.recruiter_company
    if payload.twilio_from_number is not None:
        row.twilio_from_number = payload.twilio_from_number
    if payload.sms_global_daily_limit is not None:
        row.sms_global_daily_limit = max(int(payload.sms_global_daily_limit), 0)
    if payload.sms_business_start_hour is not None:
        row.sms_business_start_hour = max(min(int(payload.sms_business_start_hour), 23), 0)
    if payload.sms_business_end_hour is not None:
        row.sms_business_end_hour = max(min(int(payload.sms_business_end_hour), 23), 0)
    db.add(row)
    db.commit()
    db.refresh(row)
    return OwnerSettingsOut(
        recruiter_company=row.recruiter_company,
        twilio_from_number=row.twilio_from_number,
        sms_global_daily_limit=row.sms_global_daily_limit,
        sms_business_start_hour=row.sms_business_start_hour,
        sms_business_end_hour=row.sms_business_end_hour,
    )


@router.get("/settings/job/{job_id}", response_model=JobSmsSettingsOut)
def get_job_sms_settings(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")
    row = _get_or_create_job_sms_settings(db, owner_id, job_id)
    return JobSmsSettingsOut(
        job_id=job_id,
        job_location_label=row.job_location_label,
        sms_template_text=row.sms_template_text,
        sms_daily_limit=row.sms_daily_limit,
    )


@router.post("/settings/job/{job_id}", response_model=JobSmsSettingsOut)
def update_job_sms_settings(
    job_id: str,
    payload: JobSmsSettingsIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")
    row = _get_or_create_job_sms_settings(db, owner_id, job_id)
    if payload.job_location_label is not None:
        row.job_location_label = payload.job_location_label
    if payload.sms_template_text is not None:
        row.sms_template_text = payload.sms_template_text
    if payload.sms_daily_limit is not None:
        row.sms_daily_limit = max(int(payload.sms_daily_limit), 0)
    db.add(row)
    db.commit()
    db.refresh(row)
    return JobSmsSettingsOut(
        job_id=job_id,
        job_location_label=row.job_location_label,
        sms_template_text=row.sms_template_text,
        sms_daily_limit=row.sms_daily_limit,
    )


# ------------------------------------------------------------------------------
# Batch endpoints
# ------------------------------------------------------------------------------


@router.post("/batches/create", response_model=CreateBatchOut)
def create_batch(
    payload: CreateBatchIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    job = db.get(Job, payload.job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    requested = max(int(payload.requested_count or 0), 0)
    if requested == 0:
        raise HTTPException(status_code=400, detail="requested_count must be > 0")

    owner_settings = _get_or_create_owner_settings(db, owner_id)
    job_settings = _get_or_create_job_sms_settings(db, owner_id, payload.job_id)

    # Safety: never create more than the configured daily limits in a single batch.
    requested = min(
        requested,
        max(int(owner_settings.sms_global_daily_limit or 0), 0) or requested,
        max(int(job_settings.sms_daily_limit or 0), 0) or requested,
    )
    if requested == 0:
        raise HTTPException(status_code=400, detail="Daily limit is 0 (owner or job)")

    from_number = (owner_settings.twilio_from_number or settings.TWILIO_FROM_NUMBER or "").strip()
    if not from_number:
        raise HTTPException(status_code=400, detail="Missing Twilio from number (set in owner settings)")

    recruiter_company = (owner_settings.recruiter_company or "").strip()
    if not recruiter_company:
        raise HTTPException(status_code=400, detail="Missing recruiter_company (set in owner settings)")

    template_text = (job_settings.sms_template_text or DEFAULT_TEMPLATE).strip()
    job_location_label = (job_settings.job_location_label or job.name).strip()

    now = datetime.utcnow()

    batch = SmsBatch(owner_id=owner_id, job_id=payload.job_id, status=SmsBatchStatus.queued, requested_count=requested)
    db.add(batch)
    db.commit()
    db.refresh(batch)

    # Pull all job profile_ids
    profile_ids = [
        pid
        for (pid,) in (
            db.query(JobProfile.profile_id)
            .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == payload.job_id)
            .order_by(JobProfile.created_at.desc())
            .all()
        )
    ]
    if not profile_ids:
        batch.status = SmsBatchStatus.completed
        batch.created_count = 0
        batch.skipped_count = requested
        batch.completed_at = datetime.utcnow()
        db.add(batch)
        db.commit()
        return CreateBatchOut(batch_id=batch.id, requested_count=requested, created_count=0, skipped_count=requested)

    # Fetch profiles + enrichments for those profiles
    profiles = (
        db.query(Profile)
        .filter(Profile.owner_id == owner_id, Profile.id.in_(profile_ids))
        .all()
    )
    p_by_id = {p.id: p for p in profiles}

    enrichments = (
        db.query(ProfileEnrichment)
        .filter(
            ProfileEnrichment.owner_id == owner_id,
            ProfileEnrichment.profile_id.in_(profile_ids),
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        )
        .all()
    )
    enrich_by_profile: dict[str, list[ProfileEnrichment]] = {}
    for e in enrichments:
        enrich_by_profile.setdefault(e.profile_id, []).append(e)

    created = 0
    skipped = 0
    used_phones: set[str] = set()

    for pid in profile_ids:
        if created >= requested:
            break

        prof = p_by_id.get(pid)
        if not prof:
            skipped += 1
            continue

        if not (prof.timezone or "").strip():
            skipped += 1
            continue

        sel = select_best_phone_for_profile(enrich_by_profile.get(pid) or [])
        if not sel:
            skipped += 1
            continue

        phone = normalize_us_phone_e164(sel.phone_e164)
        if not phone:
            skipped += 1
            continue

        if phone in used_phones:
            skipped += 1
            continue
        used_phones.add(phone)

        if is_opted_out(db, owner_id=owner_id, phone_e164=phone):
            skipped += 1
            continue

        if is_in_cooldown(db, owner_id=owner_id, phone_e164=phone, now=now):
            skipped += 1
            continue

        body = render_sms_template(
            template_text,
            first_name=(prof.first_name or "").strip(),
            job_name=(job.name or "").strip(),
            job_location=job_location_label,
            recruiter_company=recruiter_company,
        )
        if not body:
            skipped += 1
            continue

        msg = SmsOutboundMessage(
            owner_id=owner_id,
            job_id=job.id,
            batch_id=batch.id,
            profile_id=prof.id,
            to_phone_e164=phone,
            from_phone_e164=from_number,
            body=body,
            template_text=template_text,
            placeholders_json={
                "first_name": (prof.first_name or "").strip(),
                "job_name": (job.name or "").strip(),
                "job_location": job_location_label,
                "recruiter_company": recruiter_company,
                "phone_provider": sel.provider,
                "phone_raw": sel.raw,
            },
            status=SmsMessageStatus.queued,
        )
        db.add(msg)
        created += 1

    batch.created_count = created
    batch.skipped_count = skipped
    if created == 0:
        batch.status = SmsBatchStatus.completed
        batch.completed_at = datetime.utcnow()
    db.add(batch)
    db.commit()

    return CreateBatchOut(batch_id=batch.id, requested_count=requested, created_count=created, skipped_count=skipped)


@router.post("/batches/{batch_id}/cancel", response_model=ApproveBatchOut)
def cancel_batch(
    batch_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Cancel a batch and delete unsent outbound messages.
    Returns {batch_id, approved:false} to reuse existing response shape.
    """
    batch = db.get(SmsBatch, batch_id)
    if not batch or batch.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Only allow cancel when not completed.
    if batch.status in (SmsBatchStatus.completed, SmsBatchStatus.cancelled):
        return ApproveBatchOut(batch_id=batch.id, approved=False)

    # Delete anything not sent.
    db.query(SmsOutboundMessage).filter(
        SmsOutboundMessage.owner_id == owner_id,
        SmsOutboundMessage.batch_id == batch_id,
        SmsOutboundMessage.status.in_(
            [SmsMessageStatus.queued, SmsMessageStatus.approved, SmsMessageStatus.sending, SmsMessageStatus.failed]
        ),
    ).delete(synchronize_session=False)

    batch.status = SmsBatchStatus.cancelled
    batch.completed_at = datetime.utcnow()
    db.add(batch)
    db.commit()
    return ApproveBatchOut(batch_id=batch.id, approved=False)


@router.get("/batches/job/{job_id}", response_model=List[BatchOut])
def list_job_batches(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")
    rows = (
        db.query(SmsBatch)
        .filter(SmsBatch.owner_id == owner_id, SmsBatch.job_id == job_id)
        .order_by(SmsBatch.created_at.desc())
        .limit(50)
        .all()
    )
    return [
        BatchOut(
            id=b.id,
            job_id=b.job_id,
            status=b.status,
            requested_count=b.requested_count,
            created_count=b.created_count,
            skipped_count=b.skipped_count,
            created_at=b.created_at,
            approved_at=b.approved_at,
            completed_at=b.completed_at,
        )
        for b in rows
    ]


@router.get("/batches/{batch_id}/messages", response_model=List[OutboundMessageOut])
def list_batch_messages(
    batch_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(SmsOutboundMessage)
        .filter(SmsOutboundMessage.owner_id == owner_id, SmsOutboundMessage.batch_id == batch_id)
        .order_by(SmsOutboundMessage.created_at.asc())
        .limit(1000)
        .all()
    )
    return [
        OutboundMessageOut(
            id=m.id,
            job_id=m.job_id,
            batch_id=m.batch_id,
            profile_id=m.profile_id,
            to_phone_e164=m.to_phone_e164,
            from_phone_e164=m.from_phone_e164,
            body=m.body,
            status=m.status,
            created_at=m.created_at,
            sent_at=m.sent_at,
            error=m.error,
        )
        for m in rows
    ]


@router.post("/batches/{batch_id}/approve", response_model=ApproveBatchOut)
def approve_batch(
    batch_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    batch = db.get(SmsBatch, batch_id)
    if not batch or batch.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Batch not found")
    if batch.status != SmsBatchStatus.queued:
        return ApproveBatchOut(batch_id=batch.id, approved=False)

    now = datetime.utcnow()
    batch.status = SmsBatchStatus.approved
    batch.approved_at = now
    db.add(batch)

    db.query(SmsOutboundMessage).filter(
        SmsOutboundMessage.owner_id == owner_id,
        SmsOutboundMessage.batch_id == batch_id,
        SmsOutboundMessage.status == SmsMessageStatus.queued,
    ).update(
        {SmsOutboundMessage.status: SmsMessageStatus.approved, SmsOutboundMessage.approved_at: now},
        synchronize_session=False,
    )
    db.commit()

    # Kick off background sending
    try:
        send_sms_batch.delay(batch_id)
    except Exception as e:
        # Don't fail the approval flow, but do not swallow this silently.
        # (Common causes: broker down, misconfig, import issues)
        logger = logging.getLogger(__name__)
        logger.exception("Failed to enqueue send_sms_batch for batch_id=%s: %s", batch_id, e)

    return ApproveBatchOut(batch_id=batch.id, approved=True)


# ------------------------------------------------------------------------------
# Inbox
# ------------------------------------------------------------------------------


@router.get("/inbox", response_model=List[InboundMessageOut])
def list_inbox(
    job_id: Optional[str] = None,
    limit: int = 200,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    limit = min(max(int(limit or 0), 1), 500)
    q = db.query(SmsInboundMessage).filter(SmsInboundMessage.owner_id == owner_id)
    if job_id:
        q = q.filter(SmsInboundMessage.job_id == job_id)
    rows = q.order_by(SmsInboundMessage.received_at.desc()).limit(limit).all()
    return [
        InboundMessageOut(
            id=m.id,
            job_id=m.job_id,
            from_phone_e164=m.from_phone_e164,
            to_phone_e164=m.to_phone_e164,
            body=m.body,
            tag=m.tag,
            received_at=m.received_at,
        )
        for m in rows
    ]


@router.get("/conversations", response_model=List[ConversationSummaryOut])
def list_conversations(
    limit: int = 200,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Conversation-first inbox.
    v1 definition: any inbound message from a phone creates the conversation.
    """
    limit = min(max(int(limit or 0), 1), 500)

    latest = (
        db.query(
            SmsInboundMessage.from_phone_e164.label("phone"),
            func.max(SmsInboundMessage.received_at).label("last_at"),
        )
        .filter(SmsInboundMessage.owner_id == owner_id)
        .group_by(SmsInboundMessage.from_phone_e164)
        .subquery()
    )

    rows: list[SmsInboundMessage] = (
        db.query(SmsInboundMessage)
        .join(
            latest,
            (SmsInboundMessage.from_phone_e164 == latest.c.phone)
            & (SmsInboundMessage.received_at == latest.c.last_at),
        )
        .filter(SmsInboundMessage.owner_id == owner_id)
        .order_by(SmsInboundMessage.received_at.desc())
        .limit(limit)
        .all()
    )

    job_ids = [r.job_id for r in rows if r.job_id]
    jobs = (
        db.query(Job.id, Job.name)
        .filter(Job.owner_id == owner_id, Job.id.in_(job_ids))  # type: ignore[arg-type]
        .all()
        if job_ids
        else []
    )
    job_name_by_id = {jid: jname for (jid, jname) in jobs}

    # Best-effort: show a profile name from the most recent outbound message to this phone.
    phone_list = [r.from_phone_e164 for r in rows]
    profile_name_by_phone: dict[str, Optional[str]] = {}
    if phone_list:
        out_latest = (
            db.query(
                SmsOutboundMessage.to_phone_e164.label("phone"),
                func.max(SmsOutboundMessage.created_at).label("last_at"),
            )
            .filter(SmsOutboundMessage.owner_id == owner_id, SmsOutboundMessage.to_phone_e164.in_(phone_list))
            .group_by(SmsOutboundMessage.to_phone_e164)
            .subquery()
        )
        out_rows = (
            db.query(SmsOutboundMessage.to_phone_e164, SmsOutboundMessage.profile_id)
            .join(
                out_latest,
                (SmsOutboundMessage.to_phone_e164 == out_latest.c.phone)
                & (SmsOutboundMessage.created_at == out_latest.c.last_at),
            )
            .filter(SmsOutboundMessage.owner_id == owner_id)
            .all()
        )
        prof_ids = [pid for (_, pid) in out_rows if pid]
        prof_rows = (
            db.query(Profile.id, Profile.first_name, Profile.last_name)
            .filter(Profile.owner_id == owner_id, Profile.id.in_(prof_ids))  # type: ignore[arg-type]
            .all()
            if prof_ids
            else []
        )
        prof_name_by_id = {
            pid: " ".join([p for p in [first, last] if (p or "").strip()]).strip() or None
            for (pid, first, last) in prof_rows
        }
        for phone, pid in out_rows:
            profile_name_by_phone[phone] = prof_name_by_id.get(pid) if pid else None

    return [
        ConversationSummaryOut(
            peer_phone_e164=r.from_phone_e164,
            profile_name=profile_name_by_phone.get(r.from_phone_e164),
            last_message_body=r.body,
            last_message_at=r.received_at,
            last_tag=r.tag,
            job_id=r.job_id,
            job_name=job_name_by_id.get(r.job_id) if r.job_id else None,
        )
        for r in rows
    ]


@router.get("/conversations/{peer_phone_e164}/messages", response_model=List[ConversationMessageOut])
def get_conversation_messages(
    peer_phone_e164: str,
    limit: int = 200,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    limit = min(max(int(limit or 0), 1), 500)
    peer = normalize_us_phone_e164(peer_phone_e164) or peer_phone_e164

    inbound: list[SmsInboundMessage] = (
        db.query(SmsInboundMessage)
        .filter(SmsInboundMessage.owner_id == owner_id, SmsInboundMessage.from_phone_e164 == peer)
        .order_by(SmsInboundMessage.received_at.desc())
        .limit(limit)
        .all()
    )
    outbound: list[SmsOutboundMessage] = (
        db.query(SmsOutboundMessage)
        .filter(SmsOutboundMessage.owner_id == owner_id, SmsOutboundMessage.to_phone_e164 == peer)
        .order_by(SmsOutboundMessage.created_at.desc())
        .limit(limit)
        .all()
    )

    job_ids = list({m.job_id for m in inbound if m.job_id} | {m.job_id for m in outbound if m.job_id})
    jobs = (
        db.query(Job.id, Job.name)
        .filter(Job.owner_id == owner_id, Job.id.in_(job_ids))  # type: ignore[arg-type]
        .all()
        if job_ids
        else []
    )
    job_name_by_id = {jid: jname for (jid, jname) in jobs}

    merged: list[ConversationMessageOut] = []
    for m in inbound:
        merged.append(
            ConversationMessageOut(
                direction="inbound",
                job_id=m.job_id,
                job_name=job_name_by_id.get(m.job_id) if m.job_id else None,
                body=m.body,
                tag=m.tag,
                status=None,
                at=m.received_at,
            )
        )
    for m in outbound:
        merged.append(
            ConversationMessageOut(
                direction="outbound",
                job_id=m.job_id,
                job_name=job_name_by_id.get(m.job_id) if m.job_id else None,
                body=m.body,
                tag=None,
                status=m.status,
                at=(m.sent_at or m.created_at),
            )
        )

    merged.sort(key=lambda x: x.at)
    return merged[-limit:]


# ------------------------------------------------------------------------------
# Twilio webhook
# ------------------------------------------------------------------------------


@router.post("/twilio/webhook")
async def twilio_inbound_webhook(
    request: Request,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Twilio inbound SMS webhook.
    Stores inbound message, applies minimal tagging, and auto-opt-outs on STOP keywords.
    """
    form = await request.form()
    from_raw = str(form.get("From") or "").strip()
    to_raw = str(form.get("To") or "").strip()
    body = str(form.get("Body") or "").strip()
    sid = str(form.get("MessageSid") or "").strip()

    from_phone = normalize_us_phone_e164(from_raw) or from_raw
    to_phone = normalize_us_phone_e164(to_raw) or to_raw

    tag_str = tag_inbound_heuristic(body)
    try:
        tag = SmsInboundTag(tag_str)  # type: ignore[arg-type]
    except Exception:
        tag = SmsInboundTag.unknown

    # Try to map inbound to a job via most recent outbound to this phone.
    job_id = None
    try:
        last = (
            db.query(SmsOutboundMessage)
            .filter(
                SmsOutboundMessage.owner_id == owner_id,
                SmsOutboundMessage.to_phone_e164 == from_phone,
            )
            .order_by(SmsOutboundMessage.sent_at.desc().nullslast(), SmsOutboundMessage.created_at.desc())
            .first()
        )
        if last:
            job_id = last.job_id
    except Exception:
        job_id = None

    # Upsert opt-out if needed
    if tag == SmsInboundTag.unsubscribe:
        rec = db.query(SmsOptOut).filter(SmsOptOut.owner_id == owner_id, SmsOptOut.phone_e164 == from_phone).first()
        if not rec:
            rec = SmsOptOut(owner_id=owner_id, phone_e164=from_phone, reason="STOP", revoked_at=None)
        else:
            rec.revoked_at = None
            rec.reason = "STOP"
        db.add(rec)

    msg = SmsInboundMessage(
        owner_id=owner_id,
        job_id=job_id,
        from_phone_e164=from_phone,
        to_phone_e164=to_phone,
        body=body,
        twilio_sid=sid or f"no-sid-{datetime.utcnow().isoformat()}",
        tag=tag,
        raw_json={k: str(v) for k, v in form.items()},
    )
    db.add(msg)
    try:
        db.commit()
    except Exception:
        db.rollback()

    # Twilio expects 200 OK. We don't need to respond with TwiML for inbound storage.
    return {"ok": True}

