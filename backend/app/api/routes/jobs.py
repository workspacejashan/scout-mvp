import csv
import io
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.core.rate_limit import limiter
from app.db.models import (
    DroppedProfile,
    EnrichmentStatus,
    Job,
    JobChatMessage,
    JobProfile,
    JobProfilePin,
    JobSettings,
    JobStatus,
    LocationVariant,
    Profile,
    ProfileEnrichment,
    SmsBatch,
    SmsInboundMessage,
    SmsOutboundMessage,
    StrategyRun,
    StrategyRunStatus,
    TitleVariant,
    User,
)
from app.db.session import get_db
from app.services.normalize import clean_person_name, is_linkedin_in_url, normalize_linkedin_url, split_first_last
from app.worker import tasks as worker_tasks


router = APIRouter()


# ------------------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------------------


class VariantOut(BaseModel):
    id: str
    entities: List[str]
    boolean_text: str
    selected: bool


class JobListItem(BaseModel):
    id: str
    name: str
    profile_count: int
    phone_count: int
    paused: bool
    created_at: datetime


class JobOut(BaseModel):
    id: str
    name: str
    goal_text: str
    status: JobStatus
    profile_count: int
    phone_count: int
    paused: bool
    created_at: datetime
    title_variants: List[VariantOut]
    location_variants: List[VariantOut]


class ChatMessageOut(BaseModel):
    id: str
    role: str
    content: str
    created_at: datetime


class UploadProfilesOut(BaseModel):
    job_id: str
    apply_job_match: bool
    total_rows: int
    created_profiles: int
    existing_profiles: int
    updated_existing: int
    linked_to_job: int
    skipped_duplicates: int
    skipped_invalid: int
    skipped_not_matching: int
    errors: List[str]


class RepairExtractionOut(BaseModel):
    job_id: str
    queued: bool


class UploadNewJobOut(BaseModel):
    job_id: str
    job_name: str
    total_rows: int
    created_profiles: int
    existing_profiles: int
    linked_to_job: int
    skipped_duplicates: int
    skipped_invalid: int
    errors: List[str]


class JobProfileItemOut(BaseModel):
    id: str
    name: str
    linkedin_url: str
    city: str
    state: str
    title: str
    snippet: str
    source: str


class JobProfilesOut(BaseModel):
    job_id: str
    total: int
    offset: int
    limit: int
    profiles: List[JobProfileItemOut]


class PendingScoutingRunOut(BaseModel):
    id: str
    title_variant_id: str
    location_variant_id: str
    created_at: datetime


class PendingEnrichmentSourceCountOut(BaseModel):
    source: str
    queued: int


class PendingEnrichmentItemOut(BaseModel):
    enrichment_id: str
    profile_id: str
    name: str
    city: str
    state: str
    created_at: datetime


class JobPendingQueueOut(BaseModel):
    job_id: str
    paused: bool
    scouting_queued_count: int
    scouting_queued: List[PendingScoutingRunOut]
    enrichment_queued_count: int
    enrichment_queued_by_source: List[PendingEnrichmentSourceCountOut]
    enrichment_queued_sample: List[PendingEnrichmentItemOut]

# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------


@router.get("", response_model=List[JobListItem])
def list_jobs(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """List all active jobs for current owner."""

    # Get jobs with profile counts
    results = (
        db.query(
            Job,
            func.count(func.distinct(JobProfile.profile_id)).label("profile_count"),
            func.count(
                func.distinct(
                    case(
                        (
                            (ProfileEnrichment.status == EnrichmentStatus.completed)
                            & (
                                func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0)
                                > 0
                            ),
                            JobProfile.profile_id,
                        ),
                        else_=None,
                    )
                )
            ).label("phone_count"),
            func.coalesce(JobSettings.paused, False).label("paused"),
        )
        .outerjoin(JobProfile, (Job.id == JobProfile.job_id) & (JobProfile.owner_id == owner_id))
        .outerjoin(
            ProfileEnrichment,
            (ProfileEnrichment.profile_id == JobProfile.profile_id)
            & (ProfileEnrichment.owner_id == owner_id),
        )
        .outerjoin(JobSettings, (JobSettings.job_id == Job.id) & (JobSettings.owner_id == owner_id))
        .filter(Job.owner_id == owner_id, Job.status == JobStatus.active)
        .group_by(Job.id, JobSettings.paused)
        .order_by(Job.created_at.desc())
        .all()
    )

    return [
        JobListItem(
            id=job.id,
            name=job.name,
            profile_count=count,
            phone_count=phone_count,
            paused=bool(paused),
            created_at=job.created_at,
        )
        for job, count, phone_count, paused in results
    ]


@router.get("/{job_id}", response_model=JobOut)
def get_job(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Get job details including variants."""

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    settings_row = (
        db.query(JobSettings).filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id).first()
    )
    paused = bool(getattr(settings_row, "paused", False)) if settings_row else False

    profile_count = (
        db.query(JobProfile).filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id).count()
    )
    phone_count = (
        db.query(func.count(func.distinct(JobProfile.profile_id)))
        .join(
            ProfileEnrichment,
            (ProfileEnrichment.profile_id == JobProfile.profile_id)
            & (ProfileEnrichment.owner_id == owner_id),
        )
        .filter(
            JobProfile.job_id == job_id,
            JobProfile.owner_id == owner_id,
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        )
        .scalar()
        or 0
    )

    titles = (
        db.query(TitleVariant)
        .filter(TitleVariant.job_id == job_id, TitleVariant.owner_id == owner_id)
        .order_by(TitleVariant.created_at)
        .all()
    )
    locations = (
        db.query(LocationVariant)
        .filter(LocationVariant.job_id == job_id, LocationVariant.owner_id == owner_id)
        .order_by(LocationVariant.created_at)
        .all()
    )

    return JobOut(
        id=job.id,
        name=job.name,
        goal_text=job.goal_text,
        status=job.status,
        profile_count=profile_count,
        phone_count=phone_count,
        paused=paused,
        created_at=job.created_at,
        title_variants=[
            VariantOut(
                id=t.id,
                entities=t.entities,
                boolean_text=t.boolean_text,
                selected=t.selected,
            )
            for t in titles
        ],
        location_variants=[
            VariantOut(
                id=l.id,
                entities=l.entities,
                boolean_text=l.boolean_text,
                selected=l.selected,
            )
            for l in locations
        ],
    )


@router.get("/{job_id}/pending-queue", response_model=JobPendingQueueOut)
def get_job_pending_queue(
    job_id: str,
    scout_limit: int = 25,
    enrich_limit: int = 25,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Show what's pending (queued) for this job:
    - scouting runs that are queued
    - phone enrichments that are queued
    """

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    scout_limit = min(max(int(scout_limit or 0), 1), 200)
    enrich_limit = min(max(int(enrich_limit or 0), 1), 200)

    settings_row = (
        db.query(JobSettings).filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id).first()
    )
    paused = bool(getattr(settings_row, "paused", False)) if settings_row else False

    scouting_queued_count = (
        db.query(func.count(StrategyRun.id))
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job_id,
            StrategyRun.status == StrategyRunStatus.queued,
        )
        .scalar()
        or 0
    )
    scouting_queued_rows = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job_id,
            StrategyRun.status == StrategyRunStatus.queued,
        )
        .order_by(StrategyRun.created_at.asc())
        .limit(scout_limit)
        .all()
    )

    # Enrichment pending for this job (linked via JobProfile)
    enrichment_queued_count = (
        db.query(func.count(ProfileEnrichment.id))
        .join(
            JobProfile,
            (JobProfile.profile_id == ProfileEnrichment.profile_id)
            & (JobProfile.owner_id == ProfileEnrichment.owner_id),
        )
        .filter(
            JobProfile.owner_id == owner_id,
            JobProfile.job_id == job_id,
            ProfileEnrichment.status == EnrichmentStatus.queued,
        )
        .scalar()
        or 0
    )

    # Group queued enrichments by provider, but return only public "source" ids to the UI.
    try:
        from app.api.routes.enrichment import _SOURCE_TO_PROVIDER  # local import to avoid circulars

        provider_to_source = {str(v).strip().lower(): str(k).strip().lower() for k, v in _SOURCE_TO_PROVIDER.items()}
    except Exception:
        provider_to_source = {}

    by_provider_rows = (
        db.query(ProfileEnrichment.provider, func.count(ProfileEnrichment.id))
        .join(
            JobProfile,
            (JobProfile.profile_id == ProfileEnrichment.profile_id)
            & (JobProfile.owner_id == ProfileEnrichment.owner_id),
        )
        .filter(
            JobProfile.owner_id == owner_id,
            JobProfile.job_id == job_id,
            ProfileEnrichment.status == EnrichmentStatus.queued,
        )
        .group_by(ProfileEnrichment.provider)
        .all()
    )

    by_source: dict[str, int] = {}
    for prov, cnt in by_provider_rows:
        p = (prov or "").strip().lower()
        src = provider_to_source.get(p) or "other"
        by_source[src] = int(by_source.get(src, 0) + (cnt or 0))

    # Sample a few queued enrichments with profile names for UI clarity.
    enrich_sample_rows = (
        db.query(ProfileEnrichment, Profile)
        .join(
            JobProfile,
            (JobProfile.profile_id == ProfileEnrichment.profile_id)
            & (JobProfile.owner_id == ProfileEnrichment.owner_id),
        )
        .join(Profile, (Profile.id == ProfileEnrichment.profile_id) & (Profile.owner_id == owner_id))
        .filter(
            JobProfile.owner_id == owner_id,
            JobProfile.job_id == job_id,
            ProfileEnrichment.status == EnrichmentStatus.queued,
        )
        .order_by(ProfileEnrichment.created_at.asc())
        .limit(enrich_limit)
        .all()
    )

    # Stable ordering for known sources
    ordered_sources = ["source1", "source2", "source3", "other"]
    enrichment_queued_by_source = [
        PendingEnrichmentSourceCountOut(source=s, queued=by_source.get(s, 0))
        for s in ordered_sources
        if by_source.get(s, 0) > 0
    ]

    return JobPendingQueueOut(
        job_id=job_id,
        paused=paused,
        scouting_queued_count=int(scouting_queued_count),
        scouting_queued=[
            PendingScoutingRunOut(
                id=r.id,
                title_variant_id=r.title_variant_id,
                location_variant_id=r.location_variant_id,
                created_at=r.created_at,
            )
            for r in scouting_queued_rows
        ],
        enrichment_queued_count=int(enrichment_queued_count),
        enrichment_queued_by_source=enrichment_queued_by_source,
        enrichment_queued_sample=[
            PendingEnrichmentItemOut(
                enrichment_id=rec.id,
                profile_id=prof.id,
                name=prof.full_name_raw or "",
                city=prof.city or "",
                state=prof.state or "",
                created_at=rec.created_at,
            )
            for rec, prof in enrich_sample_rows
        ],
    )


@router.get("/{job_id}/profiles", response_model=JobProfilesOut)
def list_job_profiles(
    job_id: str,
    offset: int = 0,
    limit: int = 100,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    offset = max(int(offset or 0), 0)
    limit = min(max(int(limit or 0), 1), 500)

    base = (
        db.query(Profile)
        .join(
            JobProfile,
            (JobProfile.profile_id == Profile.id) & (JobProfile.owner_id == owner_id),
        )
        .filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id)
    )

    total = base.with_entities(func.count(Profile.id)).scalar() or 0

    rows = (
        db.query(Profile)
        .join(
            JobProfile,
            (JobProfile.profile_id == Profile.id) & (JobProfile.owner_id == owner_id),
        )
        .filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id)
        .order_by(JobProfile.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    items: list[JobProfileItemOut] = []
    for p in rows:
        cse = p.cse_item_json or {}
        # Prefer AI-extracted title; fall back to raw Google title if missing.
        title = ((p.title or "") or (cse.get("title") or "")).replace("\n", " ").strip()
        snippet = (cse.get("snippet") or "").replace("\n", " ").strip()
        source = str(cse.get("source") or "cse").strip() or "cse"
        items.append(
            JobProfileItemOut(
                id=p.id,
                name=p.full_name_raw or "",
                linkedin_url=p.linkedin_url_raw or "",
                city=p.city or "",
                state=p.state or "",
                title=title,
                snippet=snippet,
                source=source,
            )
        )

    return JobProfilesOut(job_id=job_id, total=total, offset=offset, limit=limit, profiles=items)


@router.get("/{job_id}/chat", response_model=List[ChatMessageOut])
def get_job_chat(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Get chat history for a job."""

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    messages = (
        db.query(JobChatMessage)
        .filter(JobChatMessage.job_id == job_id)
        .order_by(JobChatMessage.created_at)
        .all()
    )

    return [
        ChatMessageOut(
            id=m.id,
            role=m.role,
            content=m.content,
            created_at=m.created_at,
        )
        for m in messages
    ]


@router.post("/{job_id}/upload-profiles", response_model=UploadProfilesOut)
@limiter.limit("20/minute")
async def upload_profiles_csv(
    request: Request,
    job_id: str,
    file: UploadFile = File(...),
    apply_job_match: bool = Form(True),
    update_existing: bool = Form(False),
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Upload profiles to a job from a CSV file (multipart/form-data).

    Required columns (case-insensitive, flexible):
    - name OR full_name (must contain at least first + last)
    - city and state (or a single location column like "Miami, FL")
    Optional:
    - linkedin_url (or linkedin/url)
    - title
    - snippet
    """

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")

    # 10 MB max file size
    if len(raw) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10 MB)")

    try:
        text = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        text = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    if not fieldnames:
        raise HTTPException(status_code=400, detail="CSV missing header row")

    def _norm_key(s: str) -> str:
        return (s or "").strip().lower().replace("_", "").replace(" ", "")

    # Map normalized header -> actual header
    header_map: dict[str, str] = {}
    for h in fieldnames:
        nk = _norm_key(h)
        if nk and nk not in header_map:
            header_map[nk] = h

    def _get(row: dict, *aliases: str) -> str:
        for a in aliases:
            key = header_map.get(_norm_key(a))
            if not key:
                continue
            v = row.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    def _parse_city_state(row: dict) -> tuple[str, str]:
        city = _get(row, "city")
        state = _get(row, "state", "st")
        if city and state:
            return city, state

        loc = _get(row, "location")
        if loc:
            parts = [p.strip() for p in loc.split(",") if p.strip()]
            if len(parts) >= 2:
                return parts[0], parts[1]
        return "", ""

    # If we apply job match, load selected booleans once.
    #
    # IMPORTANT: For CSV uploads we intentionally match ONLY by title.
    # Location matching is brittle for uploads (users often provide state abbreviations like "PA"
    # while our location variants use full state names like "Pennsylvania").
    title_bools: list[str] = []
    location_bools: list[str] = []
    if apply_job_match:
        title_bools, _location_bools = worker_tasks._job_requirement_bools(
            db, owner_id=owner_id, job_id=job_id
        )
        location_bools = []

    stats = {
        "total_rows": 0,
        "created_profiles": 0,
        "existing_profiles": 0,
        "updated_existing": 0,
        "linked_to_job": 0,
        "skipped_duplicates": 0,
        "skipped_invalid": 0,
        "skipped_not_matching": 0,
    }
    errors: list[str] = []
    seen: set[str] = set()
    newly_linked_profile_ids: list[str] = []
    pinned_profile_ids: list[str] = []

    for row in reader:
        stats["total_rows"] += 1

        name_raw = _get(row, "full_name", "fullname", "name")
        if not name_raw:
            first = _get(row, "first_name", "firstname")
            last = _get(row, "last_name", "lastname")
            if first and last:
                name_raw = f"{first} {last}".strip()

        name_clean = clean_person_name(name_raw) if name_raw else None
        if not name_clean:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:missing_name")
            continue

        first_name, last_name = split_first_last(name_clean)
        if not last_name:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:name_missing_last")
            continue

        city, state = _parse_city_state(row)
        if not city or not state:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:missing_city_state")
            continue

        # Normalize state abbreviations to full state names so location booleans can match reliably.
        try:
            state = worker_tasks._expand_us_state_abbrev(state)
        except Exception:
            pass

        linkedin_url_raw = _get(row, "linkedin_url", "linkedin", "url")
        if linkedin_url_raw and is_linkedin_in_url(linkedin_url_raw):
            linkedin_url_canonical = normalize_linkedin_url(linkedin_url_raw)
        elif linkedin_url_raw:
            linkedin_url_canonical = linkedin_url_raw.strip()
        else:
            # Create a stable synthetic URL so we can dedupe and keep DB constraints intact.
            synthetic = f"https://scout.local/uploaded/{uuid.uuid4()}"
            linkedin_url_raw = synthetic
            linkedin_url_canonical = synthetic

        if linkedin_url_canonical in seen:
            stats["skipped_duplicates"] += 1
            continue
        seen.add(linkedin_url_canonical)

        title = _get(row, "title", "job_title", "position")
        snippet = _get(row, "snippet", "summary", "notes")

        prof = (
            db.query(Profile)
            .filter(Profile.owner_id == owner_id, Profile.linkedin_url_canonical == linkedin_url_canonical)
            .first()
        )
        if prof:
            stats["existing_profiles"] += 1
            if update_existing:
                changed = False
                if name_clean and prof.full_name_raw != name_clean:
                    prof.full_name_raw = name_clean
                    prof.first_name = first_name
                    prof.last_name = last_name
                    changed = True
                if city and prof.city != city:
                    prof.city = city
                    changed = True
                if state and prof.state != state:
                    prof.state = state
                    changed = True
                # Store uploaded title/snippet into cse_item_json for matching + exports.
                if title or snippet:
                    cse = dict(prof.cse_item_json or {})
                    if title:
                        cse["title"] = title
                    if snippet:
                        cse["snippet"] = snippet
                    cse["source"] = "upload"
                    prof.cse_item_json = cse
                    changed = True
                if changed:
                    stats["updated_existing"] += 1
                    db.add(prof)
        else:
            cse_item_json = {"title": title, "snippet": snippet, "source": "upload"}
            prof = Profile(
                owner_id=owner_id,
                linkedin_url_canonical=linkedin_url_canonical,
                linkedin_url_raw=linkedin_url_raw,
                full_name_raw=name_clean,
                first_name=first_name,
                last_name=last_name,
                city=city,
                state=state,
                country=None,
                cse_item_json=cse_item_json,
            )
            db.add(prof)
            db.flush()  # assigns id
            stats["created_profiles"] += 1

        # Optional: filter by current job requirements (uploads: TITLE ONLY).
        if apply_job_match:
            try:
                if not worker_tasks._profile_matches_job_requirement(
                    prof, title_bools=title_bools, location_bools=location_bools
                ):
                    stats["skipped_not_matching"] += 1
                    continue
            except Exception:
                # If matching fails unexpectedly, do not block upload.
                pass

        # Link to job if not already linked.
        existing_link = (
            db.query(JobProfile)
            .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id, JobProfile.profile_id == prof.id)
            .first()
        )
        if not existing_link:
            db.add(JobProfile(owner_id=owner_id, job_id=job_id, profile_id=prof.id))
            stats["linked_to_job"] += 1
            newly_linked_profile_ids.append(prof.id)

        # Pinned uploads: ensure this profile stays in the job even if it doesn't match booleans later.
        # (Rebuild should not delete pinned job_profiles.)
        existing_pin = (
            db.query(JobProfilePin)
            .filter(
                JobProfilePin.owner_id == owner_id,
                JobProfilePin.job_id == job_id,
                JobProfilePin.profile_id == prof.id,
            )
            .first()
        )
        if not existing_pin:
            db.add(
                JobProfilePin(
                    owner_id=owner_id,
                    job_id=job_id,
                    profile_id=prof.id,
                    source="upload_csv",
                )
            )
            pinned_profile_ids.append(prof.id)

    db.commit()

    # Auto-enrich newly linked profiles if this job has auto-enrichment enabled (Find phones was used).
    if newly_linked_profile_ids:
        try:
            worker_tasks.maybe_queue_job_auto_enrich(
                db, owner_id=owner_id, job_id=job_id, profile_ids=newly_linked_profile_ids
            )
        except Exception:
            pass

    # Keep job membership synced to selected booleans after upload (also pulls in global-pool matches).
    try:
        worker_tasks.rebuild_job_profiles.delay(job_id)
    except Exception:
        pass

    return UploadProfilesOut(
        job_id=job_id,
        apply_job_match=apply_job_match,
        total_rows=stats["total_rows"],
        created_profiles=stats["created_profiles"],
        existing_profiles=stats["existing_profiles"],
        updated_existing=stats["updated_existing"],
        linked_to_job=stats["linked_to_job"],
        skipped_duplicates=stats["skipped_duplicates"],
        skipped_invalid=stats["skipped_invalid"],
        skipped_not_matching=stats["skipped_not_matching"],
        errors=errors,
    )


@router.post("/upload-new", response_model=UploadNewJobOut)
@limiter.limit("20/minute")
async def upload_new_job(
    request: Request,
    name: str = Form(...),
    file: UploadFile = File(...),
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Create a new job and upload profiles to it in one step.
    No search/variants required - pure "bring your own list" flow.

    Required form fields:
    - name: Job name (user-provided)
    - file: CSV file with profiles

    CSV columns (case-insensitive, flexible):
    - name OR full_name (must contain at least first + last)
    - city and state (or a single location column like "Miami, FL")
    Optional:
    - linkedin_url (or linkedin/url)
    - title
    - snippet
    """

    job_name = (name or "").strip()
    if not job_name:
        raise HTTPException(status_code=400, detail="Job name is required")

    # Enforce free-tier job limit.
    from app.core.limits import check_can_create_job
    user = db.query(User).filter(User.id == owner_id).first()
    if user:
        check_can_create_job(user, db)

    # Create the job
    job = Job(
        owner_id=owner_id,
        name=job_name,
        goal_text="",  # No goal text for upload-only jobs
        status=JobStatus.active,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Parse CSV
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")

    # 10 MB max file size
    if len(raw) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10 MB)")

    try:
        text = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        text = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    if not fieldnames:
        raise HTTPException(status_code=400, detail="CSV missing header row")

    def _norm_key(s: str) -> str:
        return (s or "").strip().lower().replace("_", "").replace(" ", "")

    header_map: dict[str, str] = {}
    for h in fieldnames:
        nk = _norm_key(h)
        if nk and nk not in header_map:
            header_map[nk] = h

    def _get(row: dict, *aliases: str) -> str:
        for a in aliases:
            key = header_map.get(_norm_key(a))
            if not key:
                continue
            v = row.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    def _parse_city_state(row: dict) -> tuple[str, str]:
        city = _get(row, "city")
        state = _get(row, "state", "st")
        if city and state:
            return city, state

        loc = _get(row, "location")
        if loc:
            parts = [p.strip() for p in loc.split(",") if p.strip()]
            if len(parts) >= 2:
                return parts[0], parts[1]
        return "", ""

    stats = {
        "total_rows": 0,
        "created_profiles": 0,
        "existing_profiles": 0,
        "linked_to_job": 0,
        "skipped_duplicates": 0,
        "skipped_invalid": 0,
    }
    errors: list[str] = []
    seen: set[str] = set()

    for row in reader:
        stats["total_rows"] += 1

        name_raw = _get(row, "full_name", "fullname", "name")
        if not name_raw:
            first = _get(row, "first_name", "firstname")
            last = _get(row, "last_name", "lastname")
            if first and last:
                name_raw = f"{first} {last}".strip()

        name_clean = clean_person_name(name_raw) if name_raw else None
        if not name_clean:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:missing_name")
            continue

        first_name, last_name = split_first_last(name_clean)
        if not last_name:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:name_missing_last")
            continue

        city, state = _parse_city_state(row)
        if not city or not state:
            stats["skipped_invalid"] += 1
            if len(errors) < 50:
                errors.append("row_invalid:missing_city_state")
            continue

        # Normalize state abbreviations
        try:
            state = worker_tasks._expand_us_state_abbrev(state)
        except Exception:
            pass

        linkedin_url_raw = _get(row, "linkedin_url", "linkedin", "url")
        if linkedin_url_raw and is_linkedin_in_url(linkedin_url_raw):
            linkedin_url_canonical = normalize_linkedin_url(linkedin_url_raw)
        elif linkedin_url_raw:
            linkedin_url_canonical = linkedin_url_raw.strip()
        else:
            synthetic = f"https://scout.local/uploaded/{uuid.uuid4()}"
            linkedin_url_raw = synthetic
            linkedin_url_canonical = synthetic

        if linkedin_url_canonical in seen:
            stats["skipped_duplicates"] += 1
            continue
        seen.add(linkedin_url_canonical)

        title = _get(row, "title", "job_title", "position")
        snippet = _get(row, "snippet", "summary", "notes")

        prof = (
            db.query(Profile)
            .filter(Profile.owner_id == owner_id, Profile.linkedin_url_canonical == linkedin_url_canonical)
            .first()
        )
        if prof:
            stats["existing_profiles"] += 1
        else:
            cse_item_json = {"title": title, "snippet": snippet, "source": "upload"}
            prof = Profile(
                owner_id=owner_id,
                linkedin_url_canonical=linkedin_url_canonical,
                linkedin_url_raw=linkedin_url_raw,
                full_name_raw=name_clean,
                first_name=first_name,
                last_name=last_name,
                city=city,
                state=state,
                country=None,
                cse_item_json=cse_item_json,
            )
            db.add(prof)
            db.flush()
            stats["created_profiles"] += 1

        # Link to job
        existing_link = (
            db.query(JobProfile)
            .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job.id, JobProfile.profile_id == prof.id)
            .first()
        )
        if not existing_link:
            db.add(JobProfile(owner_id=owner_id, job_id=job.id, profile_id=prof.id))
            stats["linked_to_job"] += 1

        # Pin uploaded profiles so rebuild doesn't remove them
        existing_pin = (
            db.query(JobProfilePin)
            .filter(
                JobProfilePin.owner_id == owner_id,
                JobProfilePin.job_id == job.id,
                JobProfilePin.profile_id == prof.id,
            )
            .first()
        )
        if not existing_pin:
            db.add(
                JobProfilePin(
                    owner_id=owner_id,
                    job_id=job.id,
                    profile_id=prof.id,
                    source="upload_csv",
                )
            )

    db.commit()

    return UploadNewJobOut(
        job_id=job.id,
        job_name=job.name,
        total_rows=stats["total_rows"],
        created_profiles=stats["created_profiles"],
        existing_profiles=stats["existing_profiles"],
        linked_to_job=stats["linked_to_job"],
        skipped_duplicates=stats["skipped_duplicates"],
        skipped_invalid=stats["skipped_invalid"],
        errors=errors,
    )


@router.post("/{job_id}/repair-extraction", response_model=RepairExtractionOut)
def repair_extraction(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Queue a background repair pass for profiles linked to this job where
    name/location extraction from CSE was previously stored incorrectly.
    """
    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        worker_tasks.repair_job_profile_extraction.delay(job_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to queue repair")

    return RepairExtractionOut(job_id=job_id, queued=True)


class JobUpdateIn(BaseModel):
    name: Optional[str] = None
    status: Optional[JobStatus] = None
    paused: Optional[bool] = None


@router.get("/{job_id}/download-basic")
def download_basic_profiles(
    job_id: str,
    provider: Optional[str] = None,
    source: Optional[str] = None,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Download job profiles as a minimal CSV:
    - name
    - location (city, state)
    - title
    - phone_numbers (from selected provider, if available)
    """

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Optional: include phones from a specific provider.
    # If no provider is given, we fall back to ENRICH_PROVIDER (or "disabled").
    # Prefer `source` (public) to avoid leaking provider/vendor names.
    src = (source or "").strip().lower()
    prov = (provider or "").strip().lower()
    source_to_provider = {
        "source1": "scrapedo_advancedbackgroundchecks",
        "source2": "scrapedo_tps",
        "source3": "scrapedo_usphonebook",
    }
    if src == "chain":
        provider_name = "disabled"
    else:
        provider_name = source_to_provider.get(src) or prov or (settings.ENRICH_PROVIDER or "disabled")
    provider_name = (provider_name or "disabled").strip().lower() or "disabled"

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
        )
        .filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id)
        .filter(
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        )
        .order_by(JobProfile.created_at.desc(), ProfileEnrichment.finished_at.desc().nullslast())
        .all()
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "location", "title", "phone_numbers"])

    # One row per profile (job phone_count is also "any provider").
    # If provider_name is provided, prefer that provider's phones; otherwise pick the most recent.
    selected: dict[str, tuple[Profile, ProfileEnrichment]] = {}
    order: list[str] = []

    for profile, enrichment in rows:
        pid = profile.id
        if pid not in selected:
            selected[pid] = (profile, enrichment)
            order.append(pid)
            continue

        if provider_name != "disabled":
            cur_provider = (enrichment.provider or "").strip().lower()
            prev_provider = (selected[pid][1].provider or "").strip().lower()
            if cur_provider == provider_name and prev_provider != provider_name:
                selected[pid] = (profile, enrichment)

    for pid in order:
        profile, enrichment = selected[pid]
        cse = profile.cse_item_json or {}
        title = ((profile.title or "") or (cse.get("title") or "")).replace("\n", " ").strip()
        city = (profile.city or "").strip()
        state = (profile.state or "").strip()
        location = ", ".join([p for p in [city, state] if p])
        phones = enrichment.phone_numbers or []
        phones_str = ", ".join([p for p in phones if p])
        writer.writerow([profile.full_name_raw or "", location, title, phones_str])

    output.seek(0)

    job_name_slug = "".join(c if c.isalnum() or c in " -_" else "" for c in (job.name or "job"))[:40].strip()
    filename = f"{job_name_slug}_profiles_basic.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.patch("/{job_id}", response_model=JobOut)
def update_job(
    job_id: str,
    payload: JobUpdateIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Update job name or status."""

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    if payload.name is not None:
        job.name = payload.name
    if payload.status is not None:
        job.status = payload.status

    # Pause/resume processing (scouting + enrichment)
    if payload.paused is not None:
        row = (
            db.query(JobSettings)
            .filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id)
            .first()
        )
        if not row:
            row = JobSettings(owner_id=owner_id, job_id=job_id)
        row.paused = bool(payload.paused)
        db.add(row)

    db.add(job)
    db.commit()
    db.refresh(job)

    # If resuming, try to kick any queued scouting/enrichment work.
    if payload.paused is False:
        try:
            # Scouting: enqueue next queued run if nothing is running.
            running = (
                db.query(StrategyRun.id)
                .filter(
                    StrategyRun.owner_id == owner_id,
                    StrategyRun.job_id == job_id,
                    StrategyRun.status == StrategyRunStatus.running,
                )
                .first()
            )
            if not running:
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
                    worker_tasks.run_strategy_run.delay(next_run.id)
        except Exception:
            pass

        try:
            # Enrichment: run dispatcher (it will skip paused jobs).
            worker_tasks.run_enrichment_dispatcher.delay()
        except Exception:
            pass

    profile_count = (
        db.query(JobProfile).filter(JobProfile.job_id == job_id, JobProfile.owner_id == owner_id).count()
    )
    phone_count = (
        db.query(func.count(func.distinct(JobProfile.profile_id)))
        .join(
            ProfileEnrichment,
            (ProfileEnrichment.profile_id == JobProfile.profile_id)
            & (ProfileEnrichment.owner_id == owner_id),
        )
        .filter(
            JobProfile.job_id == job_id,
            JobProfile.owner_id == owner_id,
            ProfileEnrichment.status == EnrichmentStatus.completed,
            func.coalesce(func.jsonb_array_length(ProfileEnrichment.phone_numbers), 0) > 0,
        )
        .scalar()
        or 0
    )

    titles = (
        db.query(TitleVariant)
        .filter(TitleVariant.job_id == job_id, TitleVariant.owner_id == owner_id)
        .order_by(TitleVariant.created_at)
        .all()
    )
    locations = (
        db.query(LocationVariant)
        .filter(LocationVariant.job_id == job_id, LocationVariant.owner_id == owner_id)
        .order_by(LocationVariant.created_at)
        .all()
    )

    settings_row = (
        db.query(JobSettings).filter(JobSettings.owner_id == owner_id, JobSettings.job_id == job_id).first()
    )
    paused = bool(getattr(settings_row, "paused", False)) if settings_row else False

    return JobOut(
        id=job.id,
        name=job.name,
        goal_text=job.goal_text,
        status=job.status,
        profile_count=profile_count,
        phone_count=phone_count,
        paused=paused,
        created_at=job.created_at,
        title_variants=[
            VariantOut(
                id=t.id,
                entities=t.entities,
                boolean_text=t.boolean_text,
                selected=t.selected,
            )
            for t in titles
        ],
        location_variants=[
            VariantOut(
                id=l.id,
                entities=l.entities,
                boolean_text=l.boolean_text,
                selected=l.selected,
            )
            for l in locations
        ],
    )


@router.delete("/{job_id}")
def delete_job(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Delete a job and cascade-delete all associated data."""

    job = db.get(Job, job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Collect profile IDs linked to this job.
    job_profile_ids = [
        row[0]
        for row in db.query(JobProfile.profile_id)
        .filter(JobProfile.owner_id == owner_id, JobProfile.job_id == job_id)
        .all()
    ]

    # Find profiles exclusively linked to this job (not shared with other jobs).
    exclusive_profile_ids: list[str] = []
    for pid in job_profile_ids:
        other = (
            db.query(JobProfile.id)
            .filter(
                JobProfile.owner_id == owner_id,
                JobProfile.profile_id == pid,
                JobProfile.job_id != job_id,
            )
            .first()
        )
        if not other:
            exclusive_profile_ids.append(pid)

    # Delete in FK-safe order.

    # 1. SMS: outbound messages, batches, inbound messages
    db.query(SmsOutboundMessage).filter(
        SmsOutboundMessage.owner_id == owner_id, SmsOutboundMessage.job_id == job_id
    ).delete(synchronize_session=False)
    db.query(SmsBatch).filter(
        SmsBatch.owner_id == owner_id, SmsBatch.job_id == job_id
    ).delete(synchronize_session=False)
    db.query(SmsInboundMessage).filter(
        SmsInboundMessage.owner_id == owner_id, SmsInboundMessage.job_id == job_id
    ).delete(synchronize_session=False)

    # 2. Strategy runs + dropped profiles
    run_ids = [
        row[0]
        for row in db.query(StrategyRun.id)
        .filter(StrategyRun.owner_id == owner_id, StrategyRun.job_id == job_id)
        .all()
    ]
    if run_ids:
        db.query(DroppedProfile).filter(DroppedProfile.strategy_run_id.in_(run_ids)).delete(
            synchronize_session=False
        )
    db.query(StrategyRun).filter(
        StrategyRun.owner_id == owner_id, StrategyRun.job_id == job_id
    ).delete(synchronize_session=False)

    # 3. Variants
    db.query(TitleVariant).filter(
        TitleVariant.owner_id == owner_id, TitleVariant.job_id == job_id
    ).delete(synchronize_session=False)
    db.query(LocationVariant).filter(
        LocationVariant.owner_id == owner_id, LocationVariant.job_id == job_id
    ).delete(synchronize_session=False)

    # 4. Chat messages
    db.query(JobChatMessage).filter(JobChatMessage.job_id == job_id).delete(
        synchronize_session=False
    )

    # 5. Job profile pins + links
    db.query(JobProfilePin).filter(
        JobProfilePin.owner_id == owner_id, JobProfilePin.job_id == job_id
    ).delete(synchronize_session=False)
    db.query(JobProfile).filter(
        JobProfile.owner_id == owner_id, JobProfile.job_id == job_id
    ).delete(synchronize_session=False)

    # 6. Job settings
    db.query(JobSettings).filter(
        JobSettings.owner_id == owner_id, JobSettings.job_id == job_id
    ).delete(synchronize_session=False)

    # 7. Enrichments + profiles exclusively owned by this job
    if exclusive_profile_ids:
        db.query(ProfileEnrichment).filter(
            ProfileEnrichment.owner_id == owner_id,
            ProfileEnrichment.profile_id.in_(exclusive_profile_ids),
        ).delete(synchronize_session=False)
        db.query(Profile).filter(
            Profile.owner_id == owner_id,
            Profile.id.in_(exclusive_profile_ids),
        ).delete(synchronize_session=False)

    # 8. The job itself
    db.delete(job)
    db.commit()

    return {"deleted": True, "job_id": job_id}
