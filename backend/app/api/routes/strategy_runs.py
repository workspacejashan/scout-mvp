from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import (
    Job,
    LocationVariant,
    StrategyRun,
    StrategyRunStatus,
    TitleVariant,
    make_combo_signature_v2,
)
from app.db.session import get_db
from app.worker.tasks import run_strategy_run


router = APIRouter()

def _sanitize_public_run_error(err: Optional[str]) -> Optional[str]:
    """
    Do not leak provider/vendor names or implementation details to the UI.
    Keep this high-level.
    """
    if not err:
        return None
    s = str(err).strip()
    if not s:
        return None
    low = s.lower()
    if "google" in low or "serp" in low or "captcha" in low or "consent" in low:
        return "Search blocked. Try again later."
    if "scrape" in low or "scrapedo" in low or "provider" in low:
        return "Provider error. Try again later."
    if "llm" in low or "openai" in low or "openrouter" in low:
        return "AI error. Try again later."
    if "enqueue_failed" in low or "broker" in low or "redis" in low:
        return "Background worker error. Try again later."
    # Generic fallback
    return "Run failed."


# ------------------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------------------


class StrategyRunOut(BaseModel):
    id: str
    job_id: str
    title_variant_id: str
    location_variant_id: str
    boolean_text: str
    status: StrategyRunStatus
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    pages_total: int
    pages_completed: int
    added_count: int
    dropped_count: int
    error_count: int
    last_error: Optional[str]


class RunSelectedIn(BaseModel):
    job_id: str


class RunSelectedOut(BaseModel):
    queued: List[StrategyRunOut]
    skipped: int  # Already ran


class ComboIn(BaseModel):
    job_id: str
    title_variant_id: str
    location_variant_id: str


# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------


@router.post("/run-selected", response_model=RunSelectedOut)
def run_selected_combos(
    payload: RunSelectedIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Run all selected title × location combos for a job.
    Skips combos that have already been run (by signature).
    """

    job = db.get(Job, payload.job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get selected variants
    titles = (
        db.query(TitleVariant)
        .filter(
            TitleVariant.job_id == job.id,
            TitleVariant.owner_id == owner_id,
            TitleVariant.selected == True,
        )
        .all()
    )
    locations = (
        db.query(LocationVariant)
        .filter(
            LocationVariant.job_id == job.id,
            LocationVariant.owner_id == owner_id,
            LocationVariant.selected == True,
        )
        .all()
    )

    if not titles:
        raise HTTPException(status_code=400, detail="No title variants selected")
    if not locations:
        raise HTTPException(status_code=400, detail="No location variants selected")

    # Check for in-flight runs (we no longer hard-block; we only avoid double-enqueueing).
    in_flight = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job.id,
            StrategyRun.status.in_([StrategyRunStatus.queued, StrategyRunStatus.running]),
        )
        .first()
    )

    queued_runs: List[StrategyRun] = []
    skipped = 0

    # Create Cartesian product of selected variants
    for title in titles:
        for location in locations:
            combo_sig = make_combo_signature_v2(
                title.signature,
                location.signature,
                title.boolean_text,
                location.boolean_text,
            )

            # Skip if this exact combo was already created (any status).
            existing = (
                db.query(StrategyRun)
                .filter(
                    StrategyRun.owner_id == owner_id,
                    StrategyRun.job_id == job.id,
                    StrategyRun.combo_signature == combo_sig,
                )
                .first()
            )
            if existing:
                skipped += 1
                continue

            # Compose boolean
            boolean_text = f"{title.boolean_text} AND {location.boolean_text}"

            run = StrategyRun(
                owner_id=owner_id,
                job_id=job.id,
                title_variant_id=title.id,
                location_variant_id=location.id,
                boolean_text=boolean_text,
                combo_signature=combo_sig,
                status=StrategyRunStatus.queued,
            )
            db.add(run)
            queued_runs.append(run)

    if queued_runs:
        db.commit()
        for run in queued_runs:
            db.refresh(run)

    # Queue the first one ONLY if nothing is already running/queued for this job.
    # (Worker will auto-queue the next pending run when a run finishes.)
    if queued_runs and not in_flight:
        try:
            run_strategy_run.delay(queued_runs[0].id)
        except Exception as e:
            queued_runs[0].status = StrategyRunStatus.failed
            queued_runs[0].last_error = f"enqueue_failed: {e}"
            queued_runs[0].finished_at = datetime.utcnow()
            db.add(queued_runs[0])
            db.commit()

    return RunSelectedOut(
        queued=[
            StrategyRunOut(
                id=r.id,
                job_id=r.job_id,
                title_variant_id=r.title_variant_id,
                location_variant_id=r.location_variant_id,
                boolean_text=r.boolean_text,
                status=r.status,
                created_at=r.created_at,
                started_at=r.started_at,
                finished_at=r.finished_at,
                pages_total=r.pages_total,
                pages_completed=r.pages_completed,
                added_count=r.added_count,
                dropped_count=r.dropped_count,
                error_count=r.error_count,
                last_error=_sanitize_public_run_error(r.last_error),
            )
            for r in queued_runs
        ],
        skipped=skipped,
    )


@router.post("/run-combo", response_model=StrategyRunOut)
def run_single_combo(
    payload: ComboIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Run a single title+location combo."""

    job = db.get(Job, payload.job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    title = db.get(TitleVariant, payload.title_variant_id)
    if not title or title.owner_id != owner_id or title.job_id != job.id:
        raise HTTPException(status_code=404, detail="Title variant not found")

    location = db.get(LocationVariant, payload.location_variant_id)
    if not location or location.owner_id != owner_id or location.job_id != job.id:
        raise HTTPException(status_code=404, detail="Location variant not found")

    # Check for in-flight runs (we no longer hard-block; we only avoid double-enqueueing).
    in_flight = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job.id,
            StrategyRun.status.in_([StrategyRunStatus.queued, StrategyRunStatus.running]),
        )
        .first()
    )

    combo_sig = make_combo_signature_v2(
        title.signature,
        location.signature,
        title.boolean_text,
        location.boolean_text,
    )

    # Check if already ran
    existing = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == job.id,
            StrategyRun.combo_signature == combo_sig,
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="This combo has already been run")

    boolean_text = f"{title.boolean_text} AND {location.boolean_text}"

    run = StrategyRun(
        owner_id=owner_id,
        job_id=job.id,
        title_variant_id=title.id,
        location_variant_id=location.id,
        boolean_text=boolean_text,
        combo_signature=combo_sig,
        status=StrategyRunStatus.queued,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Enqueue only if no other run is already in-flight for this job.
    # Otherwise, it will be picked up by the worker's `_queue_next_run` chaining.
    if not in_flight:
        try:
            run_strategy_run.delay(run.id)
        except Exception as e:
            run.status = StrategyRunStatus.failed
            run.last_error = f"enqueue_failed: {e}"
            run.finished_at = datetime.utcnow()
            db.add(run)
            db.commit()
            db.refresh(run)

    return StrategyRunOut(
        id=run.id,
        job_id=run.job_id,
        title_variant_id=run.title_variant_id,
        location_variant_id=run.location_variant_id,
        boolean_text=run.boolean_text,
        status=run.status,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        pages_total=run.pages_total,
        pages_completed=run.pages_completed,
        added_count=run.added_count,
        dropped_count=run.dropped_count,
        error_count=run.error_count,
        last_error=_sanitize_public_run_error(run.last_error),
    )


@router.get("/{run_id}", response_model=StrategyRunOut)
def get_strategy_run(
    run_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Get status of a strategy run."""
    run = db.get(StrategyRun, run_id)
    if not run or run.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Not found")
    return StrategyRunOut(
        id=run.id,
        job_id=run.job_id,
        title_variant_id=run.title_variant_id,
        location_variant_id=run.location_variant_id,
        boolean_text=run.boolean_text,
        status=run.status,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        pages_total=run.pages_total,
        pages_completed=run.pages_completed,
        added_count=run.added_count,
        dropped_count=run.dropped_count,
        error_count=run.error_count,
        last_error=_sanitize_public_run_error(run.last_error),
    )


@router.get("/job/{job_id}", response_model=List[StrategyRunOut])
def list_strategy_runs(
    job_id: str,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """List all strategy runs for a job."""
    runs = (
        db.query(StrategyRun)
        .filter(StrategyRun.job_id == job_id, StrategyRun.owner_id == owner_id)
        .order_by(StrategyRun.created_at.desc())
        .all()
    )
    return [
        StrategyRunOut(
            id=r.id,
            job_id=r.job_id,
            title_variant_id=r.title_variant_id,
            location_variant_id=r.location_variant_id,
            boolean_text=r.boolean_text,
            status=r.status,
            created_at=r.created_at,
            started_at=r.started_at,
            finished_at=r.finished_at,
            pages_total=r.pages_total,
            pages_completed=r.pages_completed,
            added_count=r.added_count,
            dropped_count=r.dropped_count,
            error_count=r.error_count,
            last_error=_sanitize_public_run_error(r.last_error),
        )
        for r in runs
    ]


class RerunIn(BaseModel):
    run_id: str


@router.post("/rerun", response_model=StrategyRunOut)
def rerun_strategy_run(
    payload: RerunIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Rerun an existing combo (title_variant_id + location_variant_id).

    We create a NEW StrategyRun record so history isn't overwritten.
    The worker will pick it up when the current in-flight run (if any) finishes.
    """
    src = db.get(StrategyRun, payload.run_id)
    if not src or src.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Strategy run not found")

    # Create a new run with a unique signature so it can be re-run.
    rerun_sig = f"{src.combo_signature}::rerun::{uuid.uuid4().hex}"
    run = StrategyRun(
        owner_id=owner_id,
        job_id=src.job_id,
        title_variant_id=src.title_variant_id,
        location_variant_id=src.location_variant_id,
        boolean_text=src.boolean_text,
        combo_signature=rerun_sig,
        status=StrategyRunStatus.queued,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Enqueue only if nothing else is in-flight for this job.
    in_flight = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == src.job_id,
            StrategyRun.status.in_([StrategyRunStatus.queued, StrategyRunStatus.running]),
            StrategyRun.id != run.id,
        )
        .first()
    )
    if not in_flight:
        try:
            run_strategy_run.delay(run.id)
        except Exception as e:
            run.status = StrategyRunStatus.failed
            run.last_error = f"enqueue_failed: {e}"
            run.finished_at = datetime.utcnow()
            db.add(run)
            db.commit()
            db.refresh(run)

    return StrategyRunOut(
        id=run.id,
        job_id=run.job_id,
        title_variant_id=run.title_variant_id,
        location_variant_id=run.location_variant_id,
        boolean_text=run.boolean_text,
        status=run.status,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        pages_total=run.pages_total,
        pages_completed=run.pages_completed,
        added_count=run.added_count,
        dropped_count=run.dropped_count,
        error_count=run.error_count,
        last_error=_sanitize_public_run_error(run.last_error),
    )


class ResetStuckIn(BaseModel):
    job_id: str
    max_age_minutes: int = 30


class ResetStuckOut(BaseModel):
    reset_count: int
    reset_run_ids: List[str]
    enqueued_run_id: Optional[str] = None


@router.post("/reset-stuck", response_model=ResetStuckOut)
def reset_stuck_strategy_runs(
    payload: ResetStuckIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    If a run was marked `running` but the worker restarted/crashed, it can get stuck forever.
    This endpoint marks old running runs as failed and enqueues the next queued run.
    """
    max_age = max(int(payload.max_age_minutes or 0), 1)
    cutoff = datetime.utcnow() - timedelta(minutes=max_age)

    stuck = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == payload.job_id,
            StrategyRun.status == StrategyRunStatus.running,
            StrategyRun.finished_at.is_(None),
            StrategyRun.started_at.isnot(None),
            StrategyRun.started_at < cutoff,
        )
        .all()
    )

    reset_ids: list[str] = []
    for r in stuck:
        r.status = StrategyRunStatus.failed
        r.finished_at = datetime.utcnow()
        r.last_error = f"stale_run_reset (started_at {r.started_at})"
        r.error_count = int(r.error_count or 0) + 1
        db.add(r)
        reset_ids.append(r.id)

    if reset_ids:
        db.commit()

    # Enqueue next queued run (if any)
    next_run = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == payload.job_id,
            StrategyRun.status == StrategyRunStatus.queued,
        )
        .order_by(StrategyRun.created_at)
        .first()
    )

    enqueued_id: Optional[str] = None
    if next_run:
        try:
            run_strategy_run.delay(next_run.id)
            enqueued_id = next_run.id
        except Exception:
            # If enqueue fails, keep it queued; worker retry/other actions can pick it up later.
            enqueued_id = None

    return ResetStuckOut(reset_count=len(reset_ids), reset_run_ids=reset_ids, enqueued_run_id=enqueued_id)


class ResumeQueueIn(BaseModel):
    job_id: str


class ResumeQueueOut(BaseModel):
    enqueued_run_id: Optional[str] = None


@router.post("/resume-queue", response_model=ResumeQueueOut)
def resume_queued_strategy_runs(
    payload: ResumeQueueIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Safety net: If there are queued runs but nothing is running, enqueue the next queued run.
    This fixes cases where a prior bug/crash prevented chaining from continuing.
    """

    job = db.get(Job, payload.job_id)
    if not job or job.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Job not found")

    # If anything is already running, do nothing.
    running = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == payload.job_id,
            StrategyRun.status == StrategyRunStatus.running,
        )
        .first()
    )
    if running:
        return ResumeQueueOut(enqueued_run_id=None)

    next_run = (
        db.query(StrategyRun)
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.job_id == payload.job_id,
            StrategyRun.status == StrategyRunStatus.queued,
        )
        .order_by(StrategyRun.created_at)
        .first()
    )
    if not next_run:
        return ResumeQueueOut(enqueued_run_id=None)

    try:
        run_strategy_run.delay(next_run.id)
        return ResumeQueueOut(enqueued_run_id=next_run.id)
    except Exception:
        return ResumeQueueOut(enqueued_run_id=None)
