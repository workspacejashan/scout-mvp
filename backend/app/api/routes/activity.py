from __future__ import annotations

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import (
    EnrichmentStatus,
    JobProfile,
    ProfileEnrichment,
    StrategyRun,
    StrategyRunStatus,
)
from app.db.session import get_db

router = APIRouter()


class JobActivity(BaseModel):
    job_id: str
    scouting_running: int
    scouting_queued: int
    enriching_running: int
    enriching_queued: int


class ActivityStatusOut(BaseModel):
    jobs: Dict[str, JobActivity]
    total_active_jobs: int


@router.get("/status", response_model=ActivityStatusOut)
def get_activity_status(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Get aggregated activity status for all jobs.
    Returns counts of running/queued tasks for scouting and enrichment.
    """

    # Initialize result map
    activity_map: Dict[str, JobActivity] = {}

    def _get_or_create(jid: str) -> JobActivity:
        if jid not in activity_map:
            activity_map[jid] = JobActivity(
                job_id=jid,
                scouting_running=0,
                scouting_queued=0,
                enriching_running=0,
                enriching_queued=0,
            )
        return activity_map[jid]

    # 1. Scouting Activity (StrategyRuns)
    # Group by job_id, status
    scout_rows = (
        db.query(
            StrategyRun.job_id,
            StrategyRun.status,
            func.count(StrategyRun.id),
        )
        .filter(
            StrategyRun.owner_id == owner_id,
            StrategyRun.status.in_([StrategyRunStatus.queued, StrategyRunStatus.running]),
        )
        .group_by(StrategyRun.job_id, StrategyRun.status)
        .all()
    )

    for job_id, status, count in scout_rows:
        act = _get_or_create(job_id)
        if status == StrategyRunStatus.running:
            act.scouting_running += count
        elif status == StrategyRunStatus.queued:
            act.scouting_queued += count

    # 2. Enrichment Activity (ProfileEnrichment linked via JobProfile)
    # This is trickier because ProfileEnrichment is per-profile, and a profile can belong to multiple jobs.
    # We want to know if *this job* has triggered enrichment.
    # However, ProfileEnrichment doesn't link to Job directly, only Owner + Profile.
    # Best approximation: Join JobProfile to find which jobs contain these active profiles.
    # Note: If a profile is in 2 jobs, both will show activity. This is acceptable for a UI tracker.

    enrich_rows = (
        db.query(
            JobProfile.job_id,
            ProfileEnrichment.status,
            func.count(ProfileEnrichment.id),
        )
        .join(
            ProfileEnrichment,
            (ProfileEnrichment.profile_id == JobProfile.profile_id)
            & (ProfileEnrichment.owner_id == JobProfile.owner_id),
        )
        .filter(
            JobProfile.owner_id == owner_id,
            ProfileEnrichment.status.in_([EnrichmentStatus.queued, EnrichmentStatus.running]),
        )
        .group_by(JobProfile.job_id, ProfileEnrichment.status)
        .all()
    )

    for job_id, status, count in enrich_rows:
        act = _get_or_create(job_id)
        if status == EnrichmentStatus.running:
            act.enriching_running += count
        elif status == EnrichmentStatus.queued:
            act.enriching_queued += count

    # Filter out empty entries? No, _get_or_create only happens if there IS activity.
    # So activity_map only contains jobs with at least 1 running or queued item.

    return ActivityStatusOut(
        jobs=activity_map,
        total_active_jobs=len(activity_map),
    )
