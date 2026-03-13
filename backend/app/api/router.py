from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.routes import activity, billing, copilot, enrichment, jobs, strategy_runs, users
from app.core.auth import require_admin


api_router = APIRouter(dependencies=[Depends(require_admin)])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(activity.router, prefix="/activity", tags=["activity"])
api_router.include_router(copilot.router, prefix="/copilot", tags=["copilot"])
api_router.include_router(strategy_runs.router, prefix="/strategy-runs", tags=["strategy-runs"])
api_router.include_router(enrichment.router, prefix="/enrichment", tags=["enrichment"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(billing.router, prefix="/billing", tags=["billing"])
