from __future__ import annotations

from datetime import datetime
import json
import re
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import (
    Job,
    JobChatMessage,
    JobStatus,
    LocationBooleanCache,
    LocationVariant,
    TitleBooleanCache,
    TitleVariant,
    User,
    make_signature,
)
from app.db.session import get_db
from app.services.openrouter import OpenRouterError, generate_suggestions
from app.worker.tasks import rebuild_job_profiles


router = APIRouter()

def _sanitize_public_copilot_error(err: Exception) -> str:
    s = str(err or "").strip()
    low = s.lower()
    # Avoid leaking vendor names / env var names.
    if "api_key" in low or "openai_api_key" in low or "openrouter_api_key" in low:
        return "AI auth failed. Check server configuration."
    if "openai" in low or "openrouter" in low:
        return "AI provider error. Try again later."
    return "Copilot error. Try again later."


# ------------------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------------------


class SuggestionItem(BaseModel):
    entities: List[str]
    boolean: str


class SuggestionsData(BaseModel):
    title_suggestions: List[SuggestionItem]
    location_suggestions: List[SuggestionItem]
    message: str


class CopilotMessageIn(BaseModel):
    job_id: Optional[str] = None
    message: str


class CopilotJobOut(BaseModel):
    id: str
    name: str
    goal_text: str
    status: JobStatus
    created_at: datetime


class VariantOut(BaseModel):
    id: str
    entities: List[str]
    boolean_text: str
    selected: bool


class CopilotMessageOut(BaseModel):
    job: CopilotJobOut
    assistant_message: str
    suggestions: Optional[SuggestionsData] = None
    title_variants: List[VariantOut]
    location_variants: List[VariantOut]


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def _default_job_name(goal_text: str) -> str:
    s = goal_text.strip().replace("\n", " ")
    if len(s) <= 48:
        return s or "Untitled job"
    return s[:48].rstrip() + "…"


def _normalize_goal_key(text: str) -> str:
    """
    Normalize goal text for de-duping/seeding.
    Examples:
      - "Tarzana, CA" and "Tarzana CA" should match.
      - case/spacing differences should match.
    """
    s = (text or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    # Remove punctuation but keep spaces so words remain separable.
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _best_seed_job_for_goal(db: Session, *, owner_id: str, goal_key: str) -> Optional[Job]:
    """
    If the user creates the exact same search multiple times, we prefer seeding the new job
    with variants from the most "developed" existing job, so DB matching pulls in existing
    profiles immediately.

    Heuristic:
    - Only consider active jobs with at least 1 title + 1 location variant.
    - Prefer the job with the most location variants (usually the one where user expanded cities).
    - Tie-breaker: most recently created.
    """
    if not goal_key:
        return None

    candidates = (
        db.query(Job)
        .filter(Job.owner_id == owner_id, Job.status == JobStatus.active)
        .order_by(Job.created_at.desc())
        .all()
    )
    candidates = [j for j in candidates if _normalize_goal_key(j.goal_text) == goal_key]
    if not candidates:
        return None

    ids = [j.id for j in candidates]
    # Count variants per job (owner-scoped)
    loc_counts = dict(
        db.query(LocationVariant.job_id, func.count(LocationVariant.id))
        .filter(LocationVariant.owner_id == owner_id, LocationVariant.job_id.in_(ids))
        .group_by(LocationVariant.job_id)
        .all()
    )
    title_counts = dict(
        db.query(TitleVariant.job_id, func.count(TitleVariant.id))
        .filter(TitleVariant.owner_id == owner_id, TitleVariant.job_id.in_(ids))
        .group_by(TitleVariant.job_id)
        .all()
    )

    best: Optional[Job] = None
    best_loc = -1
    best_title = -1
    for j in candidates:
        lc = int(loc_counts.get(j.id, 0) or 0)
        tc = int(title_counts.get(j.id, 0) or 0)
        if lc <= 0 or tc <= 0:
            continue
        if lc > best_loc or (lc == best_loc and tc > best_title) or (
            lc == best_loc and tc == best_title and best and j.created_at > best.created_at
        ):
            best = j
            best_loc = lc
            best_title = tc
        if best is None:
            best = j
            best_loc = lc
            best_title = tc

    return best


def _seed_variants_from_job(
    db: Session,
    *,
    owner_id: str,
    dst_job_id: str,
    src_job_id: str,
) -> tuple[int, int]:
    """Copy variants from src job to dst job (skip by signature). Returns (titles_added, locations_added)."""
    titles_added = 0
    locations_added = 0

    src_titles = (
        db.query(TitleVariant)
        .filter(TitleVariant.owner_id == owner_id, TitleVariant.job_id == src_job_id)
        .order_by(TitleVariant.created_at)
        .all()
    )
    src_locations = (
        db.query(LocationVariant)
        .filter(LocationVariant.owner_id == owner_id, LocationVariant.job_id == src_job_id)
        .order_by(LocationVariant.created_at)
        .all()
    )

    dst_title_sigs = {
        r[0]
        for r in (
            db.query(TitleVariant.signature)
            .filter(TitleVariant.owner_id == owner_id, TitleVariant.job_id == dst_job_id)
            .all()
        )
    }
    dst_loc_sigs = {
        r[0]
        for r in (
            db.query(LocationVariant.signature)
            .filter(LocationVariant.owner_id == owner_id, LocationVariant.job_id == dst_job_id)
            .all()
        )
    }

    for t in src_titles:
        if t.signature in dst_title_sigs:
            continue
        db.add(
            TitleVariant(
                owner_id=owner_id,
                job_id=dst_job_id,
                entities=t.entities,
                boolean_text=t.boolean_text,
                signature=t.signature,
                selected=bool(getattr(t, "selected", True)),
            )
        )
        dst_title_sigs.add(t.signature)
        titles_added += 1

    for l in src_locations:
        if l.signature in dst_loc_sigs:
            continue
        db.add(
            LocationVariant(
                owner_id=owner_id,
                job_id=dst_job_id,
                entities=l.entities,
                boolean_text=l.boolean_text,
                signature=l.signature,
                selected=bool(getattr(l, "selected", True)),
            )
        )
        dst_loc_sigs.add(l.signature)
        locations_added += 1

    if titles_added or locations_added:
        db.commit()

    return titles_added, locations_added


_NUMBER_WORDS: dict[str, int] = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    # Common phrases
    "couple": 2,
    "few": 5,
    "dozen": 12,
}


def _parse_requested_city_count(message: str) -> Optional[int]:
    """
    Best-effort count extraction from natural language.
    - "add 7 cities" -> 7
    - "add twenty cities" -> 20
    - "add a couple cities" -> 2
    If none found, returns None.
    """
    s = (message or "").strip().lower()
    if not s:
        return None

    # Prefer explicit "N cities/locations" to avoid confusing radius miles with count.
    m = re.search(r"\b(\d{1,3})\b\s*(?:new\s*)?(?:cities|city|locations|location)\b", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    for w, n in _NUMBER_WORDS.items():
        if re.search(r"\b" + re.escape(w) + r"\b\s*(?:new\s*)?(?:cities|city|locations|location)\b", s):
            return n

    # Fallback: plain numbers, but try not to pick up miles/km radius.
    m = re.search(r"\b(\d{1,3})\b(?!\s*(?:miles?|mi\b|km\b|kilometers?))", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    # Fallback: plain number words (rare), but avoid over-triggering.
    for w, n in _NUMBER_WORDS.items():
        if re.search(r"\b" + re.escape(w) + r"\b", s):
            return n

    return None


def _wants_location_expansion(message: str) -> bool:
    s = (message or "").strip().lower()
    if not s:
        return False

    # Heuristic: user is asking to "add" locations/cities (not just chatting)
    has_add = bool(re.search(r"\badd\b|\bmore\b|\banother\b|\badditional\b", s))
    has_geo = bool(
        re.search(
            r"\bcity\b|\bcities\b|\blocation\b|\blocations\b|\bnearby\b|\baround\b|\bwithin\b|\bradius\b|\bmiles?\b|\bzip\b|\bzipcode\b",
            s,
        )
    )
    return has_add and has_geo


def _get_variants(db: Session, job_id: str, owner_id: str):
    """Get current title and location variants for a job."""
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
    return titles, locations


def _add_variants_from_suggestions(
    db: Session,
    job_id: str,
    owner_id: str,
    suggestions: dict,
):
    """Add new variants from AI suggestions (skip duplicates by signature)."""
    added_titles = 0
    added_locations = 0
    updated_titles = 0
    updated_locations = 0
    updated_caches = 0

    for item in suggestions.get("title_suggestions", []):
        entities = item.get("entities", [])
        boolean_text = item.get("boolean", "")
        if not entities or not boolean_text:
            continue
        sig = make_signature(entities)

        # Global cache (owner-scoped): signature -> boolean_text
        cached = (
            db.query(TitleBooleanCache)
            .filter(
                TitleBooleanCache.owner_id == owner_id,
                TitleBooleanCache.signature == sig,
            )
            .first()
        )
        if cached:
            # Keep cache fresh: if the model/prompt improves, don't let stale cache override it.
            if (cached.boolean_text or "").strip() != (boolean_text or "").strip() or (
                cached.entities or []
            ) != (entities or []):
                cached.boolean_text = boolean_text
                cached.entities = entities
                db.add(cached)
                updated_caches += 1
        else:
            db.add(
                TitleBooleanCache(
                    owner_id=owner_id,
                    entities=entities,
                    boolean_text=boolean_text,
                    signature=sig,
                )
            )
            updated_caches += 1

        existing = (
            db.query(TitleVariant)
            .filter(
                TitleVariant.job_id == job_id,
                TitleVariant.owner_id == owner_id,
                TitleVariant.signature == sig,
            )
            .first()
        )
        if not existing:
            db.add(
                TitleVariant(
                    owner_id=owner_id,
                    job_id=job_id,
                    entities=entities,
                    boolean_text=boolean_text,
                    signature=sig,
                    selected=True,
                )
            )
            added_titles += 1
        else:
            # Repair existing variant if cache/prompt has a better boolean now.
            if (existing.boolean_text or "").strip() != (boolean_text or "").strip() or (
                existing.entities or []
            ) != (entities or []):
                existing.boolean_text = boolean_text
                existing.entities = entities
                db.add(existing)
                updated_titles += 1

    for item in suggestions.get("location_suggestions", []):
        entities = item.get("entities", [])
        boolean_text = item.get("boolean", "")
        if not entities or not boolean_text:
            continue
        sig = make_signature(entities)

        cached = (
            db.query(LocationBooleanCache)
            .filter(
                LocationBooleanCache.owner_id == owner_id,
                LocationBooleanCache.signature == sig,
            )
            .first()
        )
        if cached:
            if (cached.boolean_text or "").strip() != (boolean_text or "").strip() or (
                cached.entities or []
            ) != (entities or []):
                cached.boolean_text = boolean_text
                cached.entities = entities
                db.add(cached)
                updated_caches += 1
        else:
            db.add(
                LocationBooleanCache(
                    owner_id=owner_id,
                    entities=entities,
                    boolean_text=boolean_text,
                    signature=sig,
                )
            )
            updated_caches += 1

        existing = (
            db.query(LocationVariant)
            .filter(
                LocationVariant.job_id == job_id,
                LocationVariant.owner_id == owner_id,
                LocationVariant.signature == sig,
            )
            .first()
        )
        if not existing:
            db.add(
                LocationVariant(
                    owner_id=owner_id,
                    job_id=job_id,
                    entities=entities,
                    boolean_text=boolean_text,
                    signature=sig,
                    selected=True,
                )
            )
            added_locations += 1
        else:
            if (existing.boolean_text or "").strip() != (boolean_text or "").strip() or (
                existing.entities or []
            ) != (entities or []):
                existing.boolean_text = boolean_text
                existing.entities = entities
                db.add(existing)
                updated_locations += 1

    if added_titles or added_locations or updated_titles or updated_locations or updated_caches:
        db.commit()

    return added_titles + updated_titles, added_locations + updated_locations


# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------


@router.post("/message", response_model=CopilotMessageOut)
def send_message(
    payload: CopilotMessageIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """
    Send a message to copilot.
    - If no job_id: creates a new job.
    - Returns structured suggestions (title/location variants).
    """
    msg = (payload.message or "").strip()
    if not msg:
        raise HTTPException(status_code=400, detail="Empty message")

    # Get or create job
    job: Optional[Job] = None
    seeded_from: Optional[str] = None
    if payload.job_id:
        job = db.get(Job, payload.job_id)
        if not job or job.owner_id != owner_id:
            raise HTTPException(status_code=404, detail="Job not found")
    else:
        # Enforce free-tier job limit before creating.
        from app.core.limits import check_can_create_job
        user = db.query(User).filter(User.id == owner_id).first()
        if user:
            check_can_create_job(user, db)
        # If this exact search already exists, seed the new job with variants from the best prior job
        # (typically the one where the user already expanded locations). This ensures DB matching
        # pulls in existing profiles immediately instead of starting from only 1 city.
        goal_key = _normalize_goal_key(msg)
        seed_job = _best_seed_job_for_goal(db, owner_id=owner_id, goal_key=goal_key)

        job = Job(
            owner_id=owner_id,
            name=_default_job_name(msg),
            goal_text=msg,
            status=JobStatus.active,
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        if seed_job and seed_job.id != job.id:
            try:
                _, loc_added = _seed_variants_from_job(
                    db,
                    owner_id=owner_id,
                    dst_job_id=job.id,
                    src_job_id=seed_job.id,
                )
                if loc_added:
                    seeded_from = seed_job.id
            except Exception:
                seeded_from = None

    # Save user message
    db.add(
        JobChatMessage(
            job_id=job.id,
            owner_id=owner_id,
            role="user",
            content=msg,
        )
    )
    db.commit()

    # Snapshot current variants (used for "add more locations" UX)
    titles_before, locations_before = _get_variants(db, job.id, owner_id)

    # Generate suggestions via AI
    suggestions_data: Optional[dict] = None
    assistant_message: str = ""

    try:
        # If we seeded a new job from a previous job's variants, don't hit the LLM again here.
        # The goal of seeding is to immediately get the "expanded locations" + DB matches the user
        # already established previously.
        if seeded_from:
            assistant_message = "Reused existing locations from your previous search."
            # Trigger rebuild so matching profiles from the global pool are linked right away.
            try:
                rebuild_job_profiles.delay(job.id)
            except Exception:
                pass
        else:
            expand_locations = bool(payload.job_id) and _wants_location_expansion(msg)

            if expand_locations:
                # Hard cap: up to 20 new cities per message (largest/most relevant first)
                requested = _parse_requested_city_count(msg)
                target_new = max(1, min(int(requested or 20), 20))

                existing_loc_entities: list[list[str]] = [l.entities for l in locations_before if l.entities]
                seen_sigs = {l.signature for l in locations_before if getattr(l, "signature", None)}

                collected: list[dict] = []
                max_attempts = 4

                for _ in range(max_attempts):
                    remaining = target_new - len(collected)
                    if remaining <= 0:
                        break

                    augmented_msg = (
                        f"{msg}\n\n"
                        f"CONTEXT:\n"
                        f"- EXISTING_LOCATIONS_JSON={json.dumps(existing_loc_entities)}\n"
                        f"- NEED_NEW_LOCATIONS={remaining}\n\n"
                        f"INSTRUCTIONS:\n"
                        f"- Return ONLY new location_suggestions; set title_suggestions to [].\n"
                        f"- Do NOT repeat anything from EXISTING_LOCATIONS_JSON.\n"
                        f"- Return at most NEED_NEW_LOCATIONS items.\n"
                        f"- Each location_suggestions[i].entities MUST be [City, FullStateName].\n"
                        f"- Prefer largest / most relevant nearby cities first.\n"
                        f"- If the user gave geo rules (radius/zip/exclusions), follow them best-effort.\n"
                        f"- Output valid JSON only (no markdown).\n"
                    )

                    resp = generate_suggestions(goal_text=job.goal_text, user_message=augmented_msg) or {}
                    new_items: list[dict] = []

                    for item in resp.get("location_suggestions", []) or []:
                        entities = item.get("entities", []) or []
                        boolean_text = (item.get("boolean", "") or "").strip()
                        if not entities or not boolean_text:
                            continue
                        # Expect [City, FullStateName]
                        if len(entities) != 2:
                            continue
                        city = (entities[0] or "").strip()
                        state = (entities[1] or "").strip()
                        if not city or not state:
                            continue
                        # Full state name (not "CA")
                        if len(re.sub(r"[^A-Za-z]", "", state)) <= 2:
                            continue
                        sig = make_signature(entities)
                        if sig in seen_sigs:
                            continue
                        seen_sigs.add(sig)
                        new_items.append({"entities": entities, "boolean": boolean_text})

                    if not new_items:
                        break

                    # Keep unique + update context for next attempt
                    for it in new_items:
                        if len(collected) >= target_new:
                            break
                        collected.append(it)
                        existing_loc_entities.append(it["entities"])

                    if len(collected) >= target_new:
                        break

                suggestions_data = {
                    "title_suggestions": [],
                    "location_suggestions": collected,
                    "message": f"Added {len(collected)} new locations" if collected else "No new locations found",
                }
            else:
                suggestions_data = generate_suggestions(
                    goal_text=job.goal_text,
                    user_message=msg,
                )

            assistant_message = (suggestions_data or {}).get("message", "Here are your options.")

            # Add variants from suggestions
            changed_titles, changed_locations = _add_variants_from_suggestions(
                db, job.id, owner_id, suggestions_data or {}
            )

            # Keep job_profiles synced when variants change (also pulls from global pool)
            if (changed_titles or 0) > 0 or (changed_locations or 0) > 0:
                try:
                    rebuild_job_profiles.delay(job.id)
                except Exception:
                    pass

    except OpenRouterError as e:
        assistant_message = f"[error] {_sanitize_public_copilot_error(e)}"

    # Save assistant message
    db.add(
        JobChatMessage(
            job_id=job.id,
            owner_id=owner_id,
            role="assistant",
            content=assistant_message,
            suggestions_json=suggestions_data,
        )
    )
    db.commit()

    # Get current variants
    titles, locations = _get_variants(db, job.id, owner_id)

    return CopilotMessageOut(
        job=CopilotJobOut(
            id=job.id,
            name=job.name,
            goal_text=job.goal_text,
            status=job.status,
            created_at=job.created_at,
        ),
        assistant_message=assistant_message,
        suggestions=SuggestionsData(
            title_suggestions=[
                SuggestionItem(entities=s["entities"], boolean=s["boolean"])
                for s in (suggestions_data or {}).get("title_suggestions", [])
            ],
            location_suggestions=[
                SuggestionItem(entities=s["entities"], boolean=s["boolean"])
                for s in (suggestions_data or {}).get("location_suggestions", [])
            ],
            message=assistant_message,
        )
        if suggestions_data
        else None,
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


# ------------------------------------------------------------------------------
# Variant Management
# ------------------------------------------------------------------------------


class ToggleVariantIn(BaseModel):
    variant_id: str
    selected: bool


@router.post("/toggle-title-variant")
def toggle_title_variant(
    payload: ToggleVariantIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Toggle selection of a title variant."""
    variant = db.get(TitleVariant, payload.variant_id)
    if not variant or variant.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Not found")
    variant.selected = payload.selected
    db.add(variant)
    db.commit()
    try:
        rebuild_job_profiles.delay(variant.job_id)
    except Exception:
        pass
    return {"ok": True}


@router.post("/toggle-location-variant")
def toggle_location_variant(
    payload: ToggleVariantIn,
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Toggle selection of a location variant."""
    variant = db.get(LocationVariant, payload.variant_id)
    if not variant or variant.owner_id != owner_id:
        raise HTTPException(status_code=404, detail="Not found")
    variant.selected = payload.selected
    db.add(variant)
    db.commit()
    try:
        rebuild_job_profiles.delay(variant.job_id)
    except Exception:
        pass
    return {"ok": True}
