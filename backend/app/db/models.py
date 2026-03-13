from __future__ import annotations

import enum
import hashlib
import re
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


def _utcnow() -> datetime:
    return datetime.utcnow()


def _uuid() -> str:
    return str(uuid.uuid4())


# ------------------------------------------------------------------------------
# Enums
# ------------------------------------------------------------------------------


class AccountTier(str, enum.Enum):
    free = "free"
    pro = "pro"          # paid via Stripe
    unlocked = "unlocked"  # universal code activation


class JobStatus(str, enum.Enum):
    active = "active"
    archived = "archived"


class StrategyRunStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    partial = "partial"
    failed = "failed"


class EnrichmentStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"


# ------------------------------------------------------------------------------
# User
# ------------------------------------------------------------------------------


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)

    # Tier: free (default), pro (Stripe subscription), unlocked (universal code)
    tier: Mapped[AccountTier] = mapped_column(
        Enum(AccountTier, native_enum=False), default=AccountTier.free
    )

    # Stripe fields (nullable for free/unlocked users)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, unique=True
    )
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    stripe_subscription_status: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # "active", "past_due", "canceled"
    stripe_current_period_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=False), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=_utcnow, onupdate=_utcnow
    )


# ------------------------------------------------------------------------------
# Job
# ------------------------------------------------------------------------------


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    name: Mapped[str] = mapped_column(String, nullable=False)
    goal_text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.active)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=_utcnow, onupdate=_utcnow
    )

    chat_messages: Mapped[list["JobChatMessage"]] = relationship(back_populates="job")
    title_variants: Mapped[list["TitleVariant"]] = relationship(back_populates="job")
    location_variants: Mapped[list["LocationVariant"]] = relationship(back_populates="job")
    strategy_runs: Mapped[list["StrategyRun"]] = relationship(back_populates="job")
    job_profiles: Mapped[list["JobProfile"]] = relationship(back_populates="job")


# ------------------------------------------------------------------------------
# Job-level settings (e.g., auto-enrichment)
# ------------------------------------------------------------------------------


class JobSettings(Base):
    """
    Per-job settings that we want to persist without introducing migrations.
    (New tables can be created via create_all; altering existing tables cannot.)
    """

    __tablename__ = "job_settings"
    __table_args__ = (UniqueConstraint("owner_id", "job_id", name="uq_owner_job_settings"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)

    # Auto-enrichment
    auto_enrich_enabled: Mapped[bool] = mapped_column(default=False)
    auto_enrich_provider: Mapped[str] = mapped_column(String, nullable=False, default="disabled")

    # Processing control (scouting + enrichment)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)

    # Outreach (SMS)
    job_location_label: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    sms_template_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sms_daily_limit: Mapped[int] = mapped_column(Integer, default=50)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow, onupdate=_utcnow)


# ------------------------------------------------------------------------------
# Owner-level settings (single-owner v1; still persisted for future multi-owner)
# ------------------------------------------------------------------------------


class OwnerSettings(Base):
    __tablename__ = "owner_settings"
    __table_args__ = (UniqueConstraint("owner_id", name="uq_owner_settings_owner_id"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    recruiter_company: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    twilio_from_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    sms_global_daily_limit: Mapped[int] = mapped_column(Integer, default=200)
    sms_business_start_hour: Mapped[int] = mapped_column(Integer, default=7)  # 7am local
    sms_business_end_hour: Mapped[int] = mapped_column(Integer, default=19)  # 7pm local

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow, onupdate=_utcnow)


# ------------------------------------------------------------------------------
# SMS Outreach
# ------------------------------------------------------------------------------


class SmsBatchStatus(str, enum.Enum):
    queued = "queued"
    approved = "approved"
    completed = "completed"
    cancelled = "cancelled"


class SmsMessageStatus(str, enum.Enum):
    queued = "queued"
    approved = "approved"
    sending = "sending"
    sent = "sent"
    failed = "failed"


class SmsInboundTag(str, enum.Enum):
    interested = "Interested"
    not_interested = "Not Interested"
    wrong_number = "Wrong Number"
    ask_later = "Ask Later"
    unsubscribe = "Unsubscribe"
    unknown = "Unknown"


class SmsOptOut(Base):
    __tablename__ = "sms_opt_outs"
    __table_args__ = (UniqueConstraint("owner_id", "phone_e164", name="uq_owner_phone_opt_out"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    phone_e164: Mapped[str] = mapped_column(String, index=True)

    reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)


class SmsBatch(Base):
    __tablename__ = "sms_batches"
    __table_args__ = (UniqueConstraint("owner_id", "id", name="uq_owner_sms_batch_id"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)

    status: Mapped[SmsBatchStatus] = mapped_column(
        Enum(SmsBatchStatus, native_enum=False), default=SmsBatchStatus.queued
    )

    requested_count: Mapped[int] = mapped_column(Integer, default=0)
    created_count: Mapped[int] = mapped_column(Integer, default=0)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    job: Mapped["Job"] = relationship()


class SmsOutboundMessage(Base):
    __tablename__ = "sms_outbound_messages"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)
    batch_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("sms_batches.id"), index=True, nullable=True)
    profile_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("profiles.id"), index=True, nullable=True)

    to_phone_e164: Mapped[str] = mapped_column(String, index=True)
    from_phone_e164: Mapped[str] = mapped_column(String, index=True)

    body: Mapped[str] = mapped_column(Text, nullable=False)
    template_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    placeholders_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    status: Mapped[SmsMessageStatus] = mapped_column(
        Enum(SmsMessageStatus, native_enum=False), default=SmsMessageStatus.queued
    )
    twilio_sid: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    job: Mapped["Job"] = relationship()
    batch: Mapped[Optional["SmsBatch"]] = relationship()
    profile: Mapped[Optional["Profile"]] = relationship()


class SmsInboundMessage(Base):
    __tablename__ = "sms_inbound_messages"
    __table_args__ = (UniqueConstraint("owner_id", "twilio_sid", name="uq_owner_twilio_inbound_sid"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("jobs.id"), index=True, nullable=True)

    from_phone_e164: Mapped[str] = mapped_column(String, index=True)
    to_phone_e164: Mapped[str] = mapped_column(String, index=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    twilio_sid: Mapped[str] = mapped_column(String, nullable=False, index=True)

    tag: Mapped[SmsInboundTag] = mapped_column(
        Enum(SmsInboundTag, native_enum=False), default=SmsInboundTag.unknown
    )
    raw_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job: Mapped[Optional["Job"]] = relationship()


# ------------------------------------------------------------------------------
# Chat
# ------------------------------------------------------------------------------


class JobChatMessage(Base):
    __tablename__ = "job_chat_messages"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    role: Mapped[str] = mapped_column(String)  # "user" | "assistant"
    content: Mapped[str] = mapped_column(Text)

    # Structured suggestions (assistant only)
    suggestions_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job: Mapped["Job"] = relationship(back_populates="chat_messages")


# ------------------------------------------------------------------------------
# Title & Location Variants (per job)
# ------------------------------------------------------------------------------


class TitleVariant(Base):
    """A title boolean variant for a job. User can have multiple."""
    __tablename__ = "title_variants"
    __table_args__ = (UniqueConstraint("job_id", "signature", name="uq_job_title_sig"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)

    # e.g. ["doctor", "physician"]
    entities: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    # e.g. ("doctor" OR "physician")
    boolean_text: Mapped[str] = mapped_column(Text, nullable=False)
    # hash of sorted lowercase entities for dedup
    signature: Mapped[str] = mapped_column(String, nullable=False, index=True)

    selected: Mapped[bool] = mapped_column(default=True)  # user can toggle

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job: Mapped["Job"] = relationship(back_populates="title_variants")


class LocationVariant(Base):
    """A location boolean variant for a job. User can have multiple."""
    __tablename__ = "location_variants"
    __table_args__ = (UniqueConstraint("job_id", "signature", name="uq_job_location_sig"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)

    # e.g. ["Seattle", "Washington"]
    entities: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    # e.g. ("Seattle" AND "Washington")
    boolean_text: Mapped[str] = mapped_column(Text, nullable=False)
    # hash of sorted lowercase entities for dedup
    signature: Mapped[str] = mapped_column(String, nullable=False, index=True)

    selected: Mapped[bool] = mapped_column(default=True)  # user can toggle

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job: Mapped["Job"] = relationship(back_populates="location_variants")


# ------------------------------------------------------------------------------
# Boolean Caches (global, entity-level)
# ------------------------------------------------------------------------------


class TitleBooleanCache(Base):
    """Cache: title entities → boolean. Reused across jobs."""
    __tablename__ = "title_boolean_cache"
    __table_args__ = (UniqueConstraint("owner_id", "signature", name="uq_title_cache_sig"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    entities: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    boolean_text: Mapped[str] = mapped_column(Text, nullable=False)
    signature: Mapped[str] = mapped_column(String, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)


class LocationBooleanCache(Base):
    """Cache: location entities → boolean. Reused across jobs."""
    __tablename__ = "location_boolean_cache"
    __table_args__ = (UniqueConstraint("owner_id", "signature", name="uq_location_cache_sig"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    entities: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    boolean_text: Mapped[str] = mapped_column(Text, nullable=False)
    signature: Mapped[str] = mapped_column(String, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)


# ------------------------------------------------------------------------------
# Strategy Run (now = title_variant + location_variant combo)
# ------------------------------------------------------------------------------


class StrategyRun(Base):
    """One scouting run = one title variant + one location variant."""
    __tablename__ = "strategy_runs"
    __table_args__ = (
        UniqueConstraint("job_id", "combo_signature", name="uq_job_combo_signature"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)

    title_variant_id: Mapped[str] = mapped_column(
        String, ForeignKey("title_variants.id"), index=True
    )
    location_variant_id: Mapped[str] = mapped_column(
        String, ForeignKey("location_variants.id"), index=True
    )

    # Composed boolean: (title) AND (location)
    boolean_text: Mapped[str] = mapped_column(Text, nullable=False)

    # Hash of title_variant.signature + location_variant.signature
    combo_signature: Mapped[str] = mapped_column(String, nullable=False, index=True)

    status: Mapped[StrategyRunStatus] = mapped_column(
        Enum(StrategyRunStatus), default=StrategyRunStatus.queued
    )

    pages_total: Mapped[int] = mapped_column(Integer, default=10)
    pages_completed: Mapped[int] = mapped_column(Integer, default=0)

    added_count: Mapped[int] = mapped_column(Integer, default=0)
    dropped_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)

    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    job: Mapped["Job"] = relationship(back_populates="strategy_runs")
    title_variant: Mapped["TitleVariant"] = relationship()
    location_variant: Mapped["LocationVariant"] = relationship()
    dropped_profiles: Mapped[list["DroppedProfile"]] = relationship(back_populates="strategy_run")


# ------------------------------------------------------------------------------
# Profile (global, deduplicated by LinkedIn URL)
# ------------------------------------------------------------------------------


class Profile(Base):
    __tablename__ = "profiles"
    __table_args__ = (
        UniqueConstraint("owner_id", "linkedin_url_canonical", name="uq_owner_linkedin_url"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    linkedin_url_canonical: Mapped[str] = mapped_column(String, nullable=False, index=True)
    linkedin_url_raw: Mapped[str] = mapped_column(String, nullable=False)

    full_name_raw: Mapped[str] = mapped_column(String, nullable=False)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)

    city: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    country: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # AI-extracted job title / role (derived from Google title/snippet).
    # Used for title-only matching; location should not exclude results.
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # IANA timezone derived from state (e.g. "America/Chicago"). Used for business-hour outreach.
    timezone: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    cse_item_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job_profiles: Mapped[list["JobProfile"]] = relationship(back_populates="profile")
    enrichments: Mapped[list["ProfileEnrichment"]] = relationship(back_populates="profile")


class ProfileEnrichment(Base):
    """
    Enrichment results for a profile (e.g., phone lookup).
    Stored separately so we can add it without migrations (create_all creates new tables).
    """

    __tablename__ = "profile_enrichments"
    __table_args__ = (
        UniqueConstraint("owner_id", "profile_id", "provider", name="uq_owner_profile_provider"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)
    profile_id: Mapped[str] = mapped_column(String, ForeignKey("profiles.id"), index=True)

    provider: Mapped[str] = mapped_column(String, nullable=False, default="disabled")
    status: Mapped[EnrichmentStatus] = mapped_column(
        Enum(EnrichmentStatus), default=EnrichmentStatus.queued
    )

    # e.g. ["+1XXXXXXXXXX", "+1YYYYYYYYYY"]
    phone_numbers: Mapped[Optional[list[str]]] = mapped_column(JSONB, nullable=True)
    raw_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)

    profile: Mapped["Profile"] = relationship(back_populates="enrichments")


class JobProfile(Base):
    """Link between job and profile."""
    __tablename__ = "job_profiles"
    __table_args__ = (UniqueConstraint("job_id", "profile_id", name="uq_job_profile"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)
    profile_id: Mapped[str] = mapped_column(String, ForeignKey("profiles.id"), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    job: Mapped["Job"] = relationship(back_populates="job_profiles")
    profile: Mapped["Profile"] = relationship(back_populates="job_profiles")


# ------------------------------------------------------------------------------
# Pinned Job Profiles (job-level, so uploads can be kept even if they don't match)
# ------------------------------------------------------------------------------


class JobProfilePin(Base):
    """
    Job-level pin marker for job_profiles.

    Why this exists:
    - Rebuilds prune job_profiles that don't match selected booleans.
    - Uploaded profiles are user-intent and should be allowed to stay ("pinned uploads").

    Note: We add this as a NEW table so create_all can create it without migrations.
    """

    __tablename__ = "job_profile_pins"
    __table_args__ = (UniqueConstraint("owner_id", "job_id", "profile_id", name="uq_owner_job_profile_pin"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)
    profile_id: Mapped[str] = mapped_column(String, ForeignKey("profiles.id"), index=True)

    # e.g. "upload_csv"
    source: Mapped[str] = mapped_column(String, nullable=False, default="upload_csv")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)


# ------------------------------------------------------------------------------
# Dropped Profile
# ------------------------------------------------------------------------------


class DroppedProfile(Base):
    __tablename__ = "dropped_profiles"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    owner_id: Mapped[str] = mapped_column(String, index=True)

    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id"), index=True)
    strategy_run_id: Mapped[str] = mapped_column(String, ForeignKey("strategy_runs.id"), index=True)

    reason: Mapped[str] = mapped_column(String, nullable=False)

    linkedin_url_raw: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    cse_item_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=_utcnow)

    strategy_run: Mapped["StrategyRun"] = relationship(back_populates="dropped_profiles")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def make_signature(entities: list[str]) -> str:
    """Create a hash signature from a list of entities (case-insensitive, sorted)."""
    normalized = sorted([e.strip().lower() for e in entities])
    return hashlib.sha256("|".join(normalized).encode()).hexdigest()[:16]


def make_combo_signature(title_sig: str, location_sig: str) -> str:
    """Create a combo signature from title and location signatures (legacy)."""
    return hashlib.sha256(f"{title_sig}|{location_sig}".encode()).hexdigest()[:16]


def make_combo_signature_v2(
    title_sig: str,
    location_sig: str,
    title_boolean_text: str,
    location_boolean_text: str,
) -> str:
    """
    Create a combo signature that changes when the boolean text changes.

    This allows re-running a combo after improving/correcting the boolean string
    (e.g., fixing AND/OR grouping), even if the entities signature stays the same.
    """

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    payload = "|".join(
        [
            (title_sig or "").strip(),
            (location_sig or "").strip(),
            _norm(title_boolean_text),
            _norm(location_boolean_text),
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
