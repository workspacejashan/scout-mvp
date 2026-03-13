from __future__ import annotations

import os
from dotenv import load_dotenv


load_dotenv()


def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


class Settings:
    # Runtime environment
    # - development: permissive defaults for local dev
    # - production: fail-closed unless explicitly configured
    ENV: str = (os.getenv("ENV") or os.getenv("APP_ENV") or "development").strip().lower()

    # Admin auth: when set (or when ENV=production), all /api endpoints require a bearer token.
    # This repo is single-tenant (APP_DEFAULT_OWNER_ID) so this is the minimum viable protection for going public.
    ADMIN_API_TOKEN: str = os.getenv("ADMIN_API_TOKEN", "")

    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+psycopg2://scout:scout@127.0.0.1:5433/scout"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6380/0")

    APP_DEFAULT_OWNER_ID: str = os.getenv("APP_DEFAULT_OWNER_ID", "local-owner")

    GOOGLE_CSE_API_KEY: str = os.getenv("GOOGLE_CSE_API_KEY", "")
    GOOGLE_CSE_CX: str = os.getenv("GOOGLE_CSE_CX", "")

    OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
    OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

    # LLM fallback for messy CSE title/snippet parsing (name/location cleanup)
    CSE_LLM_EXTRACT_ENABLED: bool = _get_bool("CSE_LLM_EXTRACT_ENABLED", True)
    CSE_LLM_EXTRACT_BATCH_SIZE: int = _get_int("CSE_LLM_EXTRACT_BATCH_SIZE", 8)

    # Enrichment (next step after scout)
    ENRICH_PROVIDER: str = os.getenv("ENRICH_PROVIDER", "disabled")
    ENFORMIONGO_API_KEY: str = os.getenv("ENFORMIONGO_API_KEY", "")
    SCRAPEDO_API_KEY: str = os.getenv("SCRAPEDO_API_KEY", "")
    # Scrape.do options for Google SERP (reduce blocks/consent pages)
    SCRAPEDO_SERP_SUPER: bool = _get_bool("SCRAPEDO_SERP_SUPER", True)
    SCRAPEDO_SERP_RENDER_ALWAYS: bool = _get_bool("SCRAPEDO_SERP_RENDER_ALWAYS", True)
    SCRAPEDO_SERP_RENDER_ON_BLOCK: bool = _get_bool("SCRAPEDO_SERP_RENDER_ON_BLOCK", True)
    SCRAPEDO_SERP_CUSTOM_WAIT_MS: int = _get_int("SCRAPEDO_SERP_CUSTOM_WAIT_MS", 2000)
    SCRAPEDO_SERP_BLOCK_RESOURCES: bool = _get_bool("SCRAPEDO_SERP_BLOCK_RESOURCES", False)
    SCRAPEDO_SERP_WAIT_SELECTOR: str = os.getenv("SCRAPEDO_SERP_WAIT_SELECTOR", "div#search")
    ENRICH_CONCURRENCY: int = _get_int("ENRICH_CONCURRENCY", 25)
    ENRICH_RETRY_BACKOFF_SECONDS: float = float(os.getenv("ENRICH_RETRY_BACKOFF_SECONDS", "2.0"))

    CSE_RESULTS_PER_PAGE: int = _get_int("CSE_RESULTS_PER_PAGE", 10)
    CSE_MAX_RESULTS: int = _get_int("CSE_MAX_RESULTS", 100)
    CSE_PAGE_CONCURRENCY: int = _get_int("CSE_PAGE_CONCURRENCY", 5)
    CSE_PAGE_RETRIES: int = _get_int("CSE_PAGE_RETRIES", 2)

    # Twilio (SMS outreach)
    TWILIO_ACCOUNT_SID: str = os.getenv("TWILIO_ACCOUNT_SID", "")
    TWILIO_AUTH_TOKEN: str = os.getenv("TWILIO_AUTH_TOKEN", "")
    # Optional default "from" number (E.164). DB owner_settings can override.
    TWILIO_FROM_NUMBER: str = os.getenv("TWILIO_FROM_NUMBER", "")

    # Outreach defaults (can be overridden per job / owner in DB)
    SMS_COOLDOWN_DAYS: int = _get_int("SMS_COOLDOWN_DAYS", 14)
    SMS_GLOBAL_DAILY_LIMIT: int = _get_int("SMS_GLOBAL_DAILY_LIMIT", 200)
    SMS_JOB_DAILY_LIMIT: int = _get_int("SMS_JOB_DAILY_LIMIT", 50)

    # Safety kill-switch: keep OFF by default in dev until you explicitly enable sending.
    SMS_SENDING_ENABLED: bool = _get_bool("SMS_SENDING_ENABLED", False)

    # Universal unlock code (shared code that grants full access)
    UNIVERSAL_UNLOCK_CODE: str = os.getenv("UNIVERSAL_UNLOCK_CODE", "")

    # Stripe
    STRIPE_SECRET_KEY: str = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_PRICE_ID: str = os.getenv("STRIPE_PRICE_ID", "")

    # Frontend URL for Stripe redirects
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

    # Free tier limits
    FREE_TIER_MAX_JOBS: int = _get_int("FREE_TIER_MAX_JOBS", 3)


settings = Settings()

