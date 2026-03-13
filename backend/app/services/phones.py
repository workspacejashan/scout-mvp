from __future__ import annotations

import re
from typing import Optional


def normalize_us_phone_e164(raw: str) -> Optional[str]:
    """
    Normalize to US-only E.164.
    - Accept 10-digit NANP -> +1XXXXXXXXXX
    - Accept 11-digit starting with 1 -> +1XXXXXXXXXX
    Reject everything else.
    """
    s = (raw or "").strip()
    if not s:
        return None

    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def looks_like_opt_out(body: str) -> bool:
    s = (body or "").strip().lower()
    if not s:
        return False
    # Twilio standard STOP keywords (+ common variants).
    return s in {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}


def tag_inbound_heuristic(body: str) -> str:
    """
    Minimal v1 tagging. We’ll add LLM tagging later.
    Returns one of:
    Interested | Not Interested | Wrong Number | Ask Later | Unsubscribe | Unknown
    """
    s = (body or "").strip().lower()
    if not s:
        return "Unknown"

    if looks_like_opt_out(s):
        return "Unsubscribe"

    if any(p in s for p in ["wrong number", "wrong #", "not me", "not the right", "you got the wrong"]):
        return "Wrong Number"

    if any(p in s for p in ["not interested", "no thanks", "no thank", "not looking", "stop texting"]):
        return "Not Interested"

    if any(p in s for p in ["later", "not now", "next week", "next month", "follow up", "reach out"]):
        return "Ask Later"

    if any(p in s for p in ["interested", "yes", "sure", "tell me more", "sounds good"]):
        return "Interested"

    return "Unknown"

