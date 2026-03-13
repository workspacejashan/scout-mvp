from __future__ import annotations

import json
import re
import time
from typing import Any, Optional

import httpx

from app.core.config import settings


class CseLlmExtractError(Exception):
    pass


def _call_openai(messages: list[dict], *, temperature: float = 0.0) -> str:
    """Call OpenAI API directly (cheaper than OpenRouter)."""
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise CseLlmExtractError("OPENAI_API_KEY is not set")

    # We do LLM extraction inside strategy runs. A single slow/hung read should not stall the page.
    # Use a generous read timeout and do a small retry on transient failures (timeouts, 429).
    timeout = httpx.Timeout(connect=10.0, read=90.0, write=30.0, pool=10.0)
    retries = 2
    retryable_statuses = {429, 500, 502, 503, 504, 520, 521, 522, 524}

    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "gpt-4o-mini",
                        "messages": messages,
                        "temperature": temperature,
                    },
                )

                # Retry transient responses (rate limit, gateway errors) once.
                if resp.status_code in retryable_statuses and attempt < retries - 1:
                    last_err = CseLlmExtractError(f"HTTP {resp.status_code}: {resp.text}")
                    time.sleep(0.8 * (2**attempt))
                    continue

                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
        except httpx.TimeoutException as e:
            last_err = e
            if attempt < retries - 1:
                continue
            raise CseLlmExtractError(str(e)) from e
        except httpx.HTTPStatusError as e:
            raise CseLlmExtractError(f"HTTP {e.response.status_code}: {e.response.text}") from e
        except Exception as e:  # noqa: BLE001
            raise CseLlmExtractError(str(e)) from e

    # Shouldn't reach, but keep type checkers happy.
    raise CseLlmExtractError(str(last_err) if last_err else "Unknown OpenAI error")


def _parse_json_object(content: str) -> Optional[dict]:
    if not content:
        return None
    s = content.strip()

    # Handle markdown code blocks
    if "```json" in s:
        s = s.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in s:
        s = s.split("```", 1)[1].split("```", 1)[0].strip()

    # If it isn't a raw object, try to extract the first {...} block.
    if not s.startswith("{"):
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            s = s[start : end + 1].strip()

    try:
        obj = json.loads(s)
    except Exception:  # noqa: BLE001
        return None

    if isinstance(obj, dict):
        return obj
    return None


def extract_profiles_from_cse_items(
    items: list[dict],
    *,
    strategy_state: Optional[str] = None,
) -> dict[str, dict[str, str]]:
    """
    LLM fallback extractor for messy Google CSE LinkedIn results.

    Input: raw CSE items (must include link/title/snippet).
    Output: mapping by link:
      { "<link>": {"name": "...", "title": "...", "city": "...", "state": "...", "country": "..."} }

    This is intentionally conservative:
    - If OpenAI is not configured or disabled, returns {}.
    - Call is small/batched; caller should only pass "hard" cases.
    """
    if not settings.CSE_LLM_EXTRACT_ENABLED:
        return {}
    if not settings.OPENAI_API_KEY:
        return {}

    batch_size = max(min(settings.CSE_LLM_EXTRACT_BATCH_SIZE, 25), 1)
    strategy_hint = (strategy_state or "").strip()

    system_prompt = """You extract PERSON NAME, JOB TITLE, and LOCATION from LinkedIn search results.

CRITICAL RULES:
1. **name**: Person's full name ONLY. REMOVE ALL credentials/suffixes:
   - Remove: RN, BSN, MSN, MD, PhD, MBA, LPN, DPT, CCRN, CMSRN, OCN, BC, etc.
   - Example: "Hannah Bass, RN, MSN" → "Hannah Bass"
   - Example: "Austin Hamlett, BSN-RN" → "Austin Hamlett"

2. **title**: Person's job title / role ONLY (e.g., "Certified Surgical First Assistant", "CVOR Surgical First Assistant", "Physician Assistant").
   - Use the snippet/title text to infer the role, but do NOT include:
     - the person's name
     - the company name
     - the location (city/state)
     - dates/tenure
     - credentials suffixes (RN, BSN, etc.)
   - If you cannot infer a role/title, use "".

3. **city**: Real city name ONLY. Look for "City, State" patterns in snippet.
   - NOT job titles (e.g., "Manager of Tele" is NOT a city)
   - NOT company names (e.g., "Denver Health Medical Center" - extract "Denver" from location part)
   - Extract city from "University of X at City" patterns (e.g., "University of North Carolina at Charlotte" → city="Charlotte")
   - If no clear city, use ""

4. **state**: Full US state name (e.g., "Colorado" not "CO", "North Carolina" not "NC")
   - Extract from "City, State" or "City, State, United States" patterns
   - If StrategyState hint is provided and snippet shows city in that state, use StrategyState
   - If no clear state, use ""

5. **country**: Usually "United States" if US location found, otherwise ""

Return ONLY valid JSON (no markdown, no explanation):
{"profiles":[{"link":"...","name":"...","title":"...","city":"...","state":"...","country":"..."}]}"""

    out: dict[str, dict[str, str]] = {}
    items2 = list(items or [])
    for offset in range(0, len(items2), batch_size):
        batch = items2[offset : offset + batch_size]

        payload: list[dict[str, str]] = []
        for item in batch:
            link = str((item or {}).get("link") or "").strip()
            if not link:
                continue
            title = str((item or {}).get("title") or "").replace("\n", " ").strip()
            snippet = str((item or {}).get("snippet") or "").replace("\n", " ").strip()
            payload.append(
                {
                    "link": link,
                    "title": title[:240],
                    "snippet": snippet[:600],
                }
            )

        if not payload:
            continue

        user_prompt = json.dumps(
            {
                "StrategyState": strategy_hint,
                "items": payload,
            },
            ensure_ascii=False,
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        content = _call_openai(messages, temperature=0.0)
        obj = _parse_json_object(content) or {}

        raw_profiles = obj.get("profiles")
        if not isinstance(raw_profiles, list):
            continue

        for p in raw_profiles:
            if not isinstance(p, dict):
                continue
            link = str(p.get("link") or "").strip()
            if not link:
                continue
            name = str(p.get("name") or "").strip()
            title = str(p.get("title") or "").strip()
            city = str(p.get("city") or "").strip()
            state = str(p.get("state") or "").strip()
            country = str(p.get("country") or "").strip()

            # Keep output stable; callers validate further.
            out[link] = {"name": name, "title": title, "city": city, "state": state, "country": country}

    return out

