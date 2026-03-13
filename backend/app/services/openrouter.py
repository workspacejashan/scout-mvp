from __future__ import annotations

import json
import os
import re
from typing import Optional

import httpx

from app.core.config import settings


class OpenRouterError(Exception):
    pass


def _call_openai(messages: list[dict], temperature: float = 0.3) -> str:
    """Call OpenAI API directly (preferred for Copilot: cheaper + avoids OpenRouter flakiness)."""
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise OpenRouterError("OPENAI_API_KEY is not set")

    # Keep aligned with our CSE extraction choice (fast/cheap, strong enough for structured JSON).
    model = "gpt-4o-mini"

    try:
        with httpx.Client(timeout=45) as client:
            resp = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
    except httpx.HTTPStatusError as e:
        raise OpenRouterError(f"HTTP {e.response.status_code}: {e.response.text}") from e
    except Exception as e:
        raise OpenRouterError(str(e)) from e


def _call_openrouter(messages: list[dict], temperature: float = 0.3) -> str:
    """Call OpenRouter API and return assistant message content."""
    api_key = settings.OPENROUTER_API_KEY
    if not api_key:
        raise OpenRouterError("OPENROUTER_API_KEY is not set")

    model = settings.OPENROUTER_MODEL or "openai/gpt-4o-mini"

    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
    except httpx.HTTPStatusError as e:
        raise OpenRouterError(f"HTTP {e.response.status_code}: {e.response.text}") from e
    except Exception as e:
        raise OpenRouterError(str(e)) from e


def generate_suggestions(goal_text: str, user_message: str) -> dict:
    """
    Generate structured title and location suggestions.
    
    Returns:
        {
            "title_suggestions": [
                {"entities": ["RRT", "registered respiratory therapist", "NICU", "neonatal intensive care"], "boolean": "((\"RRT\" OR \"registered respiratory therapist\") AND (\"NICU\" OR \"neonatal intensive care\"))"},
                ...
            ],
            "location_suggestions": [
                {"entities": ["Phoenix", "Arizona"], "boolean": "(\"Phoenix\" AND \"Arizona\")"},
                ...
            ],
            "message": "Found options for you"
        }
    """
    system_prompt = """You are a LinkedIn boolean search assistant. Given a user's goal, extract ENTITIES and generate boolean search strings.

CRITICAL RULES FOR TITLES:
1. EXTRACT each distinct concept as a SEPARATE ENTITY. Examples:
   - "RN ICU" → Entity1: RN, Entity2: ICU (2 entities)
   - "RRT NICU" → Entity1: RRT, Entity2: NICU (2 entities)
   - "Software Engineer" → Entity1: Software Engineer (1 entity)
   - "CT tech" → Entity1: CT tech (1 entity)

2. IMPORTANT: KEEP COMPOUND ROLE PHRASES INTACT.
   - If the user writes a role as a natural phrase like "psychiatric physician", "family medicine physician",
     "emergency medicine physician", "hospitalist physician", treat it as ONE entity phrase (do not split into
     generic words like "physician").

3. IMPORTANT: DISTINGUISH REQUIRED CONCEPTS vs ALTERNATIVES (synonyms).
   - If the user writes alternatives like "X / Y" or "X or Y" (meaning either title is acceptable),
     treat X and Y as the SAME ENTITY and combine all synonyms in ONE OR group.
   - If the user writes required concepts like "RN ICU" or "RN with ICU experience",
     keep them as SEPARATE ENTITIES and combine entity groups with AND.

4. For EACH entity, generate synonyms and wrap them in OR (ONLY if they are true synonyms of that entity):
   - RN → (\\"RN\\" OR \\"Registered Nurse\\" OR \\"R.N.\\")
   - ICU → (\\"ICU\\" OR \\"Intensive Care\\" OR \\"Intensive-Care\\" OR \\"CCU\\" OR \\"Critical Care\\")
   - RRT → (\\"RRT\\" OR \\"Registered Respiratory Therapist\\" OR \\"Respiratory Therapist\\")
   - NICU → (\\"NICU\\" OR \\"Neonatal Intensive Care\\" OR \\"Neonatal ICU\\")

5. COMBINE all entity groups with AND (strict by default). Never OR between entities.
   - "RN ICU" → ((\\"RN\\" OR \\"Registered Nurse\\") AND (\\"ICU\\" OR \\"Intensive Care\\" OR \\"Critical Care\\"))
   - "RRT NICU" → ((\\"RRT\\" OR \\"Registered Respiratory Therapist\\") AND (\\"NICU\\" OR \\"Neonatal Intensive Care\\"))

6. The "entities" array should contain ALL terms (from all entity groups flattened).

WRONG (mixing entities):
User: "RRT NICU"
Bad: (\\"RRT\\" OR \\"NICU Respiratory Therapist\\")  ← WRONG: mixes entities

CORRECT (entities separated):
User: "RRT NICU"
Good: ((\\"RRT\\" OR \\"Registered Respiratory Therapist\\") AND (\\"NICU\\" OR \\"Neonatal Intensive Care\\"))

CORRECT (alternatives merged into ONE entity):
User: "OR Tech / Surgical Tech"
Good: (\\"OR Tech\\" OR \\"Operating Room Technician\\" OR \\"Surgical Tech\\" OR \\"Surgical Technician\\")
Bad: ((\\"OR Tech\\" OR \\"Operating Room Technician\\") AND (\\\"Surgical Tech\\\" OR \\\"Surgical Technician\\\")) ← too strict

LOCATION RULES:
1. City + State combined with AND: (\\"Phoenix\\" AND \\"Arizona\\")
2. If user mentions multiple cities, create SEPARATE location entries for each city.
3. Use full state names (California not CA).
4. If user asks for "nearby" / "around" / "big cities" / radius (miles/km) / zip-code based rules:
   - Use best-effort geographic reasoning (no external tools).
   - Prefer the largest / most relevant cities that satisfy the rule.
   - If a count is requested, return up to that many; otherwise default to 20 max.
5. If the user message contains EXISTING_LOCATIONS_JSON, do NOT repeat any of those city+state pairs.
   If NEED_NEW_LOCATIONS is provided, return at most that many location_suggestions.

OUTPUT FORMAT (valid JSON only, no markdown):
{"title_suggestions":[{"entities":["RRT","Registered Respiratory Therapist","NICU","Neonatal Intensive Care"],"boolean":"((\\"RRT\\" OR \\"Registered Respiratory Therapist\\") AND (\\"NICU\\" OR \\"Neonatal Intensive Care\\"))"}],"location_suggestions":[{"entities":["Fayetteville","Georgia"],"boolean":"(\\"Fayetteville\\" AND \\"Georgia\\")"}],"message":"Found options for you"}

IMPORTANT:
- Escape all quotes with backslash inside JSON strings
- Generate 1 title suggestion per distinct role the user wants
- Keep message brief"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Goal: {goal_text}\n\nUser message: {user_message}"},
    ]

    # Prefer OpenAI direct for reliability + cost.
    temperature = 0.3
    content: str = ""
    try:
        if settings.OPENAI_API_KEY:
            content = _call_openai(messages, temperature=temperature)
    except Exception:
        content = ""

    if not content:
        content = _call_openrouter(messages, temperature=temperature)
    
    # Parse JSON from response
    try:
        # Handle potential markdown code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        
        # Try to find JSON object in content
        content = content.strip()
        if not content.startswith("{"):
            # Try to find first { and last }
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end != -1:
                content = content[start : end + 1]
        
        result = json.loads(content)
        
        # Validate structure
        if "title_suggestions" not in result:
            result["title_suggestions"] = []
        if "location_suggestions" not in result:
            result["location_suggestions"] = []
        if "message" not in result:
            result["message"] = "Here are some search options."
        
        # Lightweight guardrail: ensure common compound titles are grouped correctly
        result = _ensure_compound_nursing_title(result, goal_text=goal_text, user_message=user_message)
        result = _ensure_psychiatric_physician_title(result, goal_text=goal_text, user_message=user_message)
        return result
    except json.JSONDecodeError as e:
        # If parsing fails, return the raw content as message
        return {
            "title_suggestions": [],
            "location_suggestions": [],
            "message": f"I understood your request but had trouble formatting. Please try again. (parse error: {e})",
        }


_ENTITY_SYNONYMS = {
    # Roles
    "rn": ["RN", "Registered Nurse", "R.N."],
    "rrt": ["RRT", "Registered Respiratory Therapist", "Respiratory Therapist", "R.R.T."],
    "lpn": ["LPN", "Licensed Practical Nurse", "L.P.N."],
    "cna": ["CNA", "Certified Nursing Assistant", "C.N.A."],
    "ct tech": ["CT Tech", "CT Technologist", "CAT Scan Technologist", "Computed Tomography Technologist"],
    "mri tech": ["MRI Tech", "MRI Technologist", "Magnetic Resonance Imaging Technologist"],
    "cath lab tech": ["Cath Lab Tech", "Cath Lab Technologist", "Cardiac Catheterization Technologist"],
    "nuclear medicine tech": ["Nuclear Medicine Tech", "Nuclear Medicine Technologist", "Nuclear Med Tech"],
    "electrophysiology tech": ["Electrophysiology Tech", "EP Tech", "Electrophysiology Technologist"],
    "surgical first assistant": ["Surgical First Assistant", "CSFA", "Certified Surgical First Assistant", "First Assist"],
    "cvor": ["CVOR", "Cardiovascular Operating Room"],
    # Specialties / Units
    "icu": ["ICU", "Intensive Care", "Intensive Care Unit", "Intensive-Care", "CCU", "Critical Care", "Critical-Care"],
    "nicu": ["NICU", "Neonatal Intensive Care", "Neonatal ICU", "Neonatal Intensive Care Unit"],
    "picu": ["PICU", "Pediatric Intensive Care", "Pediatric ICU"],
    "tele": ["Tele", "Telemetry", "Cardiac Telemetry"],
    "er": ["ER", "Emergency Room", "Emergency Department", "ED"],
    "or": ["OR", "Operating Room", "Surgery"],
    "l&d": ["L&D", "Labor and Delivery", "Labor & Delivery"],
    "med surg": ["Med Surg", "Medical Surgical", "Med-Surg"],
}


def _ensure_psychiatric_physician_title(result: dict, *, goal_text: str, user_message: str) -> dict:
    """
    Guardrail: "psychiatric physician" / "psychiatrist physician" should not become
    ("psychiatrist" OR "physician"). That's too broad.
    """
    txt = f"{goal_text}\n{user_message}".lower()
    if "psychiatric physician" not in txt and "psychiatrist physician" not in txt:
        return result

    title_suggestions = result.get("title_suggestions") or []
    if not isinstance(title_suggestions, list):
        return result

    fixed = []
    for s in title_suggestions:
        if not isinstance(s, dict):
            fixed.append(s)
            continue
        b = str(s.get("boolean") or "")
        bl = b.lower()
        # Detect the bad pattern: psychiatrist OR physician (no AND between them)
        if ("psychiatrist" in bl) and ("physician" in bl) and (" or " in bl) and (" and " not in bl):
            # Replace with a single-entity role phrase (plus psychiatrist synonym).
            s2 = dict(s)
            s2["entities"] = ["Psychiatric Physician", "Psychiatrist"]
            s2["boolean"] = '("Psychiatric Physician" OR "Psychiatrist")'
            fixed.append(s2)
        else:
            fixed.append(s)

    result["title_suggestions"] = fixed
    return result


def _detect_entities(text: str) -> list[str]:
    """
    Detect known entities in free-form text.

    This is intentionally conservative:
    - Multi-word and punctuated terms are detected via token-based regex (\\W+ between tokens).
    - Very ambiguous abbreviations like "OR" are only detected if written in UPPERCASE ("OR"),
      otherwise we'd match normal English "or" too often.
    """

    def _term_tokens(term: str) -> list[str]:
        return re.findall(r"[a-z0-9]+", (term or "").lower())

    # Abbreviations that collide with common English words. Only match UPPERCASE in raw text.
    upper_only_abbrevs = {"OR", "ER", "ED"}

    found: list[str] = []
    for key, terms in _ENTITY_SYNONYMS.items():
        # Don't auto-detect the raw key itself for extremely ambiguous keys (e.g., "or").
        candidate_terms: list[str] = list(terms)
        if key not in {"or", "er", "ed"}:
            candidate_terms.append(key)

        matched = False
        for term in candidate_terms:
            if term in upper_only_abbrevs:
                if re.search(r"\b" + re.escape(term) + r"\b", text):
                    matched = True
                    break
                continue

            tokens = _term_tokens(term)
            if not tokens:
                continue
            pattern = r"\b" + r"\W+".join(map(re.escape, tokens)) + r"\b"
            if re.search(pattern, text, flags=re.IGNORECASE):
                matched = True
                break

        if matched:
            found.append(key)

    return found


def _build_compound_boolean(entities: list[str]) -> tuple[list[str], str]:
    """Build compound boolean from detected entities."""
    if not entities:
        return [], ""
    
    all_terms = []
    groups = []
    for ent in entities:
        terms = _ENTITY_SYNONYMS.get(ent, [ent])
        all_terms.extend(terms)
        group = "(" + " OR ".join([f'"{t}"' for t in terms]) + ")"
        groups.append(group)
    
    if len(groups) == 1:
        boolean = groups[0]
    else:
        boolean = "(" + " AND ".join(groups) + ")"
    
    return all_terms, boolean


def _ensure_compound_nursing_title(result: dict, *, goal_text: str, user_message: str) -> dict:
    """
    Guardrail: If user asks for compound title (e.g., RRT NICU), ensure entities
    are combined with AND, not mixed into one OR group.
    """
    try:
        hay = f"{goal_text} {user_message}"
        detected = _detect_entities(hay)
        
        # Only apply guardrail if 2+ entities detected
        if len(detected) < 2:
            return result
        
        title_suggestions = result.get("title_suggestions") or []
        
        # Check if any existing suggestion already has proper AND structure
        for s in title_suggestions:
            b = str((s or {}).get("boolean") or "").lower()
            # If it has AND and mentions at least 2 detected entities, assume it's correct
            if " and " in b:
                matches = sum(1 for ent in detected if any(syn.lower() in b for syn in _ENTITY_SYNONYMS.get(ent, [])))
                if matches >= 2:
                    return result
        
        # Build the correct compound boolean
        all_terms, boolean = _build_compound_boolean(detected)
        if not boolean:
            return result
        
        enforced = {
            "entities": all_terms,
            "boolean": boolean,
        }
        
        # Prepend the correct suggestion
        result["title_suggestions"] = [enforced] + title_suggestions
        return result
    except Exception:
        return result


def refine_title_boolean(entities: list[str]) -> str:
    """Generate a boolean for title entities."""
    if not entities:
        return ""
    
    system_prompt = """Generate a LinkedIn boolean search string for job titles.
Use OR to combine synonyms and related titles.
Output ONLY the boolean string, nothing else.
Example input: ["doctor", "physician"]
Example output: ("doctor" OR "physician" OR "MD" OR "medical doctor")"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Entities: {entities}"},
    ]
    
    return _call_openrouter(messages).strip()


def refine_location_boolean(entities: list[str]) -> str:
    """Generate a boolean for location entities."""
    if not entities:
        return ""
    
    system_prompt = """Generate a LinkedIn boolean search string for location.
City and state should be combined with AND.
Output ONLY the boolean string, nothing else.
Example input: ["Seattle", "WA"]
Example output: ("Seattle" AND "Washington")"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Entities: {entities}"},
    ]
    
    return _call_openrouter(messages).strip()
