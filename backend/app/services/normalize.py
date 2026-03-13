from __future__ import annotations

import re
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse


_TENURE_WORDS = ("year", "years", "month", "months")

# Valid US state abbreviations
US_STATE_ABBREVIATIONS = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
})

# Full state name -> abbreviation (lowercase keys for matching)
US_STATE_NAME_TO_ABBR: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "puerto rico": "PR", "virgin islands": "VI", "guam": "GU",
    "american samoa": "AS", "northern mariana islands": "MP",
}


def _looks_like_tenure(s: str) -> bool:
    """Reject snippets like '6 years 1 month' or 'Present 2 years 5 months'."""
    if not s:
        return False
    lower = s.lower()
    if any(w in lower for w in _TENURE_WORDS) and any(ch.isdigit() for ch in lower):
        return True
    # e.g. "Present 2 years 5 months"
    if "present" in lower and any(w in lower for w in _TENURE_WORDS):
        return True
    return False


def _strip_prefixes(s: str) -> str:
    # Common label-ish prefixes that appear in snippets
    s2 = (s or "").strip()
    s2 = re.sub(r"^\s*(location|based in|lives in)\s*[:\-]\s*", "", s2, flags=re.IGNORECASE)
    return s2.strip()


def _normalize_city_text(city: str) -> str:
    """Normalize city strings (collapse whitespace, strip punctuation)."""
    c = re.sub(r"\s+", " ", (city or "").strip())
    c = c.strip(" ,.-")
    return c

_NAME_PREFIXES = {"dr", "dr.", "mr", "mr.", "ms", "ms.", "mrs", "mrs."}
_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}
_NAME_CREDENTIALS = {"md", "md.", "phd", "phd.", "dds", "dvm", "esq", "cpa", "mba"}


def is_linkedin_in_url(url: str) -> bool:
    try:
        u = urlparse(url)
    except Exception:  # noqa: BLE001
        return False
    host = (u.netloc or "").lower()
    if "linkedin.com" not in host:
        return False
    return u.path.startswith("/in/")


def normalize_linkedin_url(url: str) -> str:
    """
    Canonicalize for global dedupe:
    - strip query/fragment
    - remove trailing slash
    - force https://www.linkedin.com
    - lower-case the /in/<slug> path
    """
    u = urlparse(url)
    path = (u.path or "").rstrip("/")
    # Only canonicalize /in/ paths; return normalized-ish for everything else.
    if path.startswith("/in/"):
        path = path.lower()
    return urlunparse(("https", "www.linkedin.com", path, "", "", ""))


def extract_name_from_title(title: str) -> Optional[str]:
    """
    Title format examples:
    - "SSANJAY JOSHI - Royal LePage Signature Realty, Toronto"
    - "Sébastien Marcel – IEEE Fellow, IAPR ..."
    """
    if not title:
        return None

    parts = None
    if " - " in title:
        parts = title.split(" - ", 1)
    elif " – " in title:
        parts = title.split(" – ", 1)

    if not parts:
        return None

    raw = parts[0].strip()
    cleaned = clean_person_name(raw)
    if not cleaned:
        return None
    if len(cleaned.split()) < 2:
        return None
    if any(ch.isdigit() for ch in cleaned):
        return None
    return cleaned


def clean_person_name(name: str) -> Optional[str]:
    if not name:
        return None
    s = re.sub(r"\s+", " ", name.strip())
    # Remove trailing punctuation commonly used in titles
    s = s.strip(" -–|,")
    if not s:
        return None

    tokens = s.split(" ")
    # strip prefixes
    while tokens and tokens[0].lower() in _NAME_PREFIXES:
        tokens = tokens[1:]
    # strip suffixes / creds
    while tokens and tokens[-1].lower().strip(".") in (_NAME_SUFFIXES | _NAME_CREDENTIALS):
        tokens = tokens[:-1]

    s2 = " ".join(tokens).strip()
    s2 = re.sub(r"\s+", " ", s2)
    return s2 or None


def split_first_last(full_name: str) -> tuple[str, str]:
    parts = [p for p in full_name.strip().split(" ") if p]
    if len(parts) < 2:
        return full_name.strip(), ""
    first = " ".join(parts[:-1]).strip()
    last = parts[-1].strip()
    return first, last


def _extract_city_state_validated(text: str) -> Optional[Tuple[str, str]]:
    """
    Deterministic city/state extraction with US state validation.
    Returns (city, state_abbrev) or None.
    """
    normalized = (text or "").replace("\n", " ").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    if not normalized:
        return None

    def _bad_city_token(token: str) -> bool:
        t = (token or "").strip().lower().strip(".")
        if not t:
            return True
        # Common non-city/address tokens that show up in snippets and produce false hits
        return t in {
            "ave",
            "avenue",
            "st",
            "street",
            "rd",
            "road",
            "blvd",
            "boulevard",
            "dr",
            "drive",
            "ln",
            "lane",
            "hwy",
            "highway",
            "suite",
            "ste",
            "floor",
            "fl",
            "unit",
            "apt",
            "nw",
            "ne",
            "se",
            "sw",
            "pkwy",
            "parkway",
            "ctr",
            "center",
        }

    # Pattern 1: "City, ST" (e.g. "Austin, TX")
    # City tokens must be TitleCased words; state is a validated US abbreviation.
    city_state_match = re.search(
        r"\b([A-Z][a-zA-Z]+(?:[\s\-][A-Z][a-zA-Z]+)*)\s*,\s*([A-Za-z]{2})\b\.?",
        normalized,
    )
    if city_state_match:
        city = city_state_match.group(1).strip()
        state = city_state_match.group(2).upper()
        if state in US_STATE_ABBREVIATIONS:
            # Validate city doesn't look like garbage
            if _looks_like_tenure(city) or any(ch.isdigit() for ch in city):
                return None
            # Reject long "phrases" masquerading as city
            words = [w for w in re.split(r"\s+", city) if w]
            if len(words) > 5 or len(city) > 40:
                return None
            if any(_bad_city_token(w) for w in words):
                return None
            return city, state

    # Pattern 2: Full state name (e.g. "Austin, Texas" or "Austin Texas")
    # Build a regex from all known state names
    state_names_pattern = "|".join(
        re.escape(name) for name in sorted(US_STATE_NAME_TO_ABBR.keys(), key=len, reverse=True)
    )
    state_name_regex = re.compile(
        rf"\b({state_names_pattern})\b",
        flags=re.IGNORECASE,
    )
    # Prefer the *last* state-name occurrence to avoid matching state names inside city names
    # e.g. "Virginia Beach, Virginia" should map to city="Virginia Beach", state="VA"
    matches = list(state_name_regex.finditer(normalized))
    for state_name_match in reversed(matches):
        state_name_key = state_name_match.group(1).lower()
        state_abbrev = US_STATE_NAME_TO_ABBR.get(state_name_key)
        if not state_abbrev:
            continue

        # Reject possessive or other non-location uses like "Colorado's ..."
        after_idx = state_name_match.end()
        if after_idx < len(normalized):
            nxt = normalized[after_idx : after_idx + 1]
            if nxt in {"'", "’"}:
                continue

        before = normalized[: state_name_match.start()]
        # Only accept state-name matches that look like an actual location, i.e. comma-delimited:
        # "City, Colorado" or "City, Colorado, United States"
        # This avoids false positives like "The Colorado Mountain Club" where "Colorado" is not a location.
        if not before.rstrip().endswith(","):
            continue

        # Look for "City," or "City " pattern before state, requiring TitleCased words
        city_match = re.search(
            r"\b([A-Z][a-zA-Z]+(?:[\s\-][A-Z][a-zA-Z]+)*)\s*,?\s*$",
            before,
        )
        if city_match:
            city = city_match.group(1).strip()
            if _looks_like_tenure(city) or any(ch.isdigit() for ch in city):
                continue
            words = [w for w in re.split(r"\s+", city) if w]
            if len(words) > 5 or len(city) > 40:
                continue
            if any(_bad_city_token(w) for w in words):
                continue
            return city, state_abbrev

    return None


def _clean_metro_area(text: str) -> str:
    """
    Strip 'Greater', 'Metro', 'Area' prefixes/suffixes to extract the core city.
    E.g., "Greater Boston Area" -> "Boston"
    """
    s = (text or "").strip()
    # Remove leading "Greater"
    s = re.sub(r"^greater\s+", "", s, flags=re.IGNORECASE)
    # Remove trailing "Area", "Metro Area", "Metropolitan Area"
    s = re.sub(r"\s+(metropolitan\s+)?area$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+metro$", "", s, flags=re.IGNORECASE)
    return s.strip()


def extract_location_city_state(
    snippet: str,
    strategy_state: Optional[str] = None,
) -> Optional[Tuple[str, str, Optional[str]]]:
    """
    Extract city and state from CSE snippet with proper US state validation.
    Returns (city, state_abbrev, country) or None.
    
    Uses deterministic heuristics:
    1. Look for "City, ST" with valid 2-letter US state abbreviation
    2. Look for "City, StateName" with full state name mapping
    3. Handle "Greater X Area" patterns by extracting X
    4. If only city found and strategy_state provided, use that
    """
    if not snippet:
        return None

    # Normalize the text
    text = (snippet or "").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    # Split on common LinkedIn delimiters
    segments = [s.strip() for s in re.split(r"[·•|]", text) if s and s.strip()]
    if not segments:
        segments = [text]

    # Also split on periods (but be careful not to break "St." etc.)
    expanded_segments: list[str] = []
    for seg in segments:
        # Split on ". " (period followed by space) to handle "6 years. Miami, FL"
        parts = re.split(r"\.\s+", seg)
        expanded_segments.extend(p.strip() for p in parts if p.strip())
    segments = expanded_segments

    # Add the full text as a fallback
    if text not in segments:
        segments.append(text)

    for seg in segments:
        seg2 = _strip_prefixes(seg)
        if not seg2:
            continue

        # Skip pure tenure segments
        if _looks_like_tenure(seg2):
            continue

        # First, try direct extraction with validation
        result = _extract_city_state_validated(seg2)
        if result:
            city, state_abbrev = result
            # Check if there's a country after (e.g., "Miami, FL, United States")
            country = None
            if "united states" in seg2.lower():
                country = "United States"
            return _normalize_city_text(city), state_abbrev, country

        # Handle "Greater X Area" patterns - extract city and look for state nearby
        lower = seg2.lower()
        if "greater" in lower or "metro" in lower or "area" in lower:
            cleaned = _clean_metro_area(seg2)
            if cleaned and cleaned != seg2:
                # Try to extract from the cleaned version
                result2 = _extract_city_state_validated(cleaned)
                if result2:
                    city, state_abbrev = result2
                    return _normalize_city_text(city), state_abbrev, None
                
                # If just a city name remains, use strategy_state if available
                if strategy_state and not _looks_like_tenure(cleaned) and not any(ch.isdigit() for ch in cleaned):
                    # Validate strategy_state is a real state
                    st_abbrev = strategy_state.upper() if len(strategy_state) == 2 else US_STATE_NAME_TO_ABBR.get(strategy_state.lower())
                    if st_abbrev and st_abbrev in US_STATE_ABBREVIATIONS:
                        return _normalize_city_text(cleaned), st_abbrev, None

    # Final fallback: if strategy_state provided, look for any plausible city name
    if strategy_state:
        st_abbrev = strategy_state.upper() if len(strategy_state) == 2 else US_STATE_NAME_TO_ABBR.get(strategy_state.lower())
        if st_abbrev and st_abbrev in US_STATE_ABBREVIATIONS:
            for seg in segments:
                seg2 = _strip_prefixes(seg).strip()
                if not seg2 or _looks_like_tenure(seg2):
                    continue
                # Accept short, clean city-like strings
                city_candidate = _clean_metro_area(seg2).strip().strip(",.")
                lc = city_candidate.lower()
                # Reject obvious non-city segments
                if (
                    ":" in city_candidate
                    or "http://" in lc
                    or "https://" in lc
                    or "www." in lc
                    or "@" in city_candidate
                    or "/" in city_candidate
                ):
                    continue
                # Reject common section labels that appear in snippets
                if lc.startswith(("experience", "education", "skills", "about", "more about")):
                    continue
                # Require city to be only letters/spaces/hyphen/apostrophe/dot
                if not re.fullmatch(r"[A-Za-z][A-Za-z .'\-]{1,60}", city_candidate):
                    continue
                # Require TitleCase-like tokens (avoid long sentence fragments)
                tokens = [t for t in re.split(r"\s+", city_candidate) if t]
                if len(tokens) > 4:
                    continue
                if any(len(t) > 2 and t[0].islower() for t in tokens):
                    continue
                if (
                    city_candidate
                    and len(city_candidate) < 50
                    and not any(ch.isdigit() for ch in city_candidate)
                    and not _looks_like_tenure(city_candidate)
                    and len(city_candidate.split()) <= 4
                ):
                    return _normalize_city_text(city_candidate), st_abbrev, None

    return None

