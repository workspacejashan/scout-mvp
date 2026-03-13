from __future__ import annotations

import re
from typing import Optional

# Most-populated timezone for multi-timezone states.
# Stored as IANA timezone names.
_STATE_ABBREV_TO_IANA_TZ: dict[str, str] = {
    "AL": "America/Chicago",
    "AK": "America/Anchorage",
    "AZ": "America/Phoenix",
    "AR": "America/Chicago",
    "CA": "America/Los_Angeles",
    "CO": "America/Denver",
    "CT": "America/New_York",
    "DC": "America/New_York",
    "DE": "America/New_York",
    "FL": "America/New_York",
    "GA": "America/New_York",
    "HI": "Pacific/Honolulu",
    "ID": "America/Boise",
    "IL": "America/Chicago",
    "IN": "America/New_York",
    "IA": "America/Chicago",
    "KS": "America/Chicago",
    "KY": "America/New_York",
    "LA": "America/Chicago",
    "ME": "America/New_York",
    "MD": "America/New_York",
    "MA": "America/New_York",
    "MI": "America/New_York",
    "MN": "America/Chicago",
    "MS": "America/Chicago",
    "MO": "America/Chicago",
    "MT": "America/Denver",
    "NE": "America/Chicago",
    "NV": "America/Los_Angeles",
    "NH": "America/New_York",
    "NJ": "America/New_York",
    "NM": "America/Denver",
    "NY": "America/New_York",
    "NC": "America/New_York",
    "ND": "America/Chicago",
    "OH": "America/New_York",
    "OK": "America/Chicago",
    "OR": "America/Los_Angeles",
    "PA": "America/New_York",
    "RI": "America/New_York",
    "SC": "America/New_York",
    "SD": "America/Chicago",
    "TN": "America/Chicago",
    "TX": "America/Chicago",
    "UT": "America/Denver",
    "VT": "America/New_York",
    "VA": "America/New_York",
    "WA": "America/Los_Angeles",
    "WV": "America/New_York",
    "WI": "America/Chicago",
    "WY": "America/Denver",
}

_STATE_NAME_TO_ABBREV: dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

_STATE_NAME_LOWER_TO_ABBREV: dict[str, str] = {k.lower(): v for k, v in _STATE_NAME_TO_ABBREV.items()}


def _state_to_abbrev(state: str) -> Optional[str]:
    s = (state or "").strip()
    if not s:
        return None

    s2 = re.sub(r"[^A-Za-z ]", " ", s).strip()
    s2 = re.sub(r"\s+", " ", s2)
    if not s2:
        return None

    # Abbreviation
    ab = re.sub(r"[^A-Za-z]", "", s2).upper()
    if len(ab) == 2 and ab in _STATE_ABBREV_TO_IANA_TZ:
        return ab

    # Full name (case-insensitive). This also avoids title-case issues like:
    # "District Of Columbia" vs "District of Columbia".
    s2_lower = s2.lower()
    if s2_lower in _STATE_NAME_LOWER_TO_ABBREV:
        return _STATE_NAME_LOWER_TO_ABBREV[s2_lower]

    # Best-effort: try to extract an abbreviation token from a longer string
    # (e.g. "New York Metropolitan Area", "CA", "Phoenix Arizona" won't match here).
    m = re.search(r"\b([A-Za-z]{2})\b", s2)
    if m:
        ab2 = m.group(1).upper()
        if ab2 in _STATE_ABBREV_TO_IANA_TZ:
            return ab2

    # Best-effort: look for a full state name as a whole-word substring.
    for name_lower, ab3 in _STATE_NAME_LOWER_TO_ABBREV.items():
        if re.search(rf"\b{re.escape(name_lower)}\b", s2_lower):
            return ab3

    return None


def state_to_timezone(state: str) -> Optional[str]:
    """
    Map a US state (full name or 2-letter abbrev) to an IANA timezone string.
    Multi-timezone states return the most-populated timezone.
    """
    ab = _state_to_abbrev(state)
    if not ab:
        return None
    return _STATE_ABBREV_TO_IANA_TZ.get(ab)

