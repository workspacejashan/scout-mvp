from __future__ import annotations

from typing import Optional

import httpx

from app.core.config import settings


class TwilioError(RuntimeError):
    pass


def is_configured() -> bool:
    return bool((settings.TWILIO_ACCOUNT_SID or "").strip() and (settings.TWILIO_AUTH_TOKEN or "").strip())


def send_sms(*, to_phone_e164: str, from_phone_e164: str, body: str) -> str:
    """
    Send a single SMS via Twilio REST API. Returns MessageSid.
    Uses env vars:
    - TWILIO_ACCOUNT_SID
    - TWILIO_AUTH_TOKEN
    """
    sid = (settings.TWILIO_ACCOUNT_SID or "").strip()
    token = (settings.TWILIO_AUTH_TOKEN or "").strip()
    if not sid or not token:
        raise TwilioError("twilio_not_configured")

    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    data = {"To": to_phone_e164, "From": from_phone_e164, "Body": body}

    with httpx.Client(timeout=30) as client:
        resp = client.post(url, data=data, auth=(sid, token))

    if resp.status_code >= 400:
        raise TwilioError(f"twilio_http_{resp.status_code}:{resp.text[:300]}")

    try:
        j = resp.json()
    except Exception as e:  # noqa: BLE001
        raise TwilioError(f"twilio_bad_json:{e}") from e

    msid: Optional[str] = j.get("sid") if isinstance(j, dict) else None
    if not msid:
        raise TwilioError("twilio_missing_sid")
    return str(msid)

