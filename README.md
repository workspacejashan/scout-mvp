## Scout MVP

Monorepo for the **Scout** slice: **Copilot → boolean strategy → Run → Google CSE (100 results) → store profiles + dropped_profiles**.

### Stack
- **Frontend**: Next.js (3-panel UI: Copilot / Jobs / Job details)
- **Backend**: FastAPI
- **Queue**: Celery + Redis
- **DB**: PostgreSQL

### Local run (dev)

1) Start infra:

```bash
docker-compose up -d
```

### Migrations (Alembic)

This repo now uses **Alembic** for schema changes (e.g., adding columns to existing tables like `profiles.timezone`).

Run migrations:

```bash
cd backend
source .venv/bin/activate
alembic upgrade head
```

Create a new migration:

```bash
cd backend
source .venv/bin/activate
alembic revision -m "your change"
```

Backfill profile timezones (derived from `profiles.state`, stored as IANA tz like `America/Chicago`):

```bash
cd backend
source .venv/bin/activate
python3 - <<'PY'
from app.worker.tasks import backfill_profile_timezones
print(backfill_profile_timezones(owner_id=None, batch_size=5000, dry_run=False))
PY
```

### SMS Outreach (Twilio)

Set these in your `.env`:

```bash
TWILIO_ACCOUNT_SID=...
TWILIO_AUTH_TOKEN=...
TWILIO_FROM_NUMBER=+14155550123
```

Inbound webhook endpoint (configure in Twilio Messaging webhook):
- `POST /api/sms/twilio/webhook`

2) Copy `env.example` to `.env` and fill keys:
- `GOOGLE_CSE_API_KEY`
- `GOOGLE_CSE_CX`
- `OPENROUTER_API_KEY`

Place `.env` either in repo root or `backend/` (dotenv searches upwards).
This compose uses **Postgres on 5433** and **Redis on 6380** by default (to avoid conflicts).

3) Backend:

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

4) Worker:

```bash
cd backend
source .venv/bin/activate
celery -A app.worker.celery_app worker -l info
```

5) Frontend:

```bash
cd frontend
npm install
npm run dev
```

