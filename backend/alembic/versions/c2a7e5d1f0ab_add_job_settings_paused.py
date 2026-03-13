"""add job_settings.paused

Revision ID: c2a7e5d1f0ab
Revises: 8f6f1b2c3d4e
Create Date: 2026-01-17

"""

from __future__ import annotations

from alembic import op

revision = "c2a7e5d1f0ab"
down_revision = "8f6f1b2c3d4e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent: safe in dev where tables/columns may already exist.
    op.execute("ALTER TABLE job_settings ADD COLUMN IF NOT EXISTS paused BOOLEAN NOT NULL DEFAULT FALSE")


def downgrade() -> None:
    op.execute("ALTER TABLE job_settings DROP COLUMN IF EXISTS paused")

