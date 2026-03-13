"""merge heads (sms tables + profiles.title)

Revision ID: 8f6f1b2c3d4e
Revises: 29d92854d069, 9c1a7d2f6b1a
Create Date: 2026-01-12
"""

from __future__ import annotations

from alembic import op

revision = "8f6f1b2c3d4e"
down_revision = ("29d92854d069", "9c1a7d2f6b1a")
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Merge migration heads; no schema change.
    pass


def downgrade() -> None:
    # No-op: downgrade path handled by individual branches.
    pass

