"""add profiles.title

Revision ID: 9c1a7d2f6b1a
Revises: b08f955ea11c
Create Date: 2026-01-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "9c1a7d2f6b1a"
down_revision = "b08f955ea11c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("profiles", sa.Column("title", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("profiles", "title")

