"""add profiles.timezone

Revision ID: b08f955ea11c
Revises: 
Create Date: 2026-01-10 20:37:45.045465

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b08f955ea11c'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Keep migration idempotent for dev environments where Base.metadata.create_all()
    # may have already created the column.
    op.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS timezone VARCHAR")


def downgrade() -> None:
    op.execute("ALTER TABLE profiles DROP COLUMN IF EXISTS timezone")
