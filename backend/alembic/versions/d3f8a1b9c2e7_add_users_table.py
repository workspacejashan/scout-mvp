"""add users table

Revision ID: d3f8a1b9c2e7
Revises: c2a7e5d1f0ab
Create Date: 2026-02-11
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from alembic import op

revision = "d3f8a1b9c2e7"
down_revision = "c2a7e5d1f0ab"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("tier", sa.String(), nullable=False, server_default="free"),
        sa.Column("stripe_customer_id", sa.String(), nullable=True),
        sa.Column("stripe_subscription_id", sa.String(), nullable=True),
        sa.Column("stripe_subscription_status", sa.String(), nullable=True),
        sa.Column("stripe_current_period_end", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.create_index("ix_users_stripe_customer_id", "users", ["stripe_customer_id"], unique=True)

    # Seed existing admin: use APP_DEFAULT_OWNER_ID as the user id so all
    # existing owner_id foreign references resolve to this user automatically.
    owner_id = os.getenv("APP_DEFAULT_OWNER_ID", "local-owner")
    admin_email = (os.getenv("ADMIN_EMAILS") or "").split(",")[0].strip().lower()
    if admin_email:
        op.execute(
            sa.text(
                "INSERT INTO users (id, email, tier, created_at, updated_at) "
                "VALUES (:id, :email, 'unlocked', NOW(), NOW()) "
                "ON CONFLICT DO NOTHING"
            ).bindparams(id=owner_id, email=admin_email)
        )


def downgrade() -> None:
    op.drop_index("ix_users_stripe_customer_id", table_name="users")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
