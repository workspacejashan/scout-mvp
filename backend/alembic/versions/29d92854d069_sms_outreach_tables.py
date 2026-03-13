"""sms outreach tables

Revision ID: 29d92854d069
Revises: b08f955ea11c
Create Date: 2026-01-10 22:21:53.681559

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '29d92854d069'
down_revision: Union[str, None] = 'b08f955ea11c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Job settings additions
    op.execute("ALTER TABLE job_settings ADD COLUMN IF NOT EXISTS job_location_label VARCHAR")
    op.execute("ALTER TABLE job_settings ADD COLUMN IF NOT EXISTS sms_template_text TEXT")
    op.execute("ALTER TABLE job_settings ADD COLUMN IF NOT EXISTS sms_daily_limit INTEGER NOT NULL DEFAULT 50")

    # Owner settings
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS owner_settings (
            id VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            recruiter_company VARCHAR NULL,
            twilio_from_number VARCHAR NULL,
            sms_global_daily_limit INTEGER NOT NULL DEFAULT 200,
            sms_business_start_hour INTEGER NOT NULL DEFAULT 7,
            sms_business_end_hour INTEGER NOT NULL DEFAULT 19,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_owner_settings_owner_id ON owner_settings(owner_id)")

    # SMS opt-outs
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sms_opt_outs (
            id VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            phone_e164 VARCHAR NOT NULL,
            reason VARCHAR NULL,
            revoked_at TIMESTAMP WITHOUT TIME ZONE NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_owner_phone_opt_out ON sms_opt_outs(owner_id, phone_e164)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_opt_outs_owner_id ON sms_opt_outs(owner_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_opt_outs_phone_e164 ON sms_opt_outs(phone_e164)")

    # SMS batches
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sms_batches (
            id VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            job_id VARCHAR NOT NULL REFERENCES jobs(id),
            status VARCHAR NOT NULL DEFAULT 'queued',
            requested_count INTEGER NOT NULL DEFAULT 0,
            created_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            approved_at TIMESTAMP WITHOUT TIME ZONE NULL,
            completed_at TIMESTAMP WITHOUT TIME ZONE NULL
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_batches_owner_id ON sms_batches(owner_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_batches_job_id ON sms_batches(job_id)")

    # Outbound messages (queue + sent log)
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sms_outbound_messages (
            id VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            job_id VARCHAR NOT NULL REFERENCES jobs(id),
            batch_id VARCHAR NULL REFERENCES sms_batches(id),
            profile_id VARCHAR NULL REFERENCES profiles(id),
            to_phone_e164 VARCHAR NOT NULL,
            from_phone_e164 VARCHAR NOT NULL,
            body TEXT NOT NULL,
            template_text TEXT NULL,
            placeholders_json JSONB NULL,
            status VARCHAR NOT NULL DEFAULT 'queued',
            twilio_sid VARCHAR NULL,
            error TEXT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
            approved_at TIMESTAMP WITHOUT TIME ZONE NULL,
            sent_at TIMESTAMP WITHOUT TIME ZONE NULL
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_owner_id ON sms_outbound_messages(owner_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_job_id ON sms_outbound_messages(job_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_batch_id ON sms_outbound_messages(batch_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_profile_id ON sms_outbound_messages(profile_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_to_phone_e164 ON sms_outbound_messages(to_phone_e164)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_outbound_messages_twilio_sid ON sms_outbound_messages(twilio_sid)")

    # Inbound messages (inbox)
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sms_inbound_messages (
            id VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            job_id VARCHAR NULL REFERENCES jobs(id),
            from_phone_e164 VARCHAR NOT NULL,
            to_phone_e164 VARCHAR NOT NULL,
            body TEXT NOT NULL,
            twilio_sid VARCHAR NOT NULL,
            tag VARCHAR NOT NULL DEFAULT 'Unknown',
            raw_json JSONB NULL,
            received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_owner_twilio_inbound_sid ON sms_inbound_messages(owner_id, twilio_sid)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_inbound_messages_owner_id ON sms_inbound_messages(owner_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_sms_inbound_messages_job_id ON sms_inbound_messages(job_id)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_sms_inbound_messages_from_phone_e164 ON sms_inbound_messages(from_phone_e164)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_sms_inbound_messages_to_phone_e164 ON sms_inbound_messages(to_phone_e164)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sms_inbound_messages")
    op.execute("DROP TABLE IF EXISTS sms_outbound_messages")
    op.execute("DROP TABLE IF EXISTS sms_batches")
    op.execute("DROP TABLE IF EXISTS sms_opt_outs")
    op.execute("DROP TABLE IF EXISTS owner_settings")

    op.execute("ALTER TABLE job_settings DROP COLUMN IF EXISTS sms_daily_limit")
    op.execute("ALTER TABLE job_settings DROP COLUMN IF EXISTS sms_template_text")
    op.execute("ALTER TABLE job_settings DROP COLUMN IF EXISTS job_location_label")
