"""add post-reset heartbeat observations

Revision ID: 20260607_010000_add_post_reset_heartbeat_observations
Revises: 20260607_000000_merge_weekly_monthly_useragent_heads
Create Date: 2026-06-07 01:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260607_010000_add_post_reset_heartbeat_observations"
down_revision = "20260607_000000_merge_weekly_monthly_useragent_heads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "post_reset_heartbeat_observations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("window", sa.String(), nullable=False),
        sa.Column("stalled_reset_at", sa.Integer(), nullable=False),
        sa.Column("observed_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("heartbeat_sent_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "account_id",
            "window",
            "stalled_reset_at",
            name="uq_post_reset_heartbeat_account_window_reset",
        ),
    )
    op.create_index(
        "idx_post_reset_heartbeat_account_window",
        "post_reset_heartbeat_observations",
        ["account_id", "window"],
    )


def downgrade() -> None:
    op.drop_index("idx_post_reset_heartbeat_account_window", table_name="post_reset_heartbeat_observations")
    op.drop_table("post_reset_heartbeat_observations")
