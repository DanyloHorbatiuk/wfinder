"""load_stats table (one row per processed file_record)

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "load_stats",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_record_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("snapshot_at", sa.DateTime(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=True),
        sa.Column("received", sa.Integer(), nullable=False),
        sa.Column("rejected", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inserted", sa.Integer(), nullable=False),
        sa.Column("changed", sa.Integer(), nullable=False),
        sa.Column("unchanged", sa.Integer(), nullable=False),
        sa.Column("closed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("closures_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("block_reason", sa.Text(), nullable=True),
        sa.Column("parser_version", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["file_record_id"], ["file_record.id"]),
        sa.UniqueConstraint("file_record_id"),
    )


def downgrade() -> None:
    op.drop_table("load_stats")
