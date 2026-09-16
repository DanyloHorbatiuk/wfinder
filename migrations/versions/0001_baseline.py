"""baseline schema: file_record, courses, analytics schema

Revision ID: 0001
Revises:
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    op.create_table(
        "file_record",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("etag", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.String(), nullable=True),
        sa.UniqueConstraint("key"),
    )

    op.create_table(
        "courses",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=True),
        sa.Column("course_type", sa.String(), nullable=True),
        sa.Column("direction", sa.String(), nullable=True),
        sa.Column("format", sa.String(), nullable=True),
        sa.Column("level", sa.String(), nullable=True),
        sa.Column("price", sa.String(), nullable=True),
        sa.Column("date_start", sa.DateTime(), nullable=True),
        sa.Column("date_end", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("languages", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("active_from", sa.DateTime(), nullable=True),
        sa.Column("active_to", sa.DateTime(), nullable=True),
        sa.Column("file_record_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["file_record_id"], ["file_record.id"]),
    )
    op.create_index(
        "uq_active_course_per_source",
        "courses",
        ["source", "source_id"],
        unique=True,
        postgresql_where=sa.text("active_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_active_course_per_source", table_name="courses")
    op.drop_table("courses")
    op.drop_table("file_record")
    op.execute("DROP SCHEMA IF EXISTS analytics")
