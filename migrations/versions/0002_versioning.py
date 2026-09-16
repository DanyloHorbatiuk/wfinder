"""file_record.fetched_at, courses content_hash/last_seen_at/close_reason/description, ix_courses_posting

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import column, table

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # D6: the transitional "processing" status is no longer committed (F-02 loader rewrite).
    op.execute("UPDATE file_record SET status = 'pending' WHERE status = 'processing'")

    op.add_column("file_record", sa.Column("fetched_at", sa.DateTime(), nullable=True))
    _backfill_fetched_at()
    op.alter_column("file_record", "fetched_at", nullable=False)

    op.add_column("courses", sa.Column("content_hash", sa.String(length=64), nullable=True))
    op.add_column("courses", sa.Column("last_seen_at", sa.DateTime(), nullable=True))
    op.add_column("courses", sa.Column("close_reason", sa.String(length=16), nullable=True))
    op.add_column("courses", sa.Column("description", sa.Text(), nullable=True))

    op.create_check_constraint(
        "ck_courses_close_reason_valid",
        "courses",
        "close_reason IN ('changed', 'removed')",
    )
    op.create_index(
        "ix_courses_posting",
        "courses",
        ["source", "source_id", "active_from"],
    )


def _backfill_fetched_at() -> None:
    """Best-effort per SPEC §4.2: prefer the object's S3 LastModified, fall back
    to uploaded_at when MinIO is unreachable or the object is gone."""
    connection = op.get_bind()
    file_record = table(
        "file_record",
        column("id", sa.Integer),
        column("bucket", sa.String),
        column("key", sa.String),
        column("uploaded_at", sa.DateTime),
        column("fetched_at", sa.DateTime),
    )
    rows = connection.execute(
        sa.select(file_record.c.id, file_record.c.bucket, file_record.c.key, file_record.c.uploaded_at)
    ).fetchall()
    if not rows:
        return

    try:
        from storage.minio import get_s3_client
        s3_client = get_s3_client()
    except Exception:
        s3_client = None

    for row in rows:
        fetched_at = row.uploaded_at
        if s3_client is not None:
            try:
                head = s3_client.head_object(Bucket=row.bucket, Key=row.key)
                fetched_at = head["LastModified"].replace(tzinfo=None)
            except Exception:
                fetched_at = row.uploaded_at
        connection.execute(
            file_record.update().where(file_record.c.id == row.id).values(fetched_at=fetched_at)
        )


def downgrade() -> None:
    op.drop_index("ix_courses_posting", table_name="courses")
    op.drop_constraint("ck_courses_close_reason_valid", "courses", type_="check")
    op.drop_column("courses", "description")
    op.drop_column("courses", "close_reason")
    op.drop_column("courses", "last_seen_at")
    op.drop_column("courses", "content_hash")
    op.alter_column("file_record", "fetched_at", nullable=True)
    op.drop_column("file_record", "fetched_at")
