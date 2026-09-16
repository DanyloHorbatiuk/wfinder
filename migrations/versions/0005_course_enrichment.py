"""course_enrichment (SPEC §9, E-04)

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "course_enrichment",
        sa.Column("course_id", sa.Integer(), primary_key=True),
        sa.Column("level_norm", sa.String(length=16), nullable=False),
        sa.Column("format_norm", sa.String(length=16), nullable=False),
        sa.Column("city_norm", sa.String(), nullable=True),
        sa.Column("country_norm", sa.String(), nullable=True),
        sa.Column("rules_version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["course_id"], ["courses.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "level_norm IN ('intern', 'junior', 'middle', 'senior', 'unknown')",
            name="ck_enrichment_level_norm",
        ),
        sa.CheckConstraint(
            "format_norm IN ('online', 'offline', 'hybrid', 'unknown')",
            name="ck_enrichment_format_norm",
        ),
    )


def downgrade() -> None:
    op.drop_table("course_enrichment")
