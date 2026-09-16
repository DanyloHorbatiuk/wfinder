"""skill, course_skill, courses.skills_version (SPEC §8.5)

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("courses", sa.Column("skills_version", sa.Integer(), nullable=True))

    op.create_table(
        "skill",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=False),
        sa.UniqueConstraint("name", name="uq_skill_name"),
    )

    op.create_table(
        "course_skill",
        sa.Column("course_id", sa.Integer(), nullable=False),
        sa.Column("skill_id", sa.Integer(), nullable=False),
        sa.Column("matched_text", sa.String(), nullable=False),
        sa.Column("field", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("course_id", "skill_id"),
        sa.ForeignKeyConstraint(["course_id"], ["courses.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["skill_id"], ["skill.id"]),
    )


def downgrade() -> None:
    op.drop_table("course_skill")
    op.drop_table("skill")
    op.drop_column("courses", "skills_version")
