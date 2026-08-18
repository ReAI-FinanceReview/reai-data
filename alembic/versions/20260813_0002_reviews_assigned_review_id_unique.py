"""reviews_assigned review_id unique

Revision ID: 20260813_0002
Revises: 20260430_0001
Create Date: 2026-08-13 00:00:00.000000

부서 배정은 리뷰 1건당 1행이며 재실행 시 UPSERT 해야 한다. 기존 스키마에는
``review_id`` 에 UNIQUE 가 없어 재실행할 때마다 같은 리뷰의 배정 행이 그대로
쌓였다. ON CONFLICT 대상이 되도록 제약을 건다.

중복 행 정리는 이 마이그레이션에서 하지 않는다. 어느 행을 남길지는 데이터
소유자의 판단이고, 마이그레이션이 조용히 지우면 되돌릴 수 없다. 중복이 있으면
제약 생성이 실패하므로 그때 운영자가 정리한 뒤 다시 실행한다.
"""

from __future__ import annotations

from alembic import op

revision = "20260813_0002"
down_revision = "20260430_0001"
branch_labels = None
depends_on = None

CONSTRAINT_NAME = "uq_reviews_assigned_review_id"


def upgrade() -> None:
    op.create_unique_constraint(
        CONSTRAINT_NAME,
        "reviews_assigned",
        ["review_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        CONSTRAINT_NAME,
        "reviews_assigned",
        type_="unique",
    )
