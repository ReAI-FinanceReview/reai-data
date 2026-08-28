"""reviews_assigned assigner discriminator + unique

Revision ID: 20260813_0002
Revises: 20260430_0001
Create Date: 2026-08-13 00:00:00.000000

부서 배정은 리뷰 1건당 배정기 1개마다 1행이며 재실행 시 UPSERT 해야 한다.
기존 스키마에는 ``review_id`` 에 UNIQUE 가 없어 재실행할 때마다 같은 리뷰의
배정 행이 그대로 쌓였다.

제약을 ``review_id`` 단독이 아니라 ``(review_id, assigner)`` 로 거는 이유는
평가 때문이다. 규칙 배정과 LLM 배정을 비교하는 것이 ``src.gold.dept_eval`` 의
목적인데, ``review_id`` 만 유일하면 두 번째 배정기가 첫 번째 결과를 덮어써서
비교할 대상이 남지 않는다. 배정기를 판별하는 컬럼이 있어야 두 결과가 공존한다.

기본값 ``'rule'`` 은 이 리비전 이전에 적재된 행을 위한 것이다. 그 시점에는
규칙 배정기가 유일한 생산자였다.

중복 행 정리는 이 마이그레이션에서 하지 않는다. 어느 행을 남길지는 데이터
소유자의 판단이고, 마이그레이션이 조용히 지우면 되돌릴 수 없다. 중복이 있으면
제약 생성이 실패하므로 그때 운영자가 정리한 뒤 다시 실행한다.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260813_0002"
down_revision = "20260430_0001"
branch_labels = None
depends_on = None

CONSTRAINT_NAME = "uq_reviews_assigned_review_id_assigner"
DEFAULT_ASSIGNER = "rule"


def upgrade() -> None:
    op.add_column(
        "reviews_assigned",
        sa.Column(
            "assigner",
            sa.Text(),
            nullable=False,
            server_default=DEFAULT_ASSIGNER,
            comment="배정기 식별자 (rule, llm) - 같은 리뷰에 배정기별 1행",
        ),
    )
    op.create_unique_constraint(
        CONSTRAINT_NAME,
        "reviews_assigned",
        ["review_id", "assigner"],
    )


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT_NAME, "reviews_assigned", type_="unique")
    op.drop_column("reviews_assigned", "assigner")
