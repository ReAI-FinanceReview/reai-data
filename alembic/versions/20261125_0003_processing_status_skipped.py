"""processing_status_type SKIPPED

Revision ID: 20261125_0003
Revises: 20260813_0002
Create Date: 2026-11-25 00:00:00.000000

규칙 기반 ABSA 가 분석할 수 없는 리뷰(빈 텍스트, 한글 없음, aspect 없음)는 재시도해도
결과가 같다 (issue #94). 지금까지는 ``FAILED`` 로 남아 ``retry_count`` 를 소진한 뒤
dead letter 로 섞였다. 이들을 종결 상태 ``SKIPPED`` 로 분리한다. 사유는
``error_message`` 의 ``skip:<사유>`` 로 남긴다.

``ALTER TYPE ... ADD VALUE`` 는 autocommit 블록에서 실행한다. 트랜잭션 안에서 추가한 값은
커밋 전까지 쓸 수 없고, PostgreSQL 12 미만은 트랜잭션 안에서 실행 자체를 거부한다.

downgrade 는 enum 값을 뺄 수 없어 타입을 다시 만든다. ``SKIPPED`` 행이 남아 있으면
거부한다. 그 행을 ``FAILED`` 로 되돌릴지 다시 분석할지는 데이터 소유자가 정한다.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261125_0003"
down_revision = "20260813_0002"
branch_labels = None
depends_on = None

TYPE_NAME = "processing_status_type"
NEW_VALUE = "SKIPPED"
PREVIOUS_VALUES = ("RAW", "CLEANED", "ANALYZED", "FAILED")
TYPE_COMMENT = "데이터 처리 상태"
# 베이스라인(schema_v4.sql)의 컬럼 주석 그대로. downgrade 가 이 값으로 되돌린다.
COLUMN_COMMENT = (
    "RAW (Bronze Parquet 저장 완료) / CLEANED (Silver 전처리 완료) / "
    "ANALYZED (Gold 분석 완료) / FAILED (실패)"
)
COLUMN_COMMENT_WITH_SKIPPED = (
    COLUMN_COMMENT + " / SKIPPED (규칙 기반 분석 불가, 종결 — 사유는 error_message 의 skip:<사유>)"
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TYPE {TYPE_NAME} ADD VALUE IF NOT EXISTS '{NEW_VALUE}'")
    _comment_column(COLUMN_COMMENT_WITH_SKIPPED)


def _comment_column(comment: str) -> None:
    # 주석 문자열에는 작은따옴표가 없다. 바인드 파라미터 대신 리터럴로 넣어 offline(--sql)
    # 모드에서도 그대로 렌더링되게 한다.
    op.execute(f"COMMENT ON COLUMN review_master_index.processing_status IS '{comment}'")


def downgrade() -> None:
    bind = op.get_bind()
    skipped = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM review_master_index "
            f"WHERE processing_status = '{NEW_VALUE}'"
        )
    ).scalar_one()
    if skipped:
        raise RuntimeError(
            f"{skipped} review_master_index rows are {NEW_VALUE}; move them to FAILED or "
            "CLEANED before downgrading — the enum value cannot be dropped while in use"
        )

    values = ", ".join(f"'{v}'" for v in PREVIOUS_VALUES)
    op.execute(f"ALTER TYPE {TYPE_NAME} RENAME TO {TYPE_NAME}_old")
    op.execute(f"CREATE TYPE {TYPE_NAME} AS ENUM ({values})")
    op.execute(
        "ALTER TABLE review_master_index ALTER COLUMN processing_status "
        f"TYPE {TYPE_NAME} USING processing_status::text::{TYPE_NAME}"
    )
    op.execute(f"DROP TYPE {TYPE_NAME}_old")
    op.execute(f"COMMENT ON TYPE {TYPE_NAME} IS '{TYPE_COMMENT}'")
    _comment_column(COLUMN_COMMENT)
