"""최종 처리 부서 할당 모델 (Gold - DB)

This module defines the ReviewAssigned model for final department assignment results.
"""

from sqlalchemy import (
    ARRAY, BigInteger, Boolean, Column, DateTime, Float, ForeignKey,
    Integer, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from .base import Base


class ReviewAssigned(Base):
    """최종 처리 부서 할당 테이블 (Gold - DB)

    리뷰 분석 결과를 바탕으로 처리 담당 부서를 할당한 최종 결과를 저장합니다.
    LLM 기반 배정 로직의 출력물입니다.
    """
    __tablename__ = 'reviews_assigned'

    # 리비전 20260813_0002. 재실행이 같은 행을 UPSERT 하도록 하고, 규칙/LLM 배정
    # 결과가 서로를 덮어쓰지 않게 배정기까지 포함해 유일성을 건다.
    __table_args__ = (
        UniqueConstraint('review_id', 'assigner', name='uq_reviews_assigned_review_id_assigner'),
    )

    assigned_id = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment='배정 레코드 ID'
    )
    review_id = Column(
        UUID(as_uuid=True),
        ForeignKey('review_master_index.review_id'),
        nullable=False,
        comment='리뷰 ID (FK to review_master_index)'
    )
    assigner = Column(
        Text,
        nullable=False,
        server_default='rule',
        comment='배정기 식별자 (rule, llm) - 같은 리뷰에 배정기별 1행'
    )
    # 배정 코드(src/gold/dept_assigner.py)는 이 컬럼을 채우지 않는다. 리뷰 1건은
    # aspect 를 N 개 낳는데 이 컬럼은 UNIQUE 라 리뷰-aspect 를 1:1 로 강제한다 —
    # N 개 중 하나를 골라 넣으면 근거가 임의로 정해지고, 두 aspect 를 가진 리뷰
    # 두 건이 같은 aspect_id 를 고르면 UNIQUE 가 배정 자체를 막는다. 배정 근거는
    # assignment_reason 텍스트로 남긴다.
    review_feature_id = Column(
        BigInteger,
        unique=True,
        comment='특성 추출된 리뷰 ID (review_aspects.aspect_id) - 현재 배정 코드는 채우지 않음'
    )
    assigned_dept = Column(
        ARRAY(Text),
        comment='배정된 부서 목록 (ltree 경로 배열)'
    )
    assignment_reason = Column(Text, comment='배정 사유')
    confidence = Column(Float, comment='배정 신뢰도 (0.0 ~ 1.0)')
    is_failed = Column(Boolean, comment='실패 여부')
    try_number = Column(Integer, comment='시도 횟수')
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment='생성 시각'
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment='수정 시각'
    )

    def __repr__(self):
        return (
            f"<ReviewAssigned(assigned_id={self.assigned_id}, review_id={self.review_id}, "
            f"dept={self.assigned_dept}, confidence={self.confidence})>"
        )
