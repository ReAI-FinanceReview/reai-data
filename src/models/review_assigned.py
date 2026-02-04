"""최종 처리 부서 할당 모델 (Gold - DB)

This module defines the ReviewAssigned model for final department assignment results.
"""

from sqlalchemy import Column, Text, Integer, DateTime, BigInteger, Float, Boolean, ForeignKey, ARRAY
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from .base import Base


class ReviewAssigned(Base):
    """최종 처리 부서 할당 테이블 (Gold - DB)

    리뷰 분석 결과를 바탕으로 처리 담당 부서를 할당한 최종 결과를 저장합니다.
    LLM 기반 배정 로직의 출력물입니다.
    """
    __tablename__ = 'reviews_assigned'

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
