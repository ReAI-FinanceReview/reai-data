"""조치 필요 여부 분석 모델

This module defines the ReviewActionAnalysis model for action requirement analysis
using Snorkel-based weak supervision.
"""

from sqlalchemy import Column, Boolean, Float, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class ReviewActionAnalysis(Base):
    """조치 필요 여부 분석 테이블 (Snorkel)

    Snorkel 기반 weak supervision으로 리뷰에 대한 조치 필요 여부를 판단합니다.
    심각한 이슈나 즉각 대응이 필요한 리뷰를 식별합니다.
    """
    __tablename__ = 'review_action_analysis'

    review_id = Column(
        UUID(as_uuid=True),
        ForeignKey('review_master_index.review_id'),
        primary_key=True,
        comment='리뷰 ID (FK to review_master_index)'
    )
    is_action_required = Column(
        Boolean,
        comment='조치 필요 여부'
    )
    action_confidence_score = Column(
        Float,
        comment='조치 필요 판단 신뢰도 (0.0 ~ 1.0)'
    )
    trigger_reason = Column(
        Text,
        comment='조치 필요 판단 사유'
    )
    is_attention_required = Column(
        Boolean,
        comment='관심 필요 여부 (경보 수준)'
    )
    is_verified = Column(
        Boolean,
        comment='검증 완료 여부'
    )
    review_summary = Column(
        Text,
        comment='LLM이 생성한 1문장 리뷰 요약 (dashboard 표시용)'
    )
    analyzed_at = Column(
        DateTime(timezone=True),
        comment='분석 시각'
    )

    def __repr__(self):
        return (
            f"<ReviewActionAnalysis(review_id={self.review_id}, "
            f"action_required={self.is_action_required}, "
            f"confidence={self.action_confidence_score})>"
        )
