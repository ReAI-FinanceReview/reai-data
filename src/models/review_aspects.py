"""애스펙트 기반 감성 분석 모델

This module defines the ReviewAspect model for aspect-based sentiment analysis results.
"""

from sqlalchemy import Column, BigInteger, Text, Float, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class ReviewAspect(Base):
    """애스펙트 기반 감성 분석 테이블 (Silver)

    리뷰의 각 측면(aspect)에 대한 감성 분석 결과를 저장합니다.
    예: "계좌이체가 빠르다" → keyword='계좌이체', sentiment_score=0.85, category='기능'
    """
    __tablename__ = 'review_aspects'

    aspect_id = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment='애스펙트 고유 ID'
    )
    review_id = Column(
        UUID(as_uuid=True),
        ForeignKey('review_master_index.review_id'),
        nullable=False,
        comment='리뷰 ID (FK to review_master_index)'
    )
    keyword = Column(Text, comment='추출된 키워드/애스펙트')
    sentiment_score = Column(Float, comment='감성 점수 (-1.0 ~ 1.0)')
    category = Column(Text, comment='카테고리 (기능, UI/UX, 성능, 고객서비스 등)')

    def __repr__(self):
        return (
            f"<ReviewAspect(aspect_id={self.aspect_id}, review_id={self.review_id}, "
            f"keyword='{self.keyword}', sentiment={self.sentiment_score})>"
        )
