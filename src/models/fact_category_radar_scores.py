"""카테고리별 레이더 차트 점수 팩트 테이블 (Gold)

서비스 × 날짜 × category_type 기준 평균 감성 점수를 저장합니다.
5대 지표(USABILITY / STABILITY / DESIGN / CUSTOMER_SUPPORT / SPEED)별
레이더 차트 시각화에 사용됩니다.
"""

from sqlalchemy import Column, Date, Integer, Float, UniqueConstraint, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID

from .base import Base
from .enums import CategoryType


class FactCategoryRadarScores(Base):
    """카테고리별 레이더 차트 점수 팩트 테이블

    집계 단위: (date, service_id, category_type)
    """
    __tablename__ = 'fact_category_radar_scores'
    __table_args__ = (
        UniqueConstraint(
            'date', 'service_id', 'category_type',
            name='uq_fact_category_radar',
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment='PK')
    date = Column(Date, nullable=False, comment='집계 날짜')
    service_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        comment='서비스 ID (denormalized)',
    )
    category_type = Column(
        SQLEnum(CategoryType, name='category_type', create_type=False),
        nullable=False,
        comment='5대 카테고리 (오방성)',
    )
    avg_sentiment_score = Column(Float, comment='카테고리 평균 감성 점수 (0.0~1.0)')
    review_cnt = Column(Integer, default=0, comment='해당 카테고리 리뷰 수')

    def __repr__(self):
        return (
            f"<FactCategoryRadarScores(date={self.date}, service_id={self.service_id}, "
            f"category={self.category_type}, score={self.avg_sentiment_score})>"
        )
