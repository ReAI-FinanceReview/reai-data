"""일별 서비스 리뷰 집계 팩트 테이블 (Gold)

서비스 × 플랫폼 × 날짜 기준으로 리뷰 건수, 조치 필요 건수, 평균 평점을 집계합니다.
"""

from sqlalchemy import Column, Date, Integer, Float, UniqueConstraint, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID

from .base import Base
from .enums import PlatformType


class FactServiceReviewDaily(Base):
    """일별 서비스 리뷰 집계 팩트 테이블

    집계 단위: (date, service_id, platform_type)
    """
    __tablename__ = 'fact_service_review_daily'
    __table_args__ = (
        UniqueConstraint('date', 'service_id', 'platform_type', name='uq_fact_srv_review_daily'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment='PK')
    date = Column(Date, nullable=False, comment='집계 날짜 (리뷰 작성일 기준)')
    service_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        comment='서비스 ID (FK to app_service, denormalized)',
    )
    platform_type = Column(
        SQLEnum(PlatformType, name='platform_type', create_type=False),
        nullable=False,
        comment='플랫폼 타입',
    )
    total_review_cnt = Column(Integer, default=0, comment='전체 리뷰 수')
    action_required_cnt = Column(Integer, default=0, comment='조치 필요 리뷰 수')
    attention_required_cnt = Column(Integer, default=0, comment='관심 필요 리뷰 수')
    avg_rating = Column(Float, comment='평균 평점')

    def __repr__(self):
        return (
            f"<FactServiceReviewDaily(date={self.date}, service_id={self.service_id}, "
            f"platform={self.platform_type}, total={self.total_review_cnt})>"
        )
