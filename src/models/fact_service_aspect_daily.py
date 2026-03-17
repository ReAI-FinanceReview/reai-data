"""일별 서비스 애스펙트 집계 팩트 테이블 (Gold)

서비스 × 날짜 × 키워드 기준으로 언급 횟수와 평균 감성 점수를 집계합니다.
"""

from sqlalchemy import Column, Date, Integer, Float, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class FactServiceAspectDaily(Base):
    """일별 서비스 애스펙트 집계 팩트 테이블

    집계 단위: (date, service_id, keyword)
    """
    __tablename__ = 'fact_service_aspect_daily'
    __table_args__ = (
        UniqueConstraint('date', 'service_id', 'keyword', name='uq_fact_srv_aspect_daily'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment='PK')
    date = Column(Date, nullable=False, comment='집계 날짜')
    service_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        comment='서비스 ID (denormalized)',
    )
    keyword = Column(Text, nullable=False, comment='애스펙트 키워드')
    mention_cnt = Column(Integer, default=0, comment='언급 횟수')
    avg_sentiment_score = Column(Float, comment='평균 감성 점수 (0.0~1.0)')

    def __repr__(self):
        return (
            f"<FactServiceAspectDaily(date={self.date}, service_id={self.service_id}, "
            f"keyword='{self.keyword}', cnt={self.mention_cnt})>"
        )
