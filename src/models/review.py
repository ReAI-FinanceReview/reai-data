"""
리뷰 데이터 모델
"""
from sqlalchemy import Column, BigInteger, String, SmallInteger, Text, DateTime, ForeignKey
from sqlalchemy.sql import func
from .base import Base

class Review(Base):
    """리뷰 데이터 모델 (app_reviews 테이블)"""
    __tablename__ = 'app_reviews'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    app_id = Column(BigInteger, ForeignKey('apps.id'), nullable=False)
    platform = Column(String, nullable=False)
    country_code = Column(String, nullable=False, default='kr')
    platform_review_id = Column(String, nullable=False)
    reviewer_name = Column(String, nullable=True)
    review_text = Column(Text, nullable=False)
    rating = Column(SmallInteger, nullable=False)
    app_version = Column(String, nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
